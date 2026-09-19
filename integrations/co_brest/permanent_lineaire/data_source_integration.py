import json
import tempfile
import zipfile
from pathlib import Path
from typing import NamedTuple

import geopandas as gpd
import polars as pl
import requests
from loguru import logger
from pyproj import Transformer
from shapely.geometry import mapping

from api.dia_log_client.models import (
    MeasureTypeEnum as MTE,
)
from api.dia_log_client.models import (
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
)
from integrations.base_data_source_integration import BaseDataSourceIntegration
from integrations.shared.local_time import start_of_local_day

from .schema import Schema

# URL = "https://echanges.brest-metropole.fr/VIPDU72/GPB/DEP_ARR_CIRC_STAT_L_V.zip"
URL = "https://www.data.gouv.fr/api/1/datasets/r/760ac62d-b3aa-4d30-898c-94fea81e4537"
FILENAME = "DEP_ARR_CIRC_STAT_L_V.shp"

transformer = Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)


class C(NamedTuple):
    measure_type: MTE
    exempted_types: list[str] | None = None


DESCRIPTION_CONFIG = {
    # Limitations de vitesse
    "Limitation Vitesse": C(MTE.SPEEDLIMITATION),
    # Stationnement
    "Stationnement interdit": C(MTE.PARKINGPROHIBITED),
    "Arrêt interdit": C(MTE.PARKINGPROHIBITED),
    "Stationnement gênant": C(MTE.PARKINGPROHIBITED),
    "Stationnement interdit aux poids-lourds": C(MTE.PARKINGPROHIBITED),
    # noEntry – limitations dimensionnelles (poids / hauteur)
    "Limitation Poids": C(MTE.NOENTRY),
    "Limitation Hauteur": C(MTE.NOENTRY),
    "Interdit aux transports de marchandises": C(MTE.NOENTRY),
    # noEntry – catégories particulières
    "Interdit dans les 2 sens": C(MTE.NOENTRY),
    "Interdit à  tous véhicules à moteur": C(MTE.NOENTRY, ["bicycle", "pedestrians"]),
    "Interdit aux véhicules à moteur sauf cyclos": C(
        MTE.NOENTRY, ["bicycle", "pedestrians", "other"]
    ),
    "Limitation Largeur": C(MTE.NOENTRY),
    "Sens interdit / Sens unique": C(MTE.NOENTRY),
}


class DataSourceIntegration(BaseDataSourceIntegration):
    """Data source for Brest permanent lineaire shapefile data."""

    name = "permanent_lineaire"
    raw_data_schema = Schema

    def fetch_raw_data(self) -> pl.DataFrame:
        logger.info(f"Downloading and reading shapefile data from {URL}")
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = Path(tmpdir) / "data.zip"

            r = requests.get(URL)
            r.raise_for_status()
            zip_path.write_bytes(r.content)
            logger.info(f"Downloaded zip file to {zip_path}")

            with zipfile.ZipFile(zip_path) as z:
                z.extractall(tmpdir)

            shp_path = next(Path(tmpdir).rglob("*.shp"))
            shp_path = Path(tmpdir) / FILENAME

            logger.info(f"Reading file {shp_path}")
            gdf = gpd.read_file(shp_path)

        # Polars cannot hold shapely geometries: carry them as WKT (EPSG:2154).
        gdf["geometry"] = gdf.geometry.to_wkt()
        return pl.from_pandas(gdf)

    def preprocess_raw_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        """Cast the OUI/NON columns to booleans and drop rows with an empty NOARR."""
        return raw_data.with_columns(
            [
                self.cast_boolean_column("CYCLO"),
                self.cast_boolean_column("VELO"),
            ]
        ).filter(~(pl.col("NOARR").eq("")))

    def compute_clean_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        return (
            raw_data.pipe(compute_measure_fields)
            .pipe(compute_period_fields)
            .pipe(compute_location_fields)
            .pipe(compute_regulation_fields)
            .pipe(compute_vehicle_fields)
        )

    def cast_boolean_column(self, column_name: str) -> pl.Expr:
        return (
            pl.when(pl.col(column_name).str.to_uppercase() == "OUI")
            .then(True)
            .when(pl.col(column_name).str.to_uppercase() == "NON")
            .then(False)
            .cast(pl.Boolean)
            .alias(column_name)
            .fill_null(False)
        )


def compute_regulation_fields(df: pl.DataFrame) -> pl.DataFrame:
    """One regulation per NOARR, which can carry several measures.

    Title ("{DESCRIPTIF} – {LIBRU}") and LIEN_URL come from the first row of the NOARR.
    """
    df = df.with_columns(pl.col("NOARR").cum_count().over("NOARR").alias("_row_num_in_regulation"))

    first_row_data = df.filter(pl.col("_row_num_in_regulation") == 1).select(
        [
            pl.col("NOARR"),
            (pl.col("DESCRIPTIF") + pl.lit(" – ") + pl.col("LIBRU")).alias("regulation_title"),
            pl.col("LIEN_URL"),
        ]
    )

    df = df.join(first_row_data, on="NOARR", how="left")

    df = df.with_columns(
        [
            (pl.col("NOARR") + pl.lit("-0")).alias("regulation_identifier"),
            pl.lit(PostApiRegulationsAddBodyCategory.PERMANENTREGULATION.value).alias(
                "regulation_category"
            ),
            pl.lit(PostApiRegulationsAddBodySubject.OTHER.value).alias("regulation_subject"),
            pl.lit("Circulation").alias("regulation_other_category_text"),
            pl.col("LIEN_URL").alias("regulation_document_url"),
        ]
    )

    num_null_titles = df.select(pl.col("regulation_title").is_null().sum()).item()
    logger.warning(f"Dropping {num_null_titles} rows with null regulation_title")
    df = df.filter(pl.col("regulation_title").is_not_null())

    return df


def compute_period_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Permanent period starting on DT_MAT; rows without it are dropped."""
    n_null_dt_mat = df.select(pl.col("DT_MAT").is_null().sum()).item()
    if n_null_dt_mat > 0:
        logger.warning(f"Dropping {n_null_dt_mat} rows with null DT_MAT (no start date available)")

    df = df.filter(pl.col("DT_MAT").is_not_null())

    return df.with_columns(
        [
            start_of_local_day(df, "DT_MAT").alias("period_start_date"),
            pl.lit(None).alias("period_end_date"),
            pl.lit("everyDay").alias("period_recurrence_type"),
            pl.lit(True).alias("period_is_permanent"),
        ]
    )


def compute_location_fields(df: pl.DataFrame) -> pl.DataFrame:
    """rawGeoJSON reprojected from Lambert 93 (EPSG:2154) to EPSG:4326; rows without
    geometry are dropped."""
    n_null_geometry = df.select(pl.col("geometry").is_null().sum()).item()
    if n_null_geometry > 0:
        logger.warning(f"Dropping {n_null_geometry} rows with null geometry")

    df = df.filter(pl.col("geometry").is_not_null())

    pdf = df.to_pandas()
    gdf = gpd.GeoDataFrame(pdf, geometry=gpd.GeoSeries.from_wkt(pdf["geometry"]), crs="EPSG:2154")
    gdf = gdf.to_crs("EPSG:4326")
    pdf["location_geometry"] = gdf.geometry.apply(lambda geom: json.dumps(mapping(geom)))
    df = pl.from_pandas(pdf)

    return df.with_columns(
        [
            pl.lit(RoadTypeEnum.RAWGEOJSON.value).alias("location_road_type"),
            (pl.col("LIBCO") + pl.lit(" – ") + pl.col("LIBRU")).alias("location_label"),
        ]
    )


def compute_measure_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Measure type from DESCRIPTIF (DESCRIPTION_CONFIG), speed from VITEMAX.

    Drops: DESCRIPTIF outside DESCRIPTION_CONFIG; "Sens interdit / Sens unique" with
    SENS=1 (direction handling, R-32); speed limitations without a positive VITEMAX.
    """
    type_mapping = {
        descriptif: config.measure_type.value for descriptif, config in DESCRIPTION_CONFIG.items()
    }

    df = (
        df.filter(pl.col("DESCRIPTIF").is_in(DESCRIPTION_CONFIG.keys()))
        .filter(~(pl.col("DESCRIPTIF").eq("Sens interdit / Sens unique") & pl.col("SENS").eq(1)))
        .with_columns(pl.col("DESCRIPTIF").replace(type_mapping).alias("measure_type_"))
    )

    invalid_speed = (pl.col("measure_type_") == MTE.SPEEDLIMITATION.value) & (
        (pl.col("VITEMAX").is_null()) | (pl.col("VITEMAX") <= 0)
    )
    n_invalid = df.select(invalid_speed.sum()).item()
    if n_invalid > 0:
        logger.warning(f"Dropping {n_invalid} SPEEDLIMITATION measures with invalid VITEMAX")

    df = df.filter(~invalid_speed)

    return df.with_columns(
        pl.when(pl.col("measure_type_") == MTE.SPEEDLIMITATION.value)
        .then(pl.col("VITEMAX"))
        .otherwise(None)
        .alias("measure_max_speed")
    )


def compute_vehicle_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Dimensions from POIDS / HAUTEUR / LARGEUR (0 means none); exemptions from
    DESCRIPTION_CONFIG, else from the CYCLO (-> other) and VELO (-> bicycle) flags;
    heavyGoodsVehicle whenever a weight limit is set, all vehicles otherwise."""
    exempted_types_mapping = {
        descriptif: config.exempted_types for descriptif, config in DESCRIPTION_CONFIG.items()
    }

    df = df.with_columns(
        [
            pl.when((pl.col("POIDS").is_null()) | (pl.col("POIDS") == 0))
            .then(None)
            .otherwise(pl.col("POIDS"))
            .alias("vehicle_heavyweight_max_weight"),
            pl.when((pl.col("HAUTEUR").is_null()) | (pl.col("HAUTEUR") == 0))
            .then(None)
            .otherwise(pl.col("HAUTEUR"))
            .alias("vehicle_max_height"),
            pl.when((pl.col("LARGEUR").is_null()) | (pl.col("LARGEUR") == 0))
            .then(None)
            .otherwise(pl.col("LARGEUR"))
            .alias("vehicle_max_width"),
        ]
    )

    df = df.with_columns(
        pl.col("DESCRIPTIF")
        .map_elements(lambda x: exempted_types_mapping.get(x), return_dtype=pl.List(pl.Utf8))
        .alias("_config_exempted_types")
    )

    def build_exempted_types(config_types, cyclo, velo):
        if config_types is not None:
            return config_types
        types = []
        if cyclo:
            types.append("other")
        if velo:
            types.append("bicycle")
        return types if types else None

    df = df.with_columns(
        pl.struct(["_config_exempted_types", "CYCLO", "VELO"])
        .map_elements(
            lambda row: build_exempted_types(
                row["_config_exempted_types"], row["CYCLO"], row["VELO"]
            ),
            return_dtype=pl.List(pl.Utf8),
        )
        .alias("vehicle_exempted_types")
    )

    def compute_other_text(exempted_types):
        if exempted_types is None or len(exempted_types) == 0:
            return None
        if "other" in exempted_types:
            return "cyclomoteur"
        return "autres véhicules autorisés"

    df = df.with_columns(
        pl.col("vehicle_exempted_types")
        .map_elements(compute_other_text, return_dtype=pl.Utf8)
        .alias("vehicle_other_exempted_type_text")
    )

    df = df.with_columns(
        pl.when(pl.col("vehicle_heavyweight_max_weight").is_not_null())
        .then(pl.lit(["heavyGoodsVehicle"]))
        .otherwise(None)
        .alias("vehicle_restricted_types")
    )

    df = df.with_columns(
        pl.when(pl.col("vehicle_restricted_types").is_not_null())
        .then(False)
        .otherwise(True)
        .alias("vehicle_all_vehicles")
    )

    return df.drop("_config_exempted_types")
