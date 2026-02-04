import json
import tempfile
import zipfile
from pathlib import Path
from typing import Any, NamedTuple

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
    PostApiRegulationsAddBodyStatus,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
    SaveMeasureDTO,
    SaveVehicleSetDTO,
)
from integrations.co_brest.schema import (
    BrestMeasure,
    BrestRawDataSchema,
)
from integrations.shared import DialogIntegration

URL = "https://www.data.gouv.fr/api/1/datasets/r/3ca7bd06-6489-45a2-aee9-efc6966121b2"
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


class Integration(DialogIntegration):
    status = PostApiRegulationsAddBodyStatus.PUBLISHED
    raw_data_schema = BrestRawDataSchema

    def fetch_raw_data(self) -> pl.DataFrame:
        logger.info(f"Downloading and reading shapefile data from {URL}")
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = Path(tmpdir) / "data.zip"

            # download
            r = requests.get(URL)
            r.raise_for_status()
            zip_path.write_bytes(r.content)
            logger.info(f"Downloaded zip file to {zip_path}")

            # unzip
            with zipfile.ZipFile(zip_path) as z:
                z.extractall(tmpdir)

            # find .shp
            shp_path = next(Path(tmpdir).rglob("*.shp"))
            shp_path = Path(tmpdir) / FILENAME

            # read
            logger.info(f"Reading file {shp_path}")
            gdf = gpd.read_file(shp_path)

        # geometry -> WKT pour Polars
        gdf["geometry"] = gdf.geometry.to_wkt()
        return pl.from_pandas(gdf)

    def preprocess_raw_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        """
        Apply Brest-specific preprocessing: cast boolean columns.
        """
        return raw_data.with_columns(
            [
                self.cast_boolean_column("CYCLO"),
                self.cast_boolean_column("VELO"),
            ]
        ).filter(~(pl.col("NOARR").eq("")))

    def compute_clean_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        return (
            raw_data.pipe(compute_measure_type)
            .pipe(compute_save_period_fields)
            .pipe(compute_save_location_fields)
            .pipe(self.compute_regulation_fields)
            .pipe(compute_measure_max_speed)
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

    def create_measure(self, measure: BrestMeasure) -> SaveMeasureDTO:
        cfg = DESCRIPTION_CONFIG.get(measure["DESCRIPTIF"])

        params = {
            "type_": MTE(measure["measure_type_"]),
            "periods": [self.create_save_period_dto(measure)],  # type: ignore
            "locations": [self.create_save_location_dto(measure)],  # type: ignore
            "vehicle_set": self.create_save_vehicle_dto(
                measure, cfg.exempted_types if cfg else None, None
            ),
        }

        # Add max_speed for SPEEDLIMITATION measures
        if measure["measure_type_"] == MTE.SPEEDLIMITATION.value:
            params["max_speed"] = measure["measure_max_speed"]

        return SaveMeasureDTO(**params)

    def create_save_vehicle_dto(
        self,
        measure: BrestMeasure,
        exempted_types: list[str] | None,
        restricted_types: list[str] | None,
    ) -> SaveVehicleSetDTO:
        def to_float(val):
            if val is None or val == 0:
                return None
            try:
                return float(val)
            except Exception:
                return None

        # Dimensions
        poids = to_float(measure.get("POIDS"))
        hauteur = to_float(measure.get("HAUTEUR"))
        largeur = to_float(measure.get("LARGEUR"))

        # Start building params
        params: dict[str, Any] = {
            "all_vehicles": True,
            "heavyweight_max_weight": poids,
            "max_height": hauteur,
            "max_width": largeur,
            "exempted_types": exempted_types,
            "restricted_types": restricted_types,
        }

        # Auto-fill exempted types from columns
        if params["exempted_types"] is None:
            params["exempted_types"] = []
            if measure.get("CYCLO") is True:
                params["exempted_types"].append("other")
            if measure.get("VELO") is True:
                params["exempted_types"].append("bicycle")

        if params["exempted_types"]:
            if "other" in params["exempted_types"]:
                params["other_exempted_type_text"] = "cyclomoteur"
            else:
                params["other_exempted_type_text"] = "autres véhicules autorisés"

        if params["heavyweight_max_weight"]:
            params["restricted_types"] = ["heavyGoodsVehicle"]

        # If we have dimensions or exemptions or restrictions → not all vehicles
        if params["restricted_types"]:
            params["all_vehicles"] = False
        else:
            params["all_vehicles"] = True

        # Clean params: remove empty lists / None
        cleaned = {k: v for k, v in params.items() if v not in (None, [], {})}

        return SaveVehicleSetDTO(**cleaned)

    def compute_regulation_fields(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Compute all regulation fields for PostApiRegulationsAddBody.
        For Brest, each NOARR (regulation ID) can have multiple measures.
        Regulation title is built from first row's DESCRIPTIF and LIBRU.
        - regulation_identifier: from NOARR field
        - regulation_status: from self.status
        - regulation_category: PERMANENTREGULATION
        - regulation_subject: OTHER
        - regulation_title: "{DESCRIPTIF} – {LIBRU}"
        - regulation_other_category_text: "Circulation"
        """
        # For each NOARR, we need the first row's DESCRIPTIF and LIBRU for the title
        # Add a row number per NOARR group to identify first row
        df = df.with_columns(
            pl.col("NOARR").cum_count().over("NOARR").alias("_row_num_in_regulation")
        )

        # Get the first row's title for each regulation
        first_row_titles = df.filter(pl.col("_row_num_in_regulation") == 1).select(
            [
                pl.col("NOARR"),
                (pl.col("DESCRIPTIF") + pl.lit(" – ") + pl.col("LIBRU")).alias("regulation_title"),
            ]
        )

        # Join back to get title for all rows
        df = df.join(first_row_titles, on="NOARR", how="left")

        # Add regulation fields
        df = df.with_columns(
            [
                pl.col("NOARR").alias("regulation_identifier"),
                pl.lit(self.status.value).alias("regulation_status"),
                pl.lit(PostApiRegulationsAddBodyCategory.PERMANENTREGULATION.value).alias(
                    "regulation_category"
                ),
                pl.lit(PostApiRegulationsAddBodySubject.OTHER.value).alias("regulation_subject"),
                pl.lit("Circulation").alias("regulation_other_category_text"),
            ]
        )

        num_null_titles = df.select(pl.col("regulation_title").is_null().sum()).item()
        logger.warning(f"Dropping {num_null_titles} rows with null regulation_title")
        df = df.filter(pl.col("regulation_title").is_not_null())

        # Drop helper columns
        return df


def compute_save_period_fields(df: pl.DataFrame) -> pl.DataFrame:
    """
    Compute all period fields for SavePeriodDTO.
    - period_start_date: from DT_MAT field
    - period_end_date, period_start_time, period_end_time: None
    - period_recurrence_type: EVERYDAY
    - period_is_permanent: True
    Filter out rows where DT_MAT is null.
    """
    # Count rows with null DT_MAT before filtering
    n_null_dt_mat = df.select(pl.col("DT_MAT").is_null().sum()).item()
    if n_null_dt_mat > 0:
        logger.warning(f"Dropping {n_null_dt_mat} rows with null DT_MAT (no start date available)")

    # Filter out rows where DT_MAT is null
    df = df.filter(pl.col("DT_MAT").is_not_null())

    # Compute all period fields
    return df.with_columns(
        [
            pl.col("DT_MAT").dt.strftime("%Y-%m-%dT%H:%M:%SZ").alias("period_start_date"),
            pl.lit(None).alias("period_end_date"),
            pl.lit(None).alias("period_start_time"),
            pl.lit(None).alias("period_end_time"),
            pl.lit("everyDay").alias("period_recurrence_type"),
            pl.lit(True).alias("period_is_permanent"),
        ]
    )


def compute_save_location_fields(df: pl.DataFrame) -> pl.DataFrame:
    """
    Compute all location fields for SaveLocationDTO.
    - location_road_type: always RoadTypeEnum.RAWGEOJSON for Brest
    - location_label: from LIBCO and LIBRU fields
    - location_geometry: from geometry field (WKT) transformed to GeoJSON (WGS84)
    Filter out rows where geometry is null.
    """
    # Count rows with null geometry before filtering
    n_null_geometry = df.select(pl.col("geometry").is_null().sum()).item()
    if n_null_geometry > 0:
        logger.warning(f"Dropping {n_null_geometry} rows with null geometry")

    # Filter out rows where geometry is null
    df = df.filter(pl.col("geometry").is_not_null())

    # Transform geometries using geopandas (thread-safe approach)
    # Convert to pandas to work with geopandas
    pdf = df.to_pandas()

    # Create GeoDataFrame from WKT
    gdf = gpd.GeoDataFrame(pdf, geometry=gpd.GeoSeries.from_wkt(pdf["geometry"]), crs="EPSG:2154")

    # Reproject to WGS84
    gdf = gdf.to_crs("EPSG:4326")

    # Convert geometry to GeoJSON string
    pdf["location_geometry"] = gdf.geometry.apply(lambda geom: json.dumps(mapping(geom)))

    # Convert back to Polars
    df = pl.from_pandas(pdf)

    return df.with_columns(
        [
            # Road type (always RAWGEOJSON as enum string value)
            pl.lit(RoadTypeEnum.RAWGEOJSON.value).alias("location_road_type"),
            # Label from LIBCO and LIBRU
            (pl.col("LIBCO") + pl.lit(" – ") + pl.col("LIBRU")).alias("location_label"),
            # location_geometry already computed above
        ]
    )


def compute_measure_type(df: pl.DataFrame) -> pl.DataFrame:
    """
    Compute measure_type_ field from DESCRIPTIF using DESCRIPTION_CONFIG.
    """
    # Create mapping dict from DESCRIPTIF to measure type enum value
    type_mapping = {
        descriptif: config.measure_type.value for descriptif, config in DESCRIPTION_CONFIG.items()
    }
    return (
        df.filter(pl.col("DESCRIPTIF").is_in(DESCRIPTION_CONFIG.keys()))
        .filter(~(pl.col("DESCRIPTIF").eq("Sens interdit / Sens unique") & pl.col("SENS").eq(1)))
        .with_columns(pl.col("DESCRIPTIF").replace(type_mapping).alias("measure_type_"))
    )


def compute_measure_max_speed(df: pl.DataFrame) -> pl.DataFrame:
    """
    Compute measure_max_speed field from VITEMAX with validation.
    - For SPEEDLIMITATION: use VITEMAX, must be not null and > 0
    - For other types: set to None
    Filters out SPEEDLIMITATION rows with invalid VITEMAX.
    """
    # Filter out invalid speed limitations
    invalid_speed = (pl.col("measure_type_") == MTE.SPEEDLIMITATION.value) & (
        (pl.col("VITEMAX").is_null()) | (pl.col("VITEMAX") <= 0)
    )
    n_invalid = df.select(invalid_speed.sum()).item()
    if n_invalid > 0:
        logger.warning(f"Dropping {n_invalid} SPEEDLIMITATION measures with invalid VITEMAX")

    df = df.filter(~invalid_speed)

    # Compute max_speed: use VITEMAX for SPEEDLIMITATION, None otherwise
    return df.with_columns(
        pl.when(pl.col("measure_type_") == MTE.SPEEDLIMITATION.value)
        .then(pl.col("VITEMAX"))
        .otherwise(None)
        .alias("measure_max_speed")
    )
