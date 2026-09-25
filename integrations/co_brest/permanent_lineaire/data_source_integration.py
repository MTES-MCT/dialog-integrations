import json
import re
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
from integrations.shared.time_slots import _clock, to_iso_slots

from .schema import Schema

# URL = "https://echanges.brest-metropole.fr/VIPDU72/GPB/DEP_ARR_CIRC_STAT_L_V.zip"
URL = "https://www.data.gouv.fr/api/1/datasets/r/760ac62d-b3aa-4d30-898c-94fea81e4537"
FILENAME = "DEP_ARR_CIRC_STAT_L_V.shp"

transformer = Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)


class C(NamedTuple):
    measure_type: MTE
    exempted_types: list[str] | None = None
    # Aimed at the vehicles over a weight, height or width: without POIDS, HAUTEUR or
    # LARGEUR it would be published for every vehicle.
    needs_threshold: bool = False


DESCRIPTION_CONFIG = {
    # Limitations de vitesse
    "Limitation Vitesse": C(MTE.SPEEDLIMITATION),
    # Stationnement
    "Stationnement interdit": C(MTE.PARKINGPROHIBITED),
    "Arrêt interdit": C(MTE.PARKINGPROHIBITED),
    "Stationnement gênant": C(MTE.PARKINGPROHIBITED),
    "Stationnement interdit aux poids-lourds": C(MTE.PARKINGPROHIBITED, needs_threshold=True),
    # noEntry – limitations dimensionnelles (poids / hauteur)
    "Limitation Poids": C(MTE.NOENTRY, needs_threshold=True),
    "Limitation Hauteur": C(MTE.NOENTRY, needs_threshold=True),
    "Interdit aux transports de marchandises": C(MTE.NOENTRY, needs_threshold=True),
    # noEntry – catégories particulières
    "Interdit dans les 2 sens": C(MTE.NOENTRY),
    "Interdit à  tous véhicules à moteur": C(MTE.NOENTRY, ["bicycle", "pedestrians"]),
    "Interdit aux véhicules à moteur sauf cyclos": C(
        MTE.NOENTRY, ["bicycle", "pedestrians", "other"]
    ),
    "Limitation Largeur": C(MTE.NOENTRY, needs_threshold=True),
    "Sens interdit / Sens unique": C(MTE.NOENTRY),
}

# CONDITION and DESCR nuance the restriction in free text. They are read in full or the
# row is dropped (R-78): three wordings are understood, and once they are removed only
# punctuation may remain. No grace in characters: on the 2026-09-24 feed the shortest
# leftovers are "30" and "sauf", and both mean something.
# "Desserte riveraine" is the producer's other word for it (Thibaut, 2026-09-25).
LOCAL_ACCESS = re.compile(
    r"(?:sauf|excepté)\s+(?:la\s+)?desserte\s+(?:locale|riveraine)", re.IGNORECASE
)
TIME_SLOT = re.compile(
    r"(?:interdit\s+)?(?:de|entre)\s+(\d{1,2})\s*h\s*(\d{2})?\s*(?:à|et)\s+(\d{1,2})\s*h\s*(\d{2})?",
    re.IGNORECASE,
)
# Notes that add nothing to what is published: parking "on the carriageway" is what a
# road location already says, and a greenway is what a ban on motor vehicles makes.
NEUTRAL_NOTE = re.compile(r"interdit\s+sur\s+chaussée|voie\s+verte", re.IGNORECASE)

TEXT_READING = pl.Struct(
    {
        "fully_read": pl.Boolean,
        "local_access": pl.Boolean,
        "time_slots": pl.List(pl.List(pl.Utf8)),
    }
)


def read_free_text(*texts: str | None) -> dict:
    """What CONDITION and DESCR say: "sauf desserte locale" (or "riveraine"), daily
    slots such as "interdit de 22H à 7H", and whether nothing else is written."""
    fully_read, local_access, slots = True, False, []
    for text in texts:
        text = text or ""
        local_access |= LOCAL_ACCESS.search(text) is not None
        for match in TIME_SLOT.finditer(text):
            slot = [_clock(match[1], match[2]), _clock(match[3], match[4])]
            if None in slot or slot[0] == slot[1]:
                fully_read = False
            elif slot not in slots:
                slots.append(slot)
        residual = NEUTRAL_NOTE.sub("", TIME_SLOT.sub("", LOCAL_ACCESS.sub("", text)))
        fully_read &= re.fullmatch(r"[\W_]*", residual) is not None
    return {"fully_read": fully_read, "local_access": local_access, "time_slots": slots}


def text_reading(field: str) -> pl.Expr:
    return (
        pl.struct(["CONDITION", "DESCR"])
        .map_elements(
            lambda row: read_free_text(row["CONDITION"], row["DESCR"]),
            return_dtype=TEXT_READING,
        )
        .struct.field(field)
    )


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
            .pipe(discard_misleading_rows)
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
    """Permanent period starting on DT_MAT; rows without it are dropped. Daily slots
    come from CONDITION or DESCR ("interdit de 22H à 7H"), anchored on DT_MAT's day."""
    n_null_dt_mat = df.select(pl.col("DT_MAT").is_null().sum()).item()
    if n_null_dt_mat > 0:
        logger.warning(f"Dropping {n_null_dt_mat} rows with null DT_MAT (no start date available)")

    df = df.filter(pl.col("DT_MAT").is_not_null())

    return df.with_columns(
        [
            start_of_local_day(df, "DT_MAT").alias("period_start_date"),
            pl.struct(pl.col("DT_MAT"), text_reading("time_slots").alias("slots"))
            .map_elements(
                lambda row: to_iso_slots(row["DT_MAT"].date(), row["slots"]) or None,
                return_dtype=pl.List(pl.Struct({"start_time": pl.Utf8, "end_time": pl.Utf8})),
            )
            .alias("period_time_slots"),
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

    Drops: DESCRIPTIF outside DESCRIPTION_CONFIG; speed limitations without a positive
    VITEMAX.
    """
    type_mapping = {
        descriptif: config.measure_type.value for descriptif, config in DESCRIPTION_CONFIG.items()
    }

    df = df.filter(pl.col("DESCRIPTIF").is_in(DESCRIPTION_CONFIG.keys())).with_columns(
        pl.col("DESCRIPTIF").replace(type_mapping).alias("measure_type_")
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


def discard_misleading_rows(df: pl.DataFrame) -> pl.DataFrame:
    """Drop the rows DiaLog would publish wrong; each motive is counted on the whole input
    (R-79), a row can fall under several.

    - "Sens interdit / Sens unique" is a one-way street whatever SENS says (orders
      07700 and 07710, audit of 2026-09-23): as a rawGeoJSON location it would be
      published as a street closed both ways. One-way data is frozen until DiaLog
      carries the direction (R-32).
    - SENS=1 is a restriction in one direction only: published in both (R-32).
    - Free text in CONDITION or DESCR that says more than `read_free_text` understands:
      a direction, another exemption, days of the week… (R-78).
    - A weight, height or width restriction without its threshold would apply to every
      vehicle (R-35).
    """
    has_threshold = pl.any_horizontal(
        pl.col(column).fill_null(0) > 0 for column in ("POIDS", "HAUTEUR", "LARGEUR")
    )
    needs_threshold = [
        descriptif for descriptif, config in DESCRIPTION_CONFIG.items() if config.needs_threshold
    ]
    motives = {
        "one-way street (R-32)": pl.col("DESCRIPTIF") == "Sens interdit / Sens unique",
        "one direction only, SENS=1 (R-32)": pl.col("SENS").fill_null(0) == 1,
        "free text in CONDITION or DESCR not read in full (R-78)": ~text_reading("fully_read"),
        "vehicle restriction without threshold (R-35)": (
            pl.col("DESCRIPTIF").is_in(needs_threshold) & ~has_threshold
        ),
    }

    counts = df.select(**{motive: rule.sum() for motive, rule in motives.items()}).row(
        0, named=True
    )
    discarded = pl.any_horizontal(motives.values())
    logger.warning(
        f"Discarding {df.select(discarded.sum()).item()}/{df.height} rows DiaLog would "
        f"publish wrong: {counts}"
    )
    return df.filter(~discarded)


def compute_vehicle_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Dimensions from POIDS / HAUTEUR / LARGEUR (0 means none); exemptions from
    DESCRIPTION_CONFIG, else from the CYCLO (-> other) and VELO (-> bicycle) flags, plus
    desserteLocale when CONDITION or DESCR says "sauf desserte locale" (or "riveraine");
    heavyGoodsVehicle for a weight limit, dimensions for a height or width limit (R-33),
    all vehicles otherwise."""
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
        .alias("_config_exempted_types"),
        text_reading("local_access").alias("_local_access"),
    )

    def build_exempted_types(config_types, cyclo, velo, local_access):
        if config_types is not None:
            types = list(config_types)
        else:
            types = []
            if cyclo:
                types.append("other")
            if velo:
                types.append("bicycle")
        if local_access:
            types.append("desserteLocale")
        return types if types else None

    df = df.with_columns(
        pl.struct(["_config_exempted_types", "CYCLO", "VELO", "_local_access"])
        .map_elements(
            lambda row: build_exempted_types(
                row["_config_exempted_types"], row["CYCLO"], row["VELO"], row["_local_access"]
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

    has_weight = pl.col("vehicle_heavyweight_max_weight").is_not_null()
    has_size = (
        pl.col("vehicle_max_height").is_not_null() | pl.col("vehicle_max_width").is_not_null()
    )
    df = df.with_columns(
        pl.when(has_weight & has_size)
        .then(pl.lit(["heavyGoodsVehicle", "dimensions"]))
        .when(has_weight)
        .then(pl.lit(["heavyGoodsVehicle"]))
        .when(has_size)
        .then(pl.lit(["dimensions"]))
        .otherwise(None)
        .alias("vehicle_restricted_types")
    )

    df = df.with_columns(
        pl.when(pl.col("vehicle_restricted_types").is_not_null())
        .then(False)
        .otherwise(True)
        .alias("vehicle_all_vehicles")
    )

    return df.drop("_config_exempted_types", "_local_access")
