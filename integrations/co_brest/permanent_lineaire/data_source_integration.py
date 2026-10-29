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
from canonical.source import BaseDataSourceIntegration
from toolkit.connectors.download import DOWNLOAD_TIMEOUT
from toolkit.rules.location import one_direction, one_way_street_named
from toolkit.rules.vehicles import pedestrians_or_cycles_named
from toolkit.transforms.local_time import start_of_local_day
from toolkit.transforms.periods import permanent_period
from toolkit.transforms.time_slots import _clock, to_iso_slots
from toolkit.transforms.vehicle_types import restricted_types_from_limits

from .schema import Schema

# URL = "https://echanges.brest-metropole.fr/VIPDU72/GPB/DEP_ARR_CIRC_STAT_L_V.zip"
URL = "https://www.data.gouv.fr/api/1/datasets/r/760ac62d-b3aa-4d30-898c-94fea81e4537"

transformer = Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)


class C(NamedTuple):
    measure_type: MTE
    exempted_types: list[str] | None = None
    threshold_column: str | None = None


DESCRIPTION_CONFIG = {
    "Limitation Vitesse": C(MTE.SPEEDLIMITATION),
    "Stationnement interdit": C(MTE.PARKINGPROHIBITED),
    "Arrêt interdit": C(MTE.PARKINGPROHIBITED),
    "Stationnement gênant": C(MTE.PARKINGPROHIBITED),
    "Stationnement interdit aux poids-lourds": C(MTE.PARKINGPROHIBITED, threshold_column="POIDS"),
    "Limitation Poids": C(MTE.NOENTRY, threshold_column="POIDS"),
    "Limitation Hauteur": C(MTE.NOENTRY, threshold_column="HAUTEUR"),
    "Interdit aux transports de marchandises": C(MTE.NOENTRY, threshold_column="POIDS"),
    "Interdit dans les 2 sens": C(MTE.NOENTRY),
    "Interdit à  tous véhicules à moteur": C(MTE.NOENTRY, ["bicycle", "pedestrians"]),
    "Interdit aux véhicules à moteur sauf cyclos": C(
        MTE.NOENTRY, ["bicycle", "pedestrians", "other"]
    ),
    "Limitation Largeur": C(MTE.NOENTRY, threshold_column="LARGEUR"),
    "Sens interdit / Sens unique": C(MTE.NOENTRY),
}

PEDESTRIANS_OR_CYCLES = pedestrians_or_cycles_named(pl.col("DESCRIPTIF"))
# SENS 0 is both ways.
ONE_DIRECTION = one_direction(pl.col("SENS"), 0)
# One-way whatever SENS says (audit of 2026-09-23).
ONE_WAY_STREET = one_way_street_named(pl.col("DESCRIPTIF"))

LIMITS = [
    descriptif for descriptif, config in DESCRIPTION_CONFIG.items() if config.threshold_column
]

# CONDITION and DESCR are read in full or the row is dropped: once the wordings below
# are removed, only punctuation may remain. Even "30" or "sauf" means something (2026-09-24).
# "Riveraine" is the producer's other word for it (confirmed 2026-09-25).
LOCAL_ACCESS = re.compile(
    r"(?:sauf|excepté)\s+(?:la\s+)?desserte\s+(?:locale|riveraine)", re.IGNORECASE
)
TIME_SLOT = re.compile(
    r"(?:interdit\s+)?(?:de|entre)\s+(\d{1,2})\s*h(\d{2})?\s*(?:à|et)\s+(\d{1,2})\s*h(\d{2})?",
    re.IGNORECASE,
)
# Already said by a road location ("on the carriageway") or by a ban on motor vehicles.
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
    name = "permanent_lineaire"
    raw_data_schema = Schema

    def fetch_raw_data(self) -> pl.DataFrame:
        logger.info(f"Downloading and reading shapefile data from {URL}")
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = Path(tmpdir) / "data.zip"

            r = requests.get(URL, timeout=DOWNLOAD_TIMEOUT)
            r.raise_for_status()
            zip_path.write_bytes(r.content)
            logger.info(f"Downloaded zip file to {zip_path}")

            with zipfile.ZipFile(zip_path) as z:
                z.extractall(tmpdir)

            shp_path = next(Path(tmpdir).rglob("*.shp"))

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
            raw_data.pipe(self.apply_rules, PEDESTRIANS_OR_CYCLES, ONE_WAY_STREET)
            .pipe(compute_measure_fields)
            .pipe(self.apply_rules, ONE_DIRECTION)
            .pipe(discard_misleading_rows)
            .pipe(compute_period_fields)
            .pipe(compute_location_fields)
            .pipe(compute_vehicle_fields)
            # Before the regulation: its title is its first row's.
            .pipe(self.apply_rules)
            .pipe(compute_regulation_fields)
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
    if num_null_titles:
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
            *permanent_period(start_of_local_day(df, "DT_MAT")),
            pl.struct(pl.col("DT_MAT"), text_reading("time_slots").alias("slots"))
            .map_elements(
                lambda row: to_iso_slots(row["DT_MAT"].date(), row["slots"]) or None,
                return_dtype=pl.List(pl.Struct({"start_time": pl.Utf8, "end_time": pl.Utf8})),
            )
            .alias("period_time_slots"),
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
    """Measure type from DESCRIPTIF, speed from VITEMAX; unknown types and speed
    limitations without a positive VITEMAX are dropped."""
    type_mapping = {
        descriptif: config.measure_type.value for descriptif, config in DESCRIPTION_CONFIG.items()
    }

    known = pl.col("DESCRIPTIF").is_in(DESCRIPTION_CONFIG.keys())
    unknown = df.filter(~known)
    if unknown.height:
        counts = dict(unknown["DESCRIPTIF"].value_counts(sort=True).iter_rows())
        logger.warning(f"Dropping {unknown.height} rows without a DiaLog measure type: {counts}")
    df = df.filter(known).with_columns(
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
    """Drop the rows DiaLog would publish wrong; each motive is counted on the whole input,
    not on what the previous ones left, so a row can count under several.

    A rawGeoJSON location carries no direction: a missing SENS may be one way, and would
    come out both ways.
    """
    motives = {
        "no SENS": pl.col("SENS").is_null(),
        "free text in CONDITION or DESCR not read in full": ~text_reading("fully_read"),
    }

    counts = df.select(**{motive: rule.sum() for motive, rule in motives.items()}).row(
        0, named=True
    )
    discarded = pl.any_horizontal(motives.values())
    if n_discarded := df.select(discarded.sum()).item():
        logger.warning(
            f"Discarding {n_discarded}/{df.height} rows DiaLog would publish wrong: {counts}"
        )
    return df.filter(~discarded)


def threshold(column: str) -> pl.Expr:
    """The column's limit, 0 meaning none. A limit label without its own threshold carries
    none: a weight on a height limit is a slip, not the limit."""
    lacks_own = pl.any_horizontal(
        (pl.col("DESCRIPTIF") == descriptif) & ~(pl.col(config.threshold_column).fill_null(0) > 0)
        for descriptif, config in DESCRIPTION_CONFIG.items()
        if config.threshold_column
    )
    value = pl.col(column)
    return pl.when(value.is_null() | (value == 0) | lacks_own).then(None).otherwise(value.round(2))


def compute_vehicle_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Dimensions (0 means none), exemptions and restricted types."""
    exempted_types_mapping = {
        descriptif: config.exempted_types for descriptif, config in DESCRIPTION_CONFIG.items()
    }

    # The producer writes some values through float32 (1.9 m as 1.89999998, 2026-09-28),
    # and DiaLog keeps them as sent.
    df = df.with_columns(
        [
            threshold("POIDS").alias("vehicle_heavyweight_max_weight"),
            threshold("HAUTEUR").alias("vehicle_max_height"),
            threshold("LARGEUR").alias("vehicle_max_width"),
            # No length in the layer; set, so the rules judge the row before it is grouped.
            pl.lit(None, dtype=pl.Float64).alias("vehicle_max_length"),
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

    # The API reads the text only next to `other`.
    def compute_other_text(exempted_types):
        return "cyclomoteur" if exempted_types is not None and "other" in exempted_types else None

    df = df.with_columns(
        pl.col("vehicle_exempted_types")
        .map_elements(compute_other_text, return_dtype=pl.Utf8)
        .alias("vehicle_other_exempted_type_text")
    )

    df = df.with_columns(
        restricted_types_from_limits(
            pl.col("vehicle_heavyweight_max_weight"),
            pl.col("vehicle_max_height"),
            pl.col("vehicle_max_width"),
        ).alias("vehicle_restricted_types")
    )

    df = df.with_columns(
        # A weight or size limit never targets every vehicle, its threshold missing or not.
        (pl.col("vehicle_restricted_types").is_null() & ~pl.col("DESCRIPTIF").is_in(LIMITS)).alias(
            "vehicle_all_vehicles"
        )
    )

    return df.drop("_config_exempted_types", "_local_access")
