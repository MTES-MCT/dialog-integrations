import json

import httpx
import polars as pl
from loguru import logger
from shapely.geometry import Point, mapping

from api.dia_log_client.models import (
    MeasureTypeEnum,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
)
from integrations.base_data_source_integration import BaseDataSourceIntegration

from .schema import IssylesMoulineauxTravauxRawDataSchema

TRAVAUX_VOIRIE_ENDPOINT = (
    "https://data.issy.com/api/explore/v2.1/catalog/datasets/travaux-voirie/records"
)

MEASURE_TYPE_BY_LABEL = {
    "Barrage de voie": MeasureTypeEnum.NOENTRY.value,
    "Circulation alternée": MeasureTypeEnum.ALTERNATEROAD.value,
    "Stationnement gênant": MeasureTypeEnum.PARKINGPROHIBITED.value,
    "Limitation vitesse": MeasureTypeEnum.SPEEDLIMITATION.value,
}

PUBLISHED_MEASURE_TYPES = [
    MeasureTypeEnum.PARKINGPROHIBITED.value,
]

TITLE_MAX_LENGTH = 255
TITLE_ELLIPSIS = "..."


class DataSourceIntegration(BaseDataSourceIntegration):
    raw_data_schema = IssylesMoulineauxTravauxRawDataSchema
    name = "travaux_voirie"

    def fetch_raw_data(self):
        records = []
        offset = 0
        limit = 100

        while True:
            response = httpx.get(
                TRAVAUX_VOIRIE_ENDPOINT,
                params={"limit": limit, "offset": offset},
            )
            response.raise_for_status()
            data = response.json()
            records.extend(data["results"])

            if len(data["results"]) < limit:
                break
            offset += limit

        return pl.DataFrame(records)

    def preprocess_raw_data(self, raw_data):
        """
        Recast raw types into augmented ones when possible :
        str -> date
        bytes -> pl.Struct
        """
        return raw_data.with_columns(
            [
                pl.col("date_debut").cast(pl.Utf8).str.to_date("%Y-%m-%d"),
                pl.col("date_fin").cast(pl.Utf8).str.to_date("%Y-%m-%d"),
                pl.col("url").cast(pl.Utf8),
            ]
        )

    def compute_clean_data(self, raw_data):
        return (
            raw_data.pipe(compute_measure_fields)
            .pipe(compute_period_fields)
            .pipe(compute_location_fields)
            .pipe(compute_regulation_fields)
            .pipe(compute_vehicle_fields)
        )


def compute_measure_fields(df: pl.DataFrame) -> pl.DataFrame:
    no_measure = df.select(pl.col("mesure_titre").is_null().sum()).item()
    if no_measure:
        logger.warning(f"Dropping {no_measure} rows without any mesure_titre")
    df = df.filter(pl.col("mesure_titre").is_not_null())

    df = df.explode(["mesure_titre", "mesures"])

    df = df.with_columns(
        [
            pl.col("mesure_titre")
            .replace_strict(MEASURE_TYPE_BY_LABEL, default=None, return_dtype=pl.Utf8)
            .alias("measure_type_"),
            pl.col("mesures")
            .str.extract(r"(\d+)\s*km/h", 1)
            .cast(pl.Int32)
            .alias("measure_max_speed"),
        ]
    )

    unmapped = (
        df.filter(pl.col("measure_type_").is_null())
        .get_column("mesure_titre")
        .value_counts(sort=True)
    )
    if unmapped.height:
        logger.warning(
            f"Dropping {unmapped.get_column('count').sum()} measures without DiaLog equivalent: "
            f"{dict(zip(unmapped.get_column('mesure_titre'), unmapped.get_column('count')))}"
        )
    df = df.filter(pl.col("measure_type_").is_not_null())

    is_published = pl.col("measure_type_").is_in(PUBLISHED_MEASURE_TYPES)
    unsupported = df.filter(~is_published).get_column("measure_type_")
    if unsupported.len():
        counts = dict(unsupported.value_counts(sort=True).iter_rows())
        logger.warning(f"Dropping {unsupported.len()} measures requiring a segment: {counts}")
    return df.filter(is_published)


def compute_period_fields(df: pl.DataFrame) -> pl.DataFrame:
    no_end = df.select(pl.col("date_fin").is_null().sum()).item()
    if no_end:
        logger.warning(f"Dropping {no_end} measures without an end date")
    df = df.filter(pl.col("date_fin").is_not_null())

    return df.with_columns(
        [
            pl.col("date_debut").dt.strftime("%Y-%m-%dT00:00:00Z").alias("period_start_date"),
            pl.col("date_fin").dt.strftime("%Y-%m-%dT00:00:00Z").alias("period_end_date"),
            pl.col("date_debut").dt.strftime("%Y-%m-%dT00:00:00Z").alias("period_start_time"),
            pl.col("date_fin").dt.strftime("%Y-%m-%dT00:00:00Z").alias("period_end_time"),
            pl.lit("everyDay").alias("period_recurrence_type"),
            pl.lit(False).alias("period_is_permanent"),
        ]
    )


def compute_location_fields(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        [
            pl.col("geolocalisation").struct.field("lon").alias("lon"),
            pl.col("geolocalisation").struct.field("lat").alias("lat"),
        ]
    )

    has_point = pl.col("lon").is_not_null() & pl.col("lat").is_not_null()
    missing_geo = df.select((~has_point).sum()).item()
    if missing_geo:
        logger.warning(f"Dropping {missing_geo} rows due to missing geolocalisation")
    df = df.filter(has_point)

    # EPSG:4326: longitude, latitude.
    geometry = pl.Series(
        "location_geometry",
        [json.dumps(mapping(Point(lon, lat))) for lon, lat in zip(df["lon"], df["lat"])],
        dtype=pl.Utf8,
    )

    return df.with_columns(
        [
            pl.lit(RoadTypeEnum.RAWGEOJSON.value).alias("location_road_type"),
            (pl.col("rue_principal") + pl.lit(" - ") + pl.col("commune")).alias("location_label"),
            geometry,
        ]
    ).drop(["lon", "lat"])


def compute_regulation_fields(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        [
            pl.col("reference").alias("regulation_identifier"),
            pl.lit(PostApiRegulationsAddBodyCategory.TEMPORARYREGULATION.value).alias(
                "regulation_category"
            ),
            pl.lit(PostApiRegulationsAddBodySubject.ROADMAINTENANCE.value).alias(
                "regulation_subject"
            ),
            pl.when(pl.col("description").str.len_chars() > TITLE_MAX_LENGTH)
            .then(
                pl.col("description").str.slice(0, TITLE_MAX_LENGTH - len(TITLE_ELLIPSIS))
                + pl.lit(TITLE_ELLIPSIS)
            )
            .otherwise(pl.col("description"))
            .alias("regulation_title"),
            pl.col("type_travaux").alias("regulation_other_category_text"),
            pl.col("url").alias("regulation_document_url"),
        ]
    )


def compute_vehicle_fields(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        [
            pl.lit(True).alias("vehicle_all_vehicles"),
        ]
    )
