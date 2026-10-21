import io
import json

import geopandas as gpd
import polars as pl
import requests
from loguru import logger
from shapely.geometry import mapping

from api.dia_log_client.models import (
    MeasureTypeEnum,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
)
from integrations.base_data_source_integration import BaseDataSourceIntegration

from .schema import AngersTravauxVoirieRawDataSchema

URL = (
    "https://data.angers.fr/"
    "api/explore/v2.1/catalog/datasets/"
    "info-travaux/exports/parquet?lang=fr&timezone=Europe%2FBerlin"
)

MODE = "remote"


class DataSourceIntegration(BaseDataSourceIntegration):
    raw_data_schema = AngersTravauxVoirieRawDataSchema
    name = "travaux_voirie"

    def fetch_raw_data(self):
        logger.info(f"Downloading data from {URL}")

        r = requests.get(URL)
        r.raise_for_status()

        return pl.read_parquet(io.BytesIO(r.content))

    def compute_clean_data(self, raw_data):
        return (
            raw_data.pipe(compute_measure_fields)
            .pipe(compute_period_fields)
            .pipe(compute_location_fields)
            .pipe(compute_regulation_fields)
            .pipe(compute_vehicle_fields)
        )


def compute_measure_fields(df: pl.DataFrame):
    """
    measure_type_ : depends on description
    Excludes : les mesures de type "chausséee rétrécies"
    """

    df = df.with_columns(
        [
            pl.when(pl.col("description").str.contains("Circulation interdite"))
            .then(pl.lit(MeasureTypeEnum.NOENTRY.value))
            .when(pl.col("description").str.contains("Circulation alternée"))
            .then(pl.lit(MeasureTypeEnum.ALTERNATEROAD.value))
            .when(pl.col("description").str.contains("Interdiction de stationnement"))
            .then(pl.lit(MeasureTypeEnum.PARKINGPROHIBITED.value))
            .otherwise(pl.lit(None))
            .alias("measure_type_"),
        ]
    )

    null_measure_type = df.select(pl.col("measure_type_").is_null().sum()).item()
    logger.warning(f"Dropping {null_measure_type} rows due to unable to infer restriction type")
    df = df.filter(pl.col("measure_type_").is_not_null())

    return df


def compute_period_fields(df: pl.DataFrame):
    """
    Compute all period fields for SavePeriodDTO.
    - period_start_date: startat
    - period_end_date: endat
    - period_start_time: startat
    - period_end_time: endat
    - period_recurrence_type: everyDay
    - period_is_permanent: True
    """

    return df.with_columns(
        [
            pl.col("startat").dt.strftime("%Y-%m-%dT00:00:00Z").alias("period_start_date"),
            pl.col("endat").dt.strftime("%Y-%m-%dT00:00:00Z").alias("period_end_date"),
            pl.col("startat").dt.strftime("%Y-%m-%dT00:00:00Z").alias("period_start_time"),
            pl.col("endat").dt.strftime("%Y-%m-%dT00:00:00Z").alias("period_end_time"),
            pl.lit("everyDay").alias("period_recurrence_type"),
            pl.lit(False).alias("period_is_permanent"),
        ]
    )


def compute_location_fields(df: pl.DataFrame) -> pl.DataFrame:
    """
    Compute all location fields for SaveLocationDTO.
    - location_road_type: always RoadTypeEnum.RAWGEOJSON
    - location_label: from address
    - location_geometry: from location
    """

    return df.with_columns(
        [
            pl.lit(RoadTypeEnum.RAWGEOJSON.value).alias("location_road_type"),
            (pl.col("address")).alias("location_label"),
            pl.from_pandas(
                gpd.GeoSeries.from_wkb(df["location"]).apply(
                    lambda geom: json.dumps(mapping(geom))
                )
            ).alias("location_geometry"),  # type: ignore
        ]
    )


def compute_regulation_fields(df: pl.DataFrame) -> pl.DataFrame:
    """
    Compute all regulation fields for PostApiRegulationsAddBody.
    - regulation_identifier: from id
    - regulation_category: TEMPORARYREGULATION
    - regulation_subject: OTHER
    - regulation_title: objectid + nature + site
    - regulation_other_category_text: "Circulation"

    Filters out rows with duplicate objectid.
    """

    return df.with_columns(
        [
            (pl.lit("49007/") + pl.col("id").cast(pl.Utf8) + pl.lit("/TRAVAUX")).alias(
                "regulation_identifier"
            ),
            pl.lit(PostApiRegulationsAddBodyCategory.TEMPORARYREGULATION.value).alias(
                "regulation_category"
            ),
            pl.lit(PostApiRegulationsAddBodySubject.ROADMAINTENANCE.value).alias(
                "regulation_subject"
            ),
            (pl.col("title").fill_null("Travaux").cast(pl.Utf8)).alias(
                "regulation_title"
            ),
            pl.lit("Travaux").alias("regulation_other_category_text"),
        ]
    )


def compute_vehicle_fields(df: pl.DataFrame):
    """
    Compute all vehicle fields for SaveVehicleSetDTO.
    - vehicle_all_vehicles: true
    """
    return df.with_columns([pl.lit(True).alias("vehicle_all_vehicles")])
