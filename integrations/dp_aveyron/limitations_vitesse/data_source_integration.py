"""Data source integration for Aveyron : limitations-de-vitesse-du-departement"""

import io

import polars as pl
import requests
from loguru import logger

from api.dia_log_client.models import (
    DirectionEnum,
    MeasureTypeEnum,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
)
from integrations.base_data_source_integration import BaseDataSourceIntegration
from integrations.dp_aveyron.limitations_vitesse.schema import (
    AveyronLimitationsVitesseRawDataSchema,
)

URL = "https://opendata.aveyron.fr/api/explore/v2.1/catalog/datasets/limitations-de-vitesse-du-departement-aveyron/exports/parquet"

LOCAL_FILE = "explorations/dp_aveyron/data/limitations-de-vitesse-du-departement-aveyron.parquet"


class DataSourceIntegration(BaseDataSourceIntegration):
    """Data source for Limitations de vitesse du département de l'Aveyron"""

    raw_data_schema = AveyronLimitationsVitesseRawDataSchema
    name = "limitation_vitesse"

    def fetch_raw_data(self):
        logger.info(f"Downloading data from {URL}")

        r = requests.get(URL)
        r.raise_for_status()

        df = pl.read_parquet(io.BytesIO(r.content))

        return df

    def compute_clean_data(self, raw_data):
        return (
            raw_data.pipe(compute_measure_fields)
            .pipe(compute_period_fields)
            .pipe(compute_location_fields)
            .pipe(compute_regulation_fields)
            .pipe(compute_vehicle_fields)
        )


def compute_measure_fields(df: pl.DataFrame):
    return df.with_columns(
        [
            pl.lit(MeasureTypeEnum.SPEEDLIMITATION.value).alias("measure_type_"),
            pl.col("limit1").alias("measure_max_speed"),
        ]
    )


def compute_period_fields(df: pl.DataFrame):
    """
    Compute all period fields for SavePeriodDTO.
    - period_start_date: from 024-08-12 (last update of the file)
    - period_end_date: None
    - period_start_time: None
    - period_end_time: None
    - period_recurrence_type: everyDay
    - period_is_permanent: True
    """
    return df.with_columns(
        [
            pl.lit("2024-08-12T02:00:00Z").alias("period_start_date"),
            pl.lit(None).alias("period_end_date"),
            pl.lit(None).alias("period_start_time"),
            pl.lit(None).alias("period_end_time"),
            pl.lit("everyDay").alias("period_recurrence_type"),
            pl.lit(True).alias("period_is_permanent"),
        ]
    )


def compute_location_fields(df: pl.DataFrame):
    """
    Compute all location fields for SaveLocationDTO.
    - location_administrator: "Aveyron"
    - location_road_type: RoadTypeEnum.DEPARTMENTALROAD
    - location_road_number: D98 (Aveyron) du PR 28+881 au PR 32+444
    - location_from_department_code: 12
    - location_from_point_number: from prdeb
    - location_from_abscissa: from absdeb
    - location_from_side: "U"
    - location_to_department_code: 12
    - location_to_point_number: from prfin
    - location_to_abscissa: from absfin
    - location_to_side: "U"
    - location_direction: "BOTH"
    #NOT TRANSMITTTED- location_geometry: from geo_shape
    """

    return df.with_columns(
        [
            pl.lit("Aveyron").alias("location_administrator"),
            pl.lit(RoadTypeEnum.DEPARTMENTALROAD.value).alias("location_road_type"),
            pl.col("idroute").str.split("_").list.last().alias("location_road_number"),
            pl.lit("12").alias("location_from_department_code"),
            pl.col("prdeb").cast(pl.Utf8).alias("location_from_point_number"),
            pl.col("absdeb").alias("location_from_abscissa"),
            pl.lit("U").alias("location_from_side"),
            pl.lit("12").alias("location_to_department_code"),
            pl.col("prfin").cast(pl.Utf8).alias("location_to_point_number"),
            pl.col("absfin").alias("location_to_abscissa"),
            pl.lit("U").alias("location_to_side"),
            pl.lit(DirectionEnum.BOTH.value).alias("location_direction"),
        ]
    )


def compute_regulation_fields(df: pl.DataFrame):
    """
    Compute all regulation fields for PostApiRegulationsAddBody.
    - regulation_identifier: from objectid (filter duplicates)
    - regulation_category: PERMANENTREGULATION
    - regulation_subject: OTHER
    - regulation_title: objectid + nature + site
    - regulation_other_category_text: "Circulation"

    Filters out rows with duplicate objectid.
    """
    return df.with_columns(
        [
            (pl.col("objectid").cast(pl.Utf8) + pl.lit("/LIMITATION-VITESSE")).alias(
                "regulation_identifier"
            ),
            pl.lit(PostApiRegulationsAddBodyCategory.PERMANENTREGULATION.value).alias(
                "regulation_category"
            ),
            pl.lit(PostApiRegulationsAddBodySubject.OTHER.value).alias("regulation_subject"),
            (
                pl.lit(" Limitation de vitesse - ")
                + pl.col("agglo").fill_null(df["location_road_number"])
            ).alias("regulation_title"),
            pl.lit("Limitation de vitesse").alias("regulation_other_category_text"),
        ]
    )


def compute_vehicle_fields(df: pl.DataFrame):
    """
    Compute all vehicle fields for SaveVehicleSetDTO.
    - vehicle_all_vehicles: true
    """
    return df.with_columns([pl.lit(True).alias("vehicle_all_vehicles")])
