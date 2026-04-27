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

from .schema import RennesCirculationInterditeRawDataSchema

URL = (
    "https://data.rennesmetropole.fr/"
    "api/explore/v2.1/catalog/datasets/"
    "sens_circulation/exports/parquet?lang=fr&timezone=Europe%2FBerlin"
)

LOCAL_FILE = "explorations/co_rennes/data/sens_circulation.parquet"

MODE = "remote"


class DataSourceIntegration(BaseDataSourceIntegration):
    raw_data_schema = RennesCirculationInterditeRawDataSchema
    name = "sens_circulation"

    def fetch_raw_data(self):
        if MODE == "remote":
            logger.info(f"Downloading data from {URL}")

            r = requests.get(URL)
            r.raise_for_status()

            df = pl.read_parquet(io.BytesIO(r.content))
        elif MODE == "local":
            logger.info(f"Opening local data from {LOCAL_FILE}")
            df = pl.read_parquet(LOCAL_FILE)
        else:
            logger.error("MODE should be local or remote")
            raise

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
    """
    measure_type_ : depends on type
    Excludes : les mesures de type "chausséee rétrécies"
    """

    df = df.with_columns(
        [
            pl.when(pl.col("sens_circule") == "Interdit dans les 2 sens")
            .then(pl.lit(MeasureTypeEnum.NOENTRY.value))
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
    - period_start_date: from 2022-01-13 (creation of the file)
    - period_end_date: None
    - period_start_time: None
    - period_end_time: None
    - period_recurrence_type: everyDay
    - period_is_permanent: True
    """

    return df.with_columns(
        [
            pl.lit("2022-01-13T02:00:00Z").alias("period_start_date"),
            pl.lit(None).alias("period_end_date"),
            pl.lit(None).alias("period_start_time"),
            pl.lit(None).alias("period_end_time"),
            pl.lit("everyDay").alias("period_recurrence_type"),
            pl.lit(True).alias("period_is_permanent"),
        ]
    )


def compute_location_fields(df: pl.DataFrame) -> pl.DataFrame:
    """
    Compute all location fields for SaveLocationDTO.
    - location_road_type: always RoadTypeEnum.RAWGEOJSON
    - location_label: from localisation_curviligne
    - location_geometry: from geo_shape
    """

    return df.with_columns(
        [
            pl.lit(RoadTypeEnum.RAWGEOJSON.value).alias("location_road_type"),
            (
                pl.col("nom_voie")
                + pl.lit(" - ")
                + pl.col("code_insee").cast(pl.Utf8)
                + pl.lit(" - ")
                + pl.col("nom_commune")
            ).alias("location_label"),
            pl.from_pandas(
                gpd.GeoSeries.from_wkb(df["geo_shape"]).apply(
                    lambda geom: json.dumps(mapping(geom))
                )
            ).alias("location_geometry"),  # type: ignore
        ]
    )


def compute_regulation_fields(df: pl.DataFrame) -> pl.DataFrame:
    """
    Compute all regulation fields for PostApiRegulationsAddBody.
    - regulation_identifier: from id
    - regulation_category: PERMANENTREGULATION
    - regulation_subject: OTHER
    - regulation_title: objectid + nature + site
    - regulation_other_category_text: "Circulation"

    Filters out rows with duplicate objectid.
    """

    return df.with_columns(
        [
            (pl.lit("35/") + pl.col("id").cast(pl.Utf8) + pl.lit("/CIRCULATION")).alias(
                "regulation_identifier"
            ),
            pl.lit(PostApiRegulationsAddBodyCategory.PERMANENTREGULATION.value).alias(
                "regulation_category"
            ),
            pl.lit(PostApiRegulationsAddBodySubject.OTHER.value).alias("regulation_subject"),
            (
                pl.lit("Sens de circulation : ")
                + pl.col("sens_circule").fill_null("").cast(pl.Utf8)
            ).alias("regulation_title"),
            pl.lit("Circulation interdite").alias("regulation_other_category_text"),
        ]
    )


def compute_vehicle_fields(df: pl.DataFrame):
    """
    Compute all vehicle fields for SaveVehicleSetDTO.
    - vehicle_all_vehicles: true
    """
    return df.with_columns([pl.lit(True).alias("vehicle_all_vehicles")])
