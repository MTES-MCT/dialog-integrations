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
from integrations.shared.local_time import end_of_local_day, start_of_local_day

from .schema import RennesTravauxVoirieRawDataSchema

URL = (
    "https://data.rennesmetropole.fr/"
    "api/explore/v2.1/catalog/datasets/"
    "travaux_1_jour/exports/parquet?lang=fr&timezone=Europe%2FBerlin"
)

LOCAL_FILE = "explorations/co_rennes/data/travaux_1_jour.parquet"

MODE = "remote"


class DataSourceIntegration(BaseDataSourceIntegration):
    raw_data_schema = RennesTravauxVoirieRawDataSchema
    name = "travaux_voirie"

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
    """Measure type from `type`; anything else (narrowed roadway, turn ban…) is dropped."""

    df = df.with_columns(
        [
            pl.when(pl.col("type").str.starts_with("Circulation interdite"))
            .then(pl.lit(MeasureTypeEnum.NOENTRY.value))
            .when(pl.col("type").str.contains("Circulation alternée"))
            .then(pl.lit(MeasureTypeEnum.ALTERNATEROAD.value))
            .when(pl.col("type").str.contains("Interdiction de stationnement"))
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
    """Temporary period from date_deb 00:00:00 to date_fin 23:59:59, Paris time."""

    return df.with_columns(
        [
            start_of_local_day(df, "date_deb").alias("period_start_date"),
            end_of_local_day(df, "date_fin").alias("period_end_date"),
            pl.lit("everyDay").alias("period_recurrence_type"),
            pl.lit(False).alias("period_is_permanent"),
        ]
    )


def compute_location_fields(df: pl.DataFrame) -> pl.DataFrame:
    """rawGeoJSON from the WKB `geo_shape`, labelled "localisation - commune"."""

    return df.with_columns(
        [
            pl.lit(RoadTypeEnum.RAWGEOJSON.value).alias("location_road_type"),
            (pl.col("localisation") + pl.lit(" - ") + pl.col("commune")).alias("location_label"),
            pl.from_pandas(
                gpd.GeoSeries.from_wkb(df["geo_shape"]).apply(
                    lambda geom: json.dumps(mapping(geom))
                )
            ).alias("location_geometry"),  # type: ignore
        ]
    )


def compute_regulation_fields(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        [
            (pl.lit("35/") + pl.col("id").cast(pl.Utf8) + pl.lit("/TRAVAUX")).alias(
                "regulation_identifier"
            ),
            pl.lit(PostApiRegulationsAddBodyCategory.TEMPORARYREGULATION.value).alias(
                "regulation_category"
            ),
            pl.lit(PostApiRegulationsAddBodySubject.ROADMAINTENANCE.value).alias(
                "regulation_subject"
            ),
            (pl.lit("Travaux ") + pl.col("libelle").fill_null("").cast(pl.Utf8)).alias(
                "regulation_title"
            ),
            pl.lit("Circulation").alias("regulation_other_category_text"),
        ]
    )


def compute_vehicle_fields(df: pl.DataFrame):
    return df.with_columns([pl.lit(True).alias("vehicle_all_vehicles")])
