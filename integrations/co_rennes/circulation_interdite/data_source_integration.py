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
    """Only "Interdit dans les 2 sens" is published (noEntry). "Sens unique" is dropped:
    the layer gives no direction to publish it with (R-32)."""

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
    """Permanent period starting on 2022-01-13, the creation date of the file."""

    return df.with_columns(
        [
            pl.lit("2022-01-13T00:00:00+01:00").alias("period_start_date"),
            pl.lit(None).alias("period_end_date"),
            pl.lit("everyDay").alias("period_recurrence_type"),
            pl.lit(True).alias("period_is_permanent"),
        ]
    )


def compute_location_fields(df: pl.DataFrame) -> pl.DataFrame:
    """rawGeoJSON from the WKB `geo_shape`, labelled "nom_voie - code_insee - nom_commune"."""

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
    return df.with_columns([pl.lit(True).alias("vehicle_all_vehicles")])
