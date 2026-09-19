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
from integrations.shared.local_time import from_epoch_ms

from .schema import NantesCirculationChantierRawDataSchema

URL = (
    "https://services6.arcgis.com/YDMPvjgKQZcAUkPG/"
    "arcgis/rest/services/"
    "Espace_public_circulation_chantier"
    "/FeatureServer/5/query"
)


class DataSourceIntegration(BaseDataSourceIntegration):
    raw_data_schema = NantesCirculationChantierRawDataSchema
    name = "circulation_chantier"

    def fetch_raw_data(self):
        all_features = []
        offset = 0
        page_size = 2000

        while True:
            params = {
                "where": "1=1",
                "outFields": "*",
                "f": "geojson",
                "outSR": "4326",
                "resultOffset": offset,
                "resultRecordCount": page_size,
            }
            logger.info(f"Downloading data from {URL} ({offset}-{page_size})")
            r = requests.get(URL, params=params).json()
            features = r.get("features", [])
            all_features.extend(features)
            if len(features) < page_size:
                break
            offset += page_size

        gdf = gpd.GeoDataFrame.from_features(all_features, crs="EPSG:4326")
        gdf["geometry"] = gdf.geometry.to_wkt()
        return pl.from_pandas(gdf)

    def compute_clean_data(self, raw_data):
        return (
            raw_data.pipe(compute_measure_fields)
            .pipe(compute_period_fields)
            .pipe(compute_location_fields)
            .pipe(compute_regulation_fields)
            .pipe(compute_vehicle_fields)
        )


def compute_measure_fields(df: pl.DataFrame) -> pl.DataFrame:
    """`contrainte_auto`: Interdite -> noEntry, Alternée -> alternateRoad. Anything else
    (narrowed roadway, "Perturbée"…) is dropped."""

    df = df.with_columns(
        [
            pl.when(pl.col("contrainte_auto") == "Interdite")
            .then(pl.lit(MeasureTypeEnum.NOENTRY.value))
            .when(pl.col("contrainte_auto") == "Alternée")
            .then(pl.lit(MeasureTypeEnum.ALTERNATEROAD.value))
            .otherwise(pl.lit(None))
            .alias("measure_type_"),
        ]
    )

    null_measure_type = df.select(pl.col("measure_type_").is_null().sum()).item()
    logger.warning(f"Dropping {null_measure_type} rows due to unable to infer restriction type")
    df = df.filter(pl.col("measure_type_").is_not_null())

    return df


def compute_period_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Temporary period; date_debut and date_fin are epoch milliseconds."""

    return df.with_columns(
        [
            from_epoch_ms("date_debut").alias("period_start_date"),
            from_epoch_ms("date_fin").alias("period_end_date"),
            pl.lit("everyDay").alias("period_recurrence_type"),
            pl.lit(False).alias("period_is_permanent"),
        ]
    )


def compute_location_fields(df: pl.DataFrame) -> pl.DataFrame:
    """rawGeoJSON from the WKT `geometry` (already EPSG:4326, `outSR` of the query)."""

    pdf = df.to_pandas()
    gdf = gpd.GeoDataFrame(pdf, geometry=gpd.GeoSeries.from_wkt(pdf["geometry"]), crs="EPSG:4326")
    pdf["location_geometry"] = gdf.geometry.apply(lambda geom: json.dumps(mapping(geom)))
    df = pl.from_pandas(pdf)

    return df.with_columns(
        [
            pl.lit(RoadTypeEnum.RAWGEOJSON.value).alias("location_road_type"),
            (pl.col("voie") + pl.lit(" – ") + pl.col("commune")).alias("location_label"),
        ]
    )


def compute_regulation_fields(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        [
            (pl.lit("44/") + pl.col("gid").cast(pl.Utf8) + pl.lit("/TRAVAUX")).alias(
                "regulation_identifier"
            ),
            pl.lit(PostApiRegulationsAddBodyCategory.TEMPORARYREGULATION.value).alias(
                "regulation_category"
            ),
            pl.lit(PostApiRegulationsAddBodySubject.ROADMAINTENANCE.value).alias(
                "regulation_subject"
            ),
            (pl.col("motif") + pl.lit(" : ") + pl.col("nature").fill_null("").cast(pl.Utf8)).alias(
                "regulation_title"
            ),
            pl.col("type_chantier").alias("regulation_other_category_text"),
        ]
    )


def compute_vehicle_fields(df: pl.DataFrame):
    return df.with_columns([pl.lit(True).alias("vehicle_all_vehicles")])
