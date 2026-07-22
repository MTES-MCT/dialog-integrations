import json
import geopandas as gpd
import polars as pl
from loguru import logger
from shapely.geometry import mapping

from api.dia_log_client.models import (
    MeasureTypeEnum,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
)
from integrations.base_data_source_integration import BaseDataSourceIntegration
from .schema import TravauxVoirieRawDataSchema


class DataSourceIntegration(BaseDataSourceIntegration):
    raw_data_schema = TravauxVoirieRawDataSchema
    name = "travaux_voirie"

    def fetch_raw_data(self):
        return pl.read_parquet("data/travaux-voirie.parquet")

    def compute_clean_data(self, raw_data):
        return (
            raw_data.pipe(compute_measure_fields)
            .pipe(compute_period_fields)
            .pipe(compute_location_fields)
            .pipe(compute_regulation_fields)
            .pipe(compute_vehicle_fields)
        )


def compute_measure_fields(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        [
            pl.when(pl.col("mesure_titre").str.contains("Barrage de voie"))
            .then(pl.lit(MeasureTypeEnum.NOENTRY.value))
            .when(pl.col("mesure_titre").str.contains("Circulation alternée"))
            .then(pl.lit(MeasureTypeEnum.ALTERNATEROAD.value))
            .when(pl.col("mesure_titre").str.contains("Stationnement gênant"))
            .then(pl.lit(MeasureTypeEnum.PARKINGPROHIBITED.value))
            .when(pl.col("mesure_titre").str.contains("Limitation vitesse"))
            .then(pl.lit(MeasureTypeEnum.SPEEDLIMITATION.value))
            .otherwise(pl.lit(None))
            .alias("measure_type_"),
        ]
    )

    null_measure_type = df.select(pl.col("measure_type_").is_null().sum()).item()
    logger.warning(f"Dropping {null_measure_type} rows due to unable to infer restriction type")
    df = df.filter(pl.col("measure_type_").is_not_null())

    return df


def compute_period_fields(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        [
            pl.col("date_debut").dt.strftime("%Y-%m-%dT%H:%M:%SZ").alias("period_start_date"),
            pl.col("date_fin").dt.strftime("%Y-%m-%dT%H:%M:%SZ").alias("period_end_date"),
            pl.lit("everyDay").alias("period_recurrence_type"),
            pl.lit(False).alias("period_is_permanent"),
        ]
    )


def compute_location_fields(df: pl.DataFrame) -> pl.DataFrame:
    pdf = df.to_pandas()
    gdf = gpd.GeoDataFrame(
        pdf, geometry=gpd.GeoSeries.from_wkb(pdf["geolocalisation"]), crs="EPSG:4326"
    )
    pdf["location_geometry"] = gdf.geometry.apply(
        lambda geom: json.dumps(mapping(geom)) if geom is not None else None
    )
    df = pl.from_pandas(pdf)

    return df.with_columns(
        [
            pl.lit(RoadTypeEnum.RAWGEOJSON.value).alias("location_road_type"),
            (pl.col("rue_principal") + pl.lit(" - ") + pl.col("commune")).alias("location_label"),
        ]
    )


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
            pl.col("description").alias("regulation_title"),
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
