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

        for r in records:
            titre = r.get("mesure_titre")
            if isinstance(titre, list):
                r["mesure_titre"] = " ".join(titre)

            mesures = r.get("mesures")
            if isinstance(mesures, list):
                r["mesures"] = " ".join(mesures)

        return pl.DataFrame(records)

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
            pl.when(pl.col("mesure_titre").str.contains("Limitation vitesse"))
            .then(pl.col("mesures").str.extract(r"(\d+)\s*km", 1).cast(pl.Int32))
            .otherwise(pl.lit(None))
            .alias("measure_max_speed"),
        ]
    )

    null_measure_type = df.select(pl.col("measure_type_").is_null().sum()).item()
    logger.warning(f"Dropping {null_measure_type} rows due to unable to infer restriction type")
    df = df.filter(pl.col("measure_type_").is_not_null())

    return df


def compute_period_fields(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        [
            pl.col("date_debut").str.to_datetime("%Y-%m-%d", strict=False),
            pl.col("date_fin").str.to_datetime("%Y-%m-%d", strict=False),
        ]
    )
    return df.with_columns(
        [
            pl.col("date_debut").dt.strftime("%Y-%m-%dT%H:%M:%SZ").alias("period_start_date"),
            pl.col("date_fin").dt.strftime("%Y-%m-%dT%H:%M:%SZ").alias("period_end_date"),
            pl.col("date_debut").dt.strftime("%Y-%m-%dT%H:%M:%SZ").alias("period_start_time"),
            pl.col("date_fin").dt.strftime("%Y-%m-%dT%H:%M:%SZ").alias("period_end_time"),
            pl.lit("everyDay").alias("period_recurrence_type"),
            pl.lit(False).alias("period_is_permanent"),
        ]
    )


def compute_location_fields(df: pl.DataFrame) -> pl.DataFrame:
    pdf = df.to_pandas()

    def to_geojson(geo):
        if geo is None:
            return None
        point = Point(geo["lon"], geo["lat"])
        return json.dumps(mapping(point))

    pdf["location_geometry"] = pdf["geolocalisation"].apply(to_geojson)
    df = pl.from_pandas(pdf)

    df = df.with_columns(
        [
            pl.lit(RoadTypeEnum.RAWGEOJSON.value).alias("location_road_type"),
            (pl.col("rue_principal") + pl.lit(" - ") + pl.col("commune")).alias("location_label"),
        ]
    )

    missing_geo = df.select(pl.col("location_geometry").is_null().sum()).item()
    logger.warning(f"Dropping {missing_geo} rows due to missing geolocalisation")
    return df.filter(pl.col("location_geometry").is_not_null())


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
            pl.col("description")
            .str.slice(0, 252)
            .map_elements(lambda s: s + "..." if s and len(s) > 252 else s, return_dtype=pl.String)
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
