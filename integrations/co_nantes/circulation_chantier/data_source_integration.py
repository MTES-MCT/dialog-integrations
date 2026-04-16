from loguru import logger
import requests
import geopandas as gpd
import json
import polars as pl
from shapely.geometry import mapping

from api.dia_log_client.models import (
    MeasureTypeEnum,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
)

from integrations.base_data_source_integration import BaseDataSourceIntegration

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
                "resultRecordCount": page_size
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
    """
    measure_type_ : 
        contrainte_auto = Interdite -> NOENTRY
        contrainte_auto = Alternée -> ALTERNATEROAD

    Excludes : les mesures de type "chausséee rétrécies"
    """

    df = df.with_columns(
        [
            pl.when(pl.col("contrainte_auto") == "Interdite")
            .then(pl.lit(MeasureTypeEnum.NOENTRY.value))
            .when(pl.col("contrainte_auto") == "Alternée")
            .then(pl.lit(MeasureTypeEnum.ALTERNATEROAD.value))
            .otherwise(pl.lit(None))
            .alias("measure_type_"),
        ])
    
    null_measure_type = df.select(pl.col("measure_type_").is_null().sum()).item()
    logger.warning(f"Dropping {null_measure_type} rows due to unable to infer restriction type")
    df = df.filter(pl.col("measure_type_").is_not_null())

    return df

def compute_period_fields(df: pl.DataFrame) -> pl.DataFrame:
    """
    Compute all period fields for SavePeriodDTO.
    - period_start_date: date_debut (timestamp)
    - period_end_date: date_fin (timestamp)
    - period_start_time: date_debut (timestamp)
    - period_end_time: date_fin (timestamp)
    - period_recurrence_type: everyDay
    - period_is_permanent: False
    """

    return df.with_columns(
        [
            pl.from_epoch("date_debut", time_unit="ms").dt.strftime("%Y-%m-%dT%H:%M:%SZ").alias("period_start_date"),
            pl.from_epoch("date_fin", time_unit="ms").dt.strftime("%Y-%m-%dT%H:%M:%SZ").alias("period_end_date"),
            pl.from_epoch("date_debut", time_unit="ms").dt.strftime("%Y-%m-%dT%H:%M:%SZ").alias("period_start_time"),
            pl.from_epoch("date_fin", time_unit="ms").dt.strftime("%Y-%m-%dT%H:%M:%SZ").alias("period_end_time"),
            pl.lit("everyDay").alias("period_recurrence_type"),
            pl.lit(False).alias("period_is_permanent"),
        ]
    )

def compute_location_fields(df: pl.DataFrame) -> pl.DataFrame:
    """
    Compute all location fields for SaveLocationDTO.
    - location_road_type: always RoadTypeEnum.RAWGEOJSON
    - location_label: voie + commune
    - location_geometry: from geometry field (WKT) transformed to GeoJSON (WGS84)
    Filter out rows where geometry is null.
    """

    pdf = df.to_pandas()
    gdf = gpd.GeoDataFrame(pdf, geometry=gpd.GeoSeries.from_wkt(pdf["geometry"]), crs="EPSG:4326")
    pdf["location_geometry"] = gdf.geometry.apply(lambda geom: json.dumps(mapping(geom)))
    df = pl.from_pandas(pdf)

    return df.with_columns(
        [
            pl.lit(RoadTypeEnum.RAWGEOJSON.value).alias("location_road_type"),
            (pl.col("voie") + pl.lit(" – ") + pl.col("commune")).alias("location_label"),
            # location_geometry already in df
        ]
    )

def compute_regulation_fields(df: pl.DataFrame) -> pl.DataFrame:
    """
    Compute all regulation fields for PostApiRegulationsAddBody.
    - regulation_identifier: from objectid field
    - regulation_category: TEMPORARYREGULATION
    - regulation_subject: ROADMAINTENANCE
    - regulation_title: "{DESCRIPTIF} – {LIBRU}"
    - regulation_other_category_text: "Circulation"
    - regulation_document_url: from LIEN_URL if available
    """

    return df.with_columns([
        (pl.lit("44/") + pl.col("objectid").cast(pl.Utf8) + pl.lit("/TRAVAUX")).alias(
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

    ])

def compute_vehicle_fields(df: pl.DataFrame):
    """
    Compute all vehicle fields for SaveVehicleSetDTO.
    - vehicle_all_vehicles: true
    """
    return df.with_columns([pl.lit(True).alias("vehicle_all_vehicles")])