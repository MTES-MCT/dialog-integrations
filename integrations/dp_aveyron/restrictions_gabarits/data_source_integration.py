"""Data source integration for Aveyron : prescriptions-routieres-du-departement"""

import io
import json

import geojson
import geopandas as gpd
from loguru import logger
import polars as pl
import requests
from shapely.geometry import mapping

from integrations.base_data_source_integration import BaseDataSourceIntegration
from integrations.dp_aveyron.restrictions_gabarits.schema import AveyronPrescriptionsRoutieresRawDataSchema

from api.dia_log_client.models import (
    MeasureTypeEnum,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodyMeasuresItemVehicleSetType0RestrictedTypesType0Item as VehicleRestrictedTypeEnum,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
)




URL = "https://opendata.aveyron.fr/api/explore/v2.1/catalog/datasets/prescriptions-routieres-du-departement-aveyron/exports/parquet"

class DataSourceIntegration(BaseDataSourceIntegration):
    """Data source for Prescription Routière du Département"""
    raw_data_schema = AveyronPrescriptionsRoutieresRawDataSchema
    name = "restrictions_gabarits"

    def fetch_raw_data(self):
        logger.info(f"Downloading data from {URL}")

        r = requests.get(URL)
        r.raise_for_status()

        df = pl.read_parquet(io.BytesIO(r.content))

        return df
    
    def compute_clean_data(self, raw_data):
        return (
            raw_data
            .pipe(unnest_measures)
            .pipe(filter_unrelevant)
            .pipe(compute_measure_fields)
            .pipe(compute_period_fields)
            .pipe(compute_location_fields)
            .pipe(compute_regulation_fields)
            .pipe(compute_vehicle_fields)
        )




def unnest_measures(df: pl.DataFrame):
    """
    We unnest all records based on the "panneau" column, and format it accordingly
    """
    return (
        df
        .with_columns(
            pl.col("panneau").str.split(";").alias("panneau")
        )
        .explode("panneau")
        .with_columns(
                pl.col("panneau").str.split_exact("_", 1)
                .struct.rename_fields(["panneau_type", "panneau_value"])
                .alias("restriction_fields")
        )
        .unnest("restriction_fields")
        .with_columns(
            pl.col("panneau_value")
            .str.strip_chars("mt²")
            .str.replace_all(",",".")
            .cast(pl.Float64)
            .alias("panneau_value")
        )
    )


PANNEAUX = {
    #'B9a' : "Interdiction piéton", # Non-traité
    #'B9b' : "Interdiction cycliste", # Non-traité
    #'B9i' : "Interdiction caravanes", # Non-traité
    'B18c' : "Interdiction Transport Matieres dangereuse",
    'B10a' : "Limitation de longueur", 
    'B9f' : "Limitation de longueur bus",
    'B11' : "Limitation de largeur", 
    'B12' : "Limitation de hauteur",
    'B13' : "Limitation de tonnage", 
}

def filter_unrelevant(df: pl.DataFrame):
    """
    Keep only rows that are relevant to DiaLog
    """
    return df.filter(pl.col("panneau_type").is_in(PANNEAUX.keys()))

def compute_measure_fields(df: pl.DataFrame):
    return df.with_columns([
        (pl.when(pl.col("panneau_type").is_null())
            .then(pl.lit(MeasureTypeEnum.SPEEDLIMITATION.value))
            .otherwise(pl.lit(MeasureTypeEnum.NOENTRY.value))
            .alias("measure_type_")),
        (pl.when(pl.col("panneau_type").is_null())
            .then(30.0)
            .otherwise(None)
            .alias("measure_max_speed"))
    ])

def compute_period_fields(df: pl.DataFrame):
    """
    Compute all period fields for SavePeriodDTO.
    - period_start_date: from date_creation
    - period_end_date: None
    - period_start_time: None
    - period_end_time: None
    - period_recurrence_type: everyDay
    - period_is_permanent: True

    Filters out rows where date_creation is not defined.
    """

    # Count rows with null date_creation before filtering
    n_null_date = df.select(pl.col("date_maj").is_null().sum()).item()
    if n_null_date > 0:
        logger.warning(
            f"Dropping {n_null_date} rows with null date_creation (no start date available)"
        )

    # Filter out rows where date_creation is null
    df = df.filter(pl.col("date_maj").is_not_null())

    return df.with_columns(
        [
            (
                pl.col("date_maj").str.to_date().dt.strftime("%Y-%m-%d")
             + pl.lit("T00:00:00")
            ).alias("period_start_date"),
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
    - location_road_type: always RoadTypeEnum.RAWGEOJSON
    - location_label: D98 (Aveyron) du PR 28+881 au PR 32+444
    - location_geometry: from geo_shape

    Filter out rows where geo_shape is null or undefined.
    """
    # Count rows with null geo_shape before filtering
    n_null_geometry = df.select(pl.col("geo_shape").is_null().sum()).item()
    if n_null_geometry > 0:
        logger.warning(
            f"Dropping {n_null_geometry} rows with null geo_shape (no geometry available)"
        )

    # Filter out rows where geo_shape is null
    df = df.filter(pl.col("geo_shape").is_not_null())

    return df.with_columns([
        (pl.col("idroute").str.split("_").list.last()
         + pl.lit(" (Aveyron) du PR ")
         + pl.col("prdeb").cast(pl.Utf8)
         + pl.lit("+")
         + pl.col("absdeb").cast(pl.Utf8)
         + pl.lit(" au PR ")
         + pl.col("prfin").cast(pl.Utf8)
         + pl.lit("+")
         + pl.col("absfin").cast(pl.Utf8)
        ).alias("location_label"),
        pl.lit(RoadTypeEnum.RAWGEOJSON.value).alias("location_road_type"),
        pl.from_pandas(gpd.GeoSeries.from_wkb(df["geo_shape"]).apply(lambda geom: json.dumps(mapping(geom)))).alias("location_geometry")
    ])

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
                (
                    pl.lit("12-restriction-")
                    + pl.col("objectid").cast(pl.Utf8)
                ).alias("regulation_identifier"),
                pl.lit(PostApiRegulationsAddBodyCategory.PERMANENTREGULATION.value).alias(
                    "regulation_category"
                ),
                pl.lit(PostApiRegulationsAddBodySubject.OTHER.value).alias("regulation_subject"),
                (
                    pl.col("arrete").cast(pl.Utf8)
                    + pl.lit(" - ")
                    + pl.col("prescript").fill_null("")
                    + pl.lit(" - ")
                    + pl.col("commune").fill_null("")
                ).alias("regulation_title"),
                pl.col("prescript").alias("regulation_other_category_text"),
            ]
        )

def compute_vehicle_fields(df: pl.DataFrame):
    """
    Compute all vehicle fields for SaveVehicleSetDTO.
    - vehicle_all_vehicles: true only for speed limits rows
    - vehicle_restricted_types : 
        None if speedlimit
        heavyGoodsVehicle if B13
        hazardousMaterials if B18c
        other and dimension if B9f
        dimensions otherwise
    - vehicle_heavyweight_max_weight if B13
    - vehicle_max_height if B12
    - vehicle_max_width if B11
    - vehicle_max_length if B10a or B9f
    - other_restricted_type_text : "Bus" if B9f

    """
    return df.with_columns(
        [
            pl.when(
                pl.col("measure_type_") == pl.lit(MeasureTypeEnum.SPEEDLIMITATION.value)
            )
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("vehicle_all_vehicles"),

            pl.when(
                pl.col("measure_type_") == pl.lit(MeasureTypeEnum.SPEEDLIMITATION.value)
            )
            .then(None)
            .when(
                pl.col("panneau_type") == "B13"
            )
            .then(pl.lit([VehicleRestrictedTypeEnum.HEAVYGOODSVEHICLE.value]))
            .when(
                pl.col("panneau_type") == "B18c"
            )
            .then(pl.lit([VehicleRestrictedTypeEnum.HAZARDOUSMATERIALS.value]))
            .when(
                pl.col("panneau_type") == "B9f"
            )
            .then(
                    pl.lit([VehicleRestrictedTypeEnum.DIMENSIONS.value,
                    VehicleRestrictedTypeEnum.OTHER.value])
            )
            .otherwise(pl.lit([VehicleRestrictedTypeEnum.DIMENSIONS.value]))
            .alias("vehicle_restricted_types")
        ]
    ).with_columns([
        pl
        .when(pl.col("panneau_type") == "B13")
        .then(pl.col("panneau_value"))
        .alias("vehicle_heavyweight_max_weight"),
        pl
        .when(pl.col("panneau_type") == "B12")
        .then(pl.col("panneau_value"))
        .alias("vehicle_max_height"),
        pl
        .when(pl.col("panneau_type") == "B11")
        .then(pl.col("panneau_value"))
        .alias("vehicle_max_width"),
        pl
        .when(pl.col("panneau_type").is_in(["B10a", "B9f"]))
        .then(pl.col("panneau_value"))
        .alias("vehicle_max_length"),
        pl
        .when(pl.col("panneau_type") == "B9f")
        .then(pl.lit("Bus"))
        .alias("other_restricted_type_text"),

    ])