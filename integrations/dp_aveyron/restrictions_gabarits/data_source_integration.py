"""Data source integration for Aveyron : prescriptions-routieres-du-departement"""

import io

from loguru import logger
import polars as pl
import requests

from integrations.base_data_source_integration import BaseDataSourceIntegration
from integrations.dp_aveyron.restrictions_gabarits.schema import AveyronPrescriptionsRoutieresRawDataSchema

from api.dia_log_client.models import (
    MeasureTypeEnum,
    PostApiRegulationsAddBodyMeasuresItemVehicleSetType0RestrictedTypesType0Item as VehicleRestrictedTypeEnum,
)




URL = "https://opendata.aveyron.fr/api/explore/v2.1/catalog/datasets/prescriptions-routieres-du-departement-aveyron/exports/parquet"

class DataSourceIntegration(BaseDataSourceIntegration):
    """Data source for Prescription Routière du Département"""
    raw_data_scheme = AveyronPrescriptionsRoutieresRawDataSchema
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
    return df

def compute_location_fields(df: pl.DataFrame):
    return df

def compute_regulation_fields(df: pl.DataFrame):
    return df

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