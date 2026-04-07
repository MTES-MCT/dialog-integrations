
import polars as pl

from api.dia_log_client.models import (
    DirectionEnum,
    MeasureTypeEnum,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
)
from integrations.base_data_source_integration import BaseDataSourceIntegration
from integrations.dp_sarthe.chantiers_routiers.schema import SartheChantiersRoutiersSchema

URL = "https://data.sarthe.fr/api/explore/v2.1/catalog/datasets/227200029_chantiers_routiers/exports/parquet"
LOCAL_FILE = "explorations/dp_sarthe/data/227200029_chantiers_routiers.parquet"


class DataSourceIntegration(BaseDataSourceIntegration):
    """Data source for Prescription Routière du Département"""

    raw_data_schema = SartheChantiersRoutiersSchema
    name = "chantiers_routiers"

    def fetch_raw_data(self):
        # logger.info(f"Downloading data from {URL}")

        # r = requests.get(URL)
        # r.raise_for_status()

        # df = pl.read_parquet(io.BytesIO(r.content))
        df = pl.read_parquet(LOCAL_FILE)

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
    Compute mesure fields
    - measure_type_ : "NOENTRY" (travaux)
    """
    return df.with_columns(
        [
            pl.when(pl.col("mode_exp").str.to_lowercase() == "alternat")
            .then(pl.lit(MeasureTypeEnum.ALTERNATEROAD.value))
            .when(pl.col("mode_exp").str.to_lowercase().str.contains("Limitation de vitesse"))
            .then(pl.lit(MeasureTypeEnum.SPEEDLIMITATION.value))
            .otherwise(pl.lit(MeasureTypeEnum.NOENTRY.value))
            .alias("measure_type_"),
        ]
    )


def compute_period_fields(df: pl.DataFrame):
    """
    Compute all period fields for SavePeriodDTO.
    - period_start_date: today
    - period_end_date: from date_fin
    - period_start_time: None
    - period_end_time: None
    - period_recurrence_type: everyDay
    - period_is_permanent: True
    """
    return df.with_columns(
        [
            pl.col("date_debut").dt.strftime("%Y-%m-%dT00:00:00Z").alias("period_start_date"),
            pl.col("date_fin").dt.strftime("%Y-%m-%dT00:00:00Z").alias("period_end_date"),
            pl.col("date_debut").dt.strftime("%Y-%m-%dT00:00:00Z").alias("period_start_time"),
            pl.col("date_fin").dt.strftime("%Y-%m-%dT00:00:00Z").alias("period_end_time"),
            pl.lit("everyDay").alias("period_recurrence_type"),
            pl.lit(True).alias("period_is_permanent"),
        ]
    )


def compute_location_fields(df: pl.DataFrame):
    r"""
    Compute all location fields for SaveLocationDTO.
    Parse the following Regexp
        (RD) (\d+) : Du (\d+)\+(\d+) au (\d+)\+(\d+)

    - location_administrator: "Sarthe"
    - location_road_type: RoadTypeEnum.DEPARTMENTALROAD
    - location_road_number: pattern[1]+pattern[2]
    - location_from_department_code: 72
    - location_from_point_number: pattern[3]
    - location_from_abscissa: pattern[4]
    - location_from_side: "U"
    - location_to_department_code: 72
    - location_to_point_number: pattern[5]
    - location_to_abscissa: pattern[6]
    - location_to_side: "U"
    - location_direction: "BOTH"
    """
    pattern = r"(RD) (\d+) : Du (\d+)\+(\d+) au (\d+)\+(\d+)"

    return df.with_columns(
        [
            pl.lit("Sarthe").alias("location_administrator"),
            pl.lit(RoadTypeEnum.DEPARTMENTALROAD.value).alias("location_road_type"),
            (pl.lit("D") + df["loc_txt"].str.extract(pattern, 2).str.strip_chars_start("0")).alias(
                "location_road_number"
            ),
            pl.lit("72").alias("location_from_department_code"),
            (df["loc_txt"].str.extract(pattern, 3)).alias("location_from_point_number"),
            (df["loc_txt"].str.extract(pattern, 4).cast(int)).alias("location_from_abscissa"),
            pl.lit("U").alias("location_from_side"),
            pl.lit("72").alias("location_to_department_code"),
            (df["loc_txt"].str.extract(pattern, 5)).alias("location_to_point_number"),
            (df["loc_txt"].str.extract(pattern, 6).cast(int)).alias("location_to_abscissa"),
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

    Filters out rows with duplicate objectid.
    """
    return df.with_columns(
        [
            (pl.lit("72-chantiers-routiers-") + pl.col("objectid").cast(pl.Utf8)).alias(
                "regulation_identifier"
            ),
            pl.lit(PostApiRegulationsAddBodyCategory.TEMPORARYREGULATION.value).alias(
                "regulation_category"
            ),
            pl.lit(PostApiRegulationsAddBodySubject.ROADMAINTENANCE.value).alias(
                "regulation_subject"
            ),
            (pl.lit("Travaux : ") + pl.col("nature_trvx")).alias("regulation_title"),
        ]
    )


def compute_vehicle_fields(df: pl.DataFrame):
    """
    Compute all vehicle fields for SaveVehicleSetDTO.
    - vehicle_all_vehicles: true
    """
    return df.with_columns([pl.lit(True).alias("vehicle_all_vehicles")])
