"""Data source integration for Sarthe restriction gabarits CSV data."""

import io

import polars as pl
import requests
from loguru import logger

from api.dia_log_client.models import (
    MeasureTypeEnum,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
)
from integrations.base_data_source_integration import BaseDataSourceIntegration
from integrations.dp_sarthe.restrictions_gabarits.schema import (
    SartheRestrictionGabaritsRawDataSchema,
)

URL = "https://data.sarthe.fr/api/explore/v2.1/catalog/datasets/227200029_restrictions_gabarits/exports/csv?lang=fr&timezone=Europe%2FLondon&use_labels=true&delimiter=%3B"


class DataSourceIntegration(BaseDataSourceIntegration):
    """Data source for Sarthe restriction gabarits CSV data."""

    raw_data_schema = SartheRestrictionGabaritsRawDataSchema
    name = "restrictions_gabarits"

    def fetch_raw_data(self) -> pl.DataFrame:
        # download
        logger.info(f"Downloading data from {URL}")
        r = requests.get(URL)
        r.raise_for_status()

        # read CSV into Polars
        df = pl.read_csv(io.BytesIO(r.content), separator=";", encoding="utf8", ignore_errors=True)

        # Rename columns with spaces to use underscores
        return df.rename(
            {
                "localisation curviligne": "localisation_curviligne",
                "Type de restriction": "type_de_restriction",
            }
        )

    def compute_clean_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        return (
            raw_data.pipe(compute_measure_fields)
            .pipe(compute_period_fields)
            .pipe(compute_location_fields)
            .pipe(self.compute_regulation_fields)
            .pipe(compute_vehicle_fields)
        )

    def compute_regulation_fields(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Compute all regulation fields for PostApiRegulationsAddBody.
        - regulation_identifier: from objectid (filter duplicates)
        - regulation_category: PERMANENTREGULATION
        - regulation_subject: OTHER
        - regulation_title: objectid + nature + site
        - regulation_other_category_text: "Circulation"

        Filters out rows with duplicate objectid.
        """
        # Find and drop duplicated objectids
        dup_ids = df.group_by("objectid").len().filter(pl.col("len") > 1).select("objectid")

        if dup_ids.height > 0:
            logger.warning(
                "Found %d duplicated objectids, dropping ALL corresponding rows",
                dup_ids.height,
            )
            logger.debug("Duplicated objectids: %s", dup_ids["objectid"].to_list())

        df = df.join(dup_ids, on="objectid", how="anti")

        # Compute regulation fields
        return df.with_columns(
            [
                pl.col("objectid").cast(pl.Utf8).alias("regulation_identifier"),
                pl.lit(PostApiRegulationsAddBodyCategory.PERMANENTREGULATION.value).alias(
                    "regulation_category"
                ),
                pl.lit(PostApiRegulationsAddBodySubject.OTHER.value).alias("regulation_subject"),
                (
                    pl.col("objectid").cast(pl.Utf8)
                    + pl.lit(" - ")
                    + pl.col("nature").fill_null("")
                    + pl.lit(" - ")
                    + pl.col("site").fill_null("")
                ).alias("regulation_title"),
                pl.lit("Circulation").alias("regulation_other_category_text"),
            ]
        )


def compute_measure_fields(df: pl.DataFrame) -> pl.DataFrame:
    """
    Compute measure_type_ and measure_max_speed fields.

    - measure_type_: always NOENTRY for restriction gabarits
    - measure_max_speed: null for all rows
    """
    return df.with_columns(
        [
            pl.lit(MeasureTypeEnum.NOENTRY.value).alias("measure_type_"),
            pl.lit(None).cast(pl.Int64).alias("measure_max_speed"),
        ]
    )


def compute_period_fields(df: pl.DataFrame) -> pl.DataFrame:
    """
    Compute all period fields for SavePeriodDTO.
    - period_start_date: from date_creation
    - period_end_date: None
    - period_start_time: None
    - period_end_time: None
    - period_recurrence_type: None
    - period_is_permanent: True

    Filters out rows where date_creation is not defined.
    """
    # Count rows with null date_creation before filtering
    n_null_date = df.select(pl.col("date_creation").is_null().sum()).item()
    if n_null_date > 0:
        logger.warning(
            f"Dropping {n_null_date} rows with null date_creation (no start date available)"
        )

    # Filter out rows where date_creation is null
    df = df.filter(pl.col("date_creation").is_not_null())

    return df.with_columns(
        [
            pl.col("date_creation").alias("period_start_date"),
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

    return df.with_columns(
        [
            pl.lit(RoadTypeEnum.RAWGEOJSON.value).alias("location_road_type"),
            pl.col("localisation_curviligne").alias("location_label"),
            pl.col("geo_shape").alias("location_geometry"),
        ]
    )


def compute_vehicle_fields(df: pl.DataFrame) -> pl.DataFrame:
    """
    Compute all vehicle fields for SaveVehicleSetDTO.
    - vehicle_all_vehicles: false
    - vehicle_heavyweight_max_weight: tonnage * 1000 (convert tons to kg) if defined and not 0
    - vehicle_max_height: hauteur if defined and not 0
    - vehicle_max_width: largeur if defined and not 0
    - vehicle_exempted_types: []
    - vehicle_restricted_types: ["heavyGoodsVehicle"] if tonnage > 0, ["dimensions"] otherwise
    - vehicle_other_exempted_type_text: None
    """
    return df.with_columns(
        [
            pl.lit(False).alias("vehicle_all_vehicles"),
            # Convert tonnage from tons to kg (multiply by 1000)
            pl.when((pl.col("tonnage").is_not_null()) & (pl.col("tonnage") > 0))
            .then(pl.col("tonnage") * 1000)
            .otherwise(None)
            .alias("vehicle_heavyweight_max_weight"),
            pl.when((pl.col("hauteur").is_not_null()) & (pl.col("hauteur") > 0))
            .then(pl.col("hauteur"))
            .otherwise(None)
            .alias("vehicle_max_height"),
            pl.when((pl.col("largeur").is_not_null()) & (pl.col("largeur") > 0))
            .then(pl.col("largeur"))
            .otherwise(None)
            .alias("vehicle_max_width"),
            pl.lit([]).cast(pl.List(pl.Utf8)).alias("vehicle_exempted_types"),
            # Set restricted_types based on restriction type:
            # - "heavyGoodsVehicle" for weight restrictions (tonnage > 0)
            # - "dimensions" for height/width restrictions (tonnage == 0)
            pl.when((pl.col("tonnage").is_not_null()) & (pl.col("tonnage") > 0))
            .then(pl.lit(["heavyGoodsVehicle"]).cast(pl.List(pl.Utf8)))
            .otherwise(pl.lit(["dimensions"]).cast(pl.List(pl.Utf8)))
            .alias("vehicle_restricted_types"),
            pl.lit(None).cast(pl.Utf8).alias("vehicle_other_exempted_type_text"),
        ]
    )
