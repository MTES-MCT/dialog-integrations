from typing import TypedDict, get_type_hints

import pandera.polars as pa
import polars as pl
from loguru import logger

from api.dia_log_client import Client
from settings import OrganizationSettings


class RegulationMeasure(TypedDict):
    """
    Unified type for all measure and regulation data.
    Contains all fields needed to create regulations and measures.
    """

    # Period fields (prefixed with period_)
    period_start_date: str | None
    period_end_date: str | None
    period_start_time: str | None
    period_end_time: str | None
    period_recurrence_type: str | None
    period_is_permanent: bool | None
    # Location fields (prefixed with location_)
    location_road_type: str
    location_label: str
    location_geometry: str
    # Regulation fields (prefixed with regulation_)
    regulation_identifier: str
    regulation_category: str
    regulation_subject: str
    regulation_title: str
    regulation_other_category_text: str
    regulation_document_url: str | None
    # Measure fields
    measure_type_: str
    measure_max_speed: int | None
    # Vehicle fields (prefixed with vehicle_)
    vehicle_all_vehicles: bool
    vehicle_heavyweight_max_weight: float | None
    vehicle_max_height: float | None
    vehicle_max_width: float | None
    vehicle_exempted_types: list[str] | None
    vehicle_restricted_types: list[str] | None
    vehicle_other_exempted_type_text: str | None


class BaseDataSourceIntegration:
    """
    Base class for data source integrations.
    Each data source should extend this class and implement the abstract methods.
    """

    name: str | None = None  # Subclasses must set this
    raw_data_schema: type[pa.DataFrameModel] | None = None  # Subclasses must set this
    organization_settings: OrganizationSettings
    client: Client

    def __init__(self, organization_settings: OrganizationSettings, client: Client):
        self.organization_settings = organization_settings
        self.client = client

    @property
    def organization(self) -> str:
        return self.organization_settings.organization

    def compute_data_regulations(self) -> pl.DataFrame:
        """
        Fetch, validate, and clean data from a single data source.
        Returns a DataFrame with RegulationMeasure fields.
        Override this method in subclasses for custom data processing.
        """
        raw_data = self.fetch_raw_data()
        logger.info(f"Fetched {raw_data.shape[0]} raw records")
        validated_data = self.validate_raw_data(raw_data)
        clean_data = validated_data.pipe(self.compute_clean_data)
        logger.info(f"After cleaning, got {clean_data.shape[0]} records")

        # Select only RegulationMeasure fields
        clean_data = self.select_regulation_measure_fields(clean_data)
        return clean_data

    def fetch_raw_data(self) -> pl.DataFrame:
        """
        Fetch raw data from the source system.
        Returns as typed polars dataframe.
        """
        raise NotImplementedError("Subclasses must implement fetch_raw_data method")

    def preprocess_raw_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        """
        Apply minimal preprocessing transformations before validation.
        Default implementation returns data unchanged.
        Override in subclasses for integration-specific preprocessing (e.g., boolean casting).
        """
        return raw_data

    def validate_raw_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        """
        Validate raw data schema and keep only columns we need.
        Applies minimal transformations via preprocess_raw_data, then validates.
        """
        if self.raw_data_schema is None:
            raise NotImplementedError("Subclasses must set raw_data_schema class attribute")

        logger.info(f"Validating raw data schema with {raw_data.shape[0]} rows")

        # Select only the columns we need
        columns_to_keep = list(self.raw_data_schema.to_schema().columns.keys())
        logger.info(f"Keeping {len(columns_to_keep)} columns: {columns_to_keep}")
        logger.info(f"Discarding columns: {set(raw_data.columns) - set(columns_to_keep)}")
        df = raw_data.select(columns_to_keep)

        # Apply integration-specific preprocessing (e.g., boolean casting)
        df = self.preprocess_raw_data(df)

        # Validate with pandera schema
        validated_df = self.raw_data_schema.validate(df)

        logger.info(
            f"Raw data validation successful: {validated_df.shape[0]} rows, "
            f"{validated_df.shape[1]} columns"
        )

        return validated_df

    def compute_clean_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        """
        Clean and transform the raw data into the desired format.
        Returns as typed polars dataframe.
        """
        raise NotImplementedError("Subclasses must implement compute_clean_data method")

    def select_regulation_measure_fields(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Select only the fields defined in RegulationMeasure from the dataframe.
        This ensures we only keep the necessary columns for creating regulations.
        """
        # Get field names from RegulationMeasure TypedDict
        required_fields = list(get_type_hints(RegulationMeasure).keys())

        # Filter to only include fields that exist in the dataframe
        available_fields = [field for field in required_fields if field in df.columns]

        return df.select(available_fields)
