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
    period_recurrence_type: str | None
    period_is_permanent: bool | None
    # Grouping fields — how rows collapse into measures and regulations.
    #
    # A source row is not always a measure: a SIG publishes one row per road segment,
    # and a single measure ("30 km/h") covers thousands of them. Rows sharing
    # `(regulation_identifier, measure_group_key)` become ONE measure carrying N
    # locations, provided the data source sets `group_locations_by_measure`.
    #
    # `regulation_split_order` orders the locations of a regulation that has to be cut
    # into several POSTs (see `max_locations_per_regulation`): rows are sorted by it, so
    # a source that fills it with a geographic ranking keeps each slice spatially
    # coherent instead of arbitrary.
    measure_group_key: str | None
    regulation_split_order: int | None
    # Location fields (prefixed with location_)
    location_road_type: str
    location_label: str | None
    location_geometry: str | None
    location_administrator: str | None
    location_road_number: str | None
    location_from_department_code: str | None
    location_from_point_number: str | None
    location_from_abscissa: int | None
    location_from_side: str | None
    location_to_department_code: str | None
    location_to_point_number: str | None
    location_to_abscissa: int | None
    location_to_side: str | None
    location_direction: str | None
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
    vehicle_max_length: float | None
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

    # Opt-in: collapse the rows sharing a `measure_group_key` into a single measure
    # carrying every one of their locations. Off by default so that the sources already
    # in production keep emitting one measure per row.
    group_locations_by_measure: bool = False

    # Opt-in: hard ceiling on the number of locations a single POST may carry. Above it
    # the regulation is cut into `IDENTIFIER-01`, `IDENTIFIER-02`… slices, ordered by
    # `regulation_split_order`. `None` means no ceiling.
    max_locations_per_regulation: int | None = None

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
