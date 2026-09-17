from typing import TypedDict, get_type_hints

import pandera.polars as pa
import polars as pl
from loguru import logger

from api.dia_log_client import Client
from settings import OrganizationSettings


class RegulationMeasure(TypedDict):
    """The pivot: one row of a source's clean data. A column not declared here is
    dropped silently by `select_regulation_measure_fields` (D-08)."""

    # Period fields (prefixed with period_)
    period_start_date: str | None
    period_end_date: str | None
    period_recurrence_type: str | None
    period_is_permanent: bool | None
    period_time_slots: list[dict[str, str]] | None
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
    # Named street (`location_road_type = lane`): no geometry, DiaLog geocodes the
    # city and road names itself. `*_point_type` is `houseNumber` or `intersection`,
    # and picks which of `*_house_number` / `*_road_name` bounds the section.
    location_city_code: str | None
    location_city_label: str | None
    location_road_name: str | None
    location_from_point_type: str | None
    location_from_house_number: str | None
    location_from_road_name: str | None
    location_to_point_type: str | None
    location_to_house_number: str | None
    location_to_road_name: str | None
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
    # Vehicle fields (prefixed with vehicle_)
    vehicle_all_vehicles: bool
    vehicle_heavyweight_max_weight: float | None
    vehicle_max_height: float | None
    vehicle_max_width: float | None
    vehicle_max_length: float | None
    vehicle_exempted_types: list[str] | None
    vehicle_restricted_types: list[str] | None
    vehicle_other_exempted_type_text: str | None
    vehicle_other_restricted_type_text: str | None


class BaseDataSourceIntegration:
    """One data source. Subclasses set `name` and `raw_data_schema`, and implement
    `fetch_raw_data` and `compute_clean_data`."""

    name: str | None = None
    raw_data_schema: type[pa.DataFrameModel] | None = None

    # Opt-in: collapse the rows sharing a `measure_group_key` into a single measure
    # carrying every one of their locations (R-28). Off by default: the sources already
    # in production emit one measure per row (D-18).
    group_locations_by_measure: bool = False

    # Opt-in: hard ceiling on the number of locations a single POST may carry. Above it
    # the regulation is cut into `IDENTIFIER-01`, `IDENTIFIER-02`… slices, ordered by
    # `regulation_split_order`. `None` means no ceiling.
    max_locations_per_regulation: int | None = None
    organization_settings: OrganizationSettings
    client: Client

    def __init__(self, organization_settings: OrganizationSettings, client: Client):
        self.organization_settings = organization_settings
        self.client = client

    @property
    def organization(self) -> str:
        return self.organization_settings.organization

    # What the dataset states and what the pipeline keeps, in the source's own unit: a
    # row, a segment, or one restriction of a segment that carries several. Their ratio
    # is the share of the dataset retained, shown in the Tchap report. Defaults: the
    # rows fetched, and the rows `compute_clean_data` returns. A source overrides them
    # when a row is not one restriction: it splits rows into restrictions, or merges
    # rows into measures.
    dataset_restrictions: int | None = None
    retained_restrictions: int | None = None

    def count_dataset_restrictions(self, df: pl.DataFrame) -> pl.DataFrame:
        """Pipe it where every row is one restriction the dataset states: right after
        the step that splits a row into its restrictions, before any filter."""
        self.dataset_restrictions = df.height
        logger.info(f"{df.height} restrictions stated by the dataset")
        return df

    def count_retained_restrictions(self, df: pl.DataFrame) -> pl.DataFrame:
        """Pipe it after the last filter and before rows are merged into measures, so
        the count stays in the unit of `count_dataset_restrictions`."""
        self.retained_restrictions = df.height
        logger.info(f"{df.height} restrictions retained by the pipeline")
        return df

    def compute_data_regulations(self) -> pl.DataFrame:
        """Fetch, validate, clean, then keep the `RegulationMeasure` columns."""
        raw_data = self.fetch_raw_data()
        logger.info(f"Fetched {raw_data.shape[0]} raw records")
        validated_data = self.validate_raw_data(raw_data)
        clean_data = validated_data.pipe(self.compute_clean_data)
        logger.info(f"After cleaning, got {clean_data.shape[0]} records")

        clean_data = self.select_regulation_measure_fields(clean_data)
        return clean_data

    def fetch_raw_data(self) -> pl.DataFrame:
        raise NotImplementedError("Subclasses must implement fetch_raw_data method")

    def preprocess_raw_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        """Minimal casts before validation (e.g. booleans); identity by default."""
        return raw_data

    def validate_raw_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        """Keep only the schema's columns, preprocess, then validate with Pandera.

        A schema column missing from the source fails the whole integration.
        """
        if self.raw_data_schema is None:
            raise NotImplementedError("Subclasses must set raw_data_schema class attribute")

        logger.info(f"Validating raw data schema with {raw_data.shape[0]} rows")

        columns_to_keep = list(self.raw_data_schema.to_schema().columns.keys())
        logger.info(f"Keeping {len(columns_to_keep)} columns: {columns_to_keep}")
        logger.info(f"Discarding columns: {set(raw_data.columns) - set(columns_to_keep)}")
        df = raw_data.select(columns_to_keep)

        df = self.preprocess_raw_data(df)
        validated_df = self.raw_data_schema.validate(df)

        logger.info(
            f"Raw data validation successful: {validated_df.shape[0]} rows, "
            f"{validated_df.shape[1]} columns"
        )

        return validated_df

    def compute_clean_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        raise NotImplementedError("Subclasses must implement compute_clean_data method")

    def select_regulation_measure_fields(self, df: pl.DataFrame) -> pl.DataFrame:
        """Keep the `RegulationMeasure` columns present in `df`. Any other column,
        a misspelt one included, is dropped without warning (D-08)."""
        required_fields = list(get_type_hints(RegulationMeasure).keys())
        available_fields = [field for field in required_fields if field in df.columns]

        return df.select(available_fields)
