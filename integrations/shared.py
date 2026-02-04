import json
from datetime import datetime
from importlib import util as importlib_util
from pathlib import Path
from typing import TypedDict, get_type_hints

import pandera.polars as pa
import polars as pl
from loguru import logger

from api.dia_log_client import Client
from api.dia_log_client.api.private.get_api_organization_identifiers import (
    sync_detailed as _get_identifiers,
)
from api.dia_log_client.api.private.post_api_regulations_add import (
    sync_detailed as add_regulation,
)
from api.dia_log_client.api.private.put_api_regulations_publish import (
    sync_detailed as publish_regulation,
)
from api.dia_log_client.models import (
    MeasureTypeEnum,
    PostApiRegulationsAddBody,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodyStatus,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
    SaveLocationDTO,
    SaveMeasureDTO,
    SavePeriodDTO,
    SaveRawGeoJSONDTO,
    SaveVehicleSetDTO,
)
from settings import OrganizationSettings

PY_TO_POLARS = {
    str: pl.Utf8,
    int: pl.Int64,
    float: pl.Float64,
    bool: pl.Boolean,
    datetime: pl.Datetime,
}


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
    regulation_status: str
    regulation_category: str
    regulation_subject: str
    regulation_title: str
    regulation_other_category_text: str
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


class DialogIntegration:
    client: Client
    draft_status: bool = False
    organization_settings: OrganizationSettings
    raw_data_schema: type[pa.DataFrameModel] | None = None  # Subclasses must set this

    def __init__(self, organization_settings: OrganizationSettings, client: Client):  # type: ignore
        self.organization_settings = organization_settings
        self.client = client

    @property
    def organization(self) -> str:
        return self.organization_settings.organization

    @classmethod
    def from_organization(cls, organization: str, env: str = "dev") -> "DialogIntegration":
        """Create DialogIntegration from organization name and environment."""
        organization_settings = OrganizationSettings.from_env(organization, env=env)
        return cls.from_settings(organization_settings)

    @classmethod
    def from_settings(cls, organization_settings: OrganizationSettings) -> "DialogIntegration":
        """Create DialogIntegration from pre-configured settings."""
        client = Client(
            base_url=organization_settings.base_url,  # type: ignore
            raise_on_unexpected_status=True,
            headers={
                "X-Client-Id": organization_settings.client_id,
                "X-Client-Secret": organization_settings.client_secret,
                "Accept": "application/json",
            },  # type: ignore
        )

        integration_file = (
            Path("integrations") / organization_settings.organization / "integration.py"
        )
        if not integration_file.exists():
            raise FileNotFoundError(integration_file)

        spec = importlib_util.spec_from_file_location(
            f"integrations.{organization_settings.organization}.integrations", integration_file
        )
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load {integration_file}")

        module = importlib_util.module_from_spec(spec)
        spec.loader.exec_module(module)

        if not hasattr(module, "Integration"):
            raise AttributeError("Integration class not found")

        return getattr(module, "Integration")(organization_settings, client)

    def integrate_regulations(self) -> None:
        raw_data = self.fetch_raw_data()
        logger.info(f"Fetched {raw_data.shape[0]} raw records")
        validated_data = self.validate_raw_data(raw_data)
        clean_data = validated_data.pipe(self.compute_clean_data)
        logger.info(f"After cleaning, got {clean_data.shape[0]} records")

        # Select only RegulationMeasure fields
        clean_data = self.select_regulation_measure_fields(clean_data)

        regulations = self.create_regulations(clean_data)
        for regulation in regulations:
            regulation.identifier = f"{regulation.identifier}-0"
        num_measures = sum([len(regulation.measures or []) for regulation in regulations])
        logger.info(f"Created {len(regulations)} regulations with a total of {num_measures}")

        integrated_regulation_ids = self.fetch_regulation_ids()

        regulation_ids_to_integrate = set([r.identifier for r in regulations]) - set(
            integrated_regulation_ids
        )
        regulations_to_integrate = [
            regulation
            for regulation in regulations
            if regulation.identifier in regulation_ids_to_integrate
        ]
        logger.info(f"Found {len(regulations_to_integrate)} new regulations to integrate")

        self._integrate_regulations(regulations_to_integrate)

    def publish_regulations(self) -> None:
        regulation_ids = self.fetch_regulation_ids()
        count_error = 0
        for index, regulation_id in enumerate(regulation_ids):
            try:
                publish_regulation(identifier=regulation_id, client=self.client)
                logger.success(
                    f"Measure {index}/{len(regulation_ids)} successfully published: {regulation_id}"
                )
            except Exception:
                logger.error(
                    f"Measure {index}/{len(regulation_ids)} failed to publish: {regulation_id}"
                )
                count_error += 1

        if count_error > 0:
            logger.error(f"Failed to publish {count_error} identifier(s)")
        logger.success(
            f"Finished publishing {len(regulation_ids) - count_error} measures successfully"
        )

    def _integrate_regulations(self, regulations: list[PostApiRegulationsAddBody]) -> None:
        count_error = 0
        for index, regulation in enumerate(regulations):
            logger.info(f"Creating regulation {index}/{len(regulations)}: {regulation.identifier}")
            logger.info(f"Contains {len(regulation.measures)} measures.") # type: ignore
            try:
                resp = add_regulation(client=self.client, body=regulation)
                assert resp.status_code == 201, f"got status {resp.status_code}"
            except Exception as e:
                logger.error(f"Failed to create: {regulation.identifier} - {e}")
                logger.error(json.loads(resp.content))
                count_error += 1

        count_success = len(regulations) - count_error
        logger.success(
            f"Finished integrating {count_success}/{len(regulations)} regulations successfully"
        )

    def fetch_raw_data(self) -> pl.DataFrame:
        """
        Fetch raw data from the source system.
        Returns as typed polars dataframe.
        """
        raise NotImplementedError("Subclasses must implement fetch_data method")

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

    def create_measure(self, measure: RegulationMeasure) -> SaveMeasureDTO:
        """
        Create a single measure from a RegulationMeasure.
        Default implementation that works for most cases.
        Subclasses can override if needed.
        """
        params = {
            "type_": MeasureTypeEnum(measure["measure_type_"]),
            "periods": [self.create_save_period_dto(measure)],
            "locations": [self.create_save_location_dto(measure)],
            "vehicle_set": self.create_save_vehicle_dto(measure),
        }

        # Add max_speed if present and not None
        if measure["measure_type_"] == MeasureTypeEnum.SPEEDLIMITATION.value:
            params["max_speed"] = int(measure["measure_max_speed"])  # type: ignore

        return SaveMeasureDTO(**params)

    def create_regulations(self, clean_data: pl.DataFrame) -> list[PostApiRegulationsAddBody]:
        """
        Create regulation payloads from clean data.
        Groups by regulation_identifier and creates measures for each group.
        Uses precomputed regulation fields from the DataFrame.
        """
        regulations = []

        for _, group_df in clean_data.group_by("regulation_identifier"):
            # Create measures for all rows in this regulation
            measures = []
            for row in group_df.iter_rows(named=True):
                try:
                    measures.append(self.create_measure(row))  # type: ignore
                except Exception as e:
                    logger.error(f"Error creating measure: {e}")

            # Skip if no measures were created
            if not measures:
                continue

            # Get regulation fields from first row (all rows have same values)
            first_row = group_df.row(0, named=True)

            regulations.append(
                PostApiRegulationsAddBody(
                    identifier=first_row["regulation_identifier"],
                    category=PostApiRegulationsAddBodyCategory(first_row["regulation_category"]),
                    status=PostApiRegulationsAddBodyStatus(first_row["regulation_status"]),
                    subject=PostApiRegulationsAddBodySubject(first_row["regulation_subject"]),
                    title=first_row["regulation_title"],
                    other_category_text=first_row["regulation_other_category_text"],
                    measures=measures,  # type: ignore
                )
            )

        return regulations

    def create_save_period_dto(self, measure: RegulationMeasure) -> SavePeriodDTO:
        """
        Create a SavePeriodDTO from a RegulationMeasure with period_ prefixed fields.
        Any field starting with 'period_' will be mapped to SavePeriodDTO,
        with the prefix stripped (e.g., period_start_date -> start_date).
        """
        period_fields = {}
        for key, value in measure.items():
            if key.startswith("period_"):
                field_name = key.replace("period_", "", 1)
                period_fields[field_name] = value

        return SavePeriodDTO(**period_fields)

    def create_save_location_dto(self, measure: RegulationMeasure) -> SaveLocationDTO:
        """
        Create a SaveLocationDTO from a RegulationMeasure with location_ prefixed fields.
        Expects location_road_type (string), location_label, and location_geometry fields.
        """
        road_type_value = measure["location_road_type"]
        road_type = RoadTypeEnum(road_type_value)

        return SaveLocationDTO(
            road_type=road_type,
            raw_geo_json=SaveRawGeoJSONDTO(
                label=measure["location_label"],
                geometry=measure["location_geometry"],
            ),
        )

    def create_save_vehicle_dto(self, measure: RegulationMeasure) -> SaveVehicleSetDTO:
        """
        Create a SaveVehicleSetDTO from a measure with vehicle_ prefixed fields.
        Intelligently handles the all_vehicles flag:
        - If all_vehicles=True and no restrictions/dimensions, only passes all_vehicles
        - Otherwise, includes all relevant fields
        """
        # Extract vehicle fields
        vehicle_fields = {}
        for key, value in measure.items():
            if key.startswith("vehicle_"):
                field_name = key.replace("vehicle_", "", 1)
                vehicle_fields[field_name] = value

        # Clean params: remove None, empty lists
        cleaned = {k: v for k, v in vehicle_fields.items() if v not in (None, [], {})}

        # If all_vehicles is True and there are no other constraints, simplify
        if cleaned.get("all_vehicles") is True and len(cleaned) == 1:
            return SaveVehicleSetDTO(all_vehicles=True)

        return SaveVehicleSetDTO(**cleaned)

    def fetch_regulation_ids(self) -> list[str]:
        logger.info(f"Fetching identifiers for organization: {self.organization}")
        resp = _get_identifiers(client=self.client)

        if resp.parsed is None or not hasattr(resp.parsed, "identifiers"):
            raise Exception("Failed to fetch identifiers")

        identifiers: list[str] = resp.parsed.identifiers  # type: ignore

        logger.info(f"Found {len(identifiers)} identifier(s) for organization {self.organization}")

        return list(identifiers)
