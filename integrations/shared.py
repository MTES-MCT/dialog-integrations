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
    PostApiRegulationsAddBody,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodyStatus,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
    SaveLocationDTO,
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


def typed_dict_to_polars_schema(td: type[TypedDict]) -> dict[str, pl.DataType]:  # type: ignore
    import types
    from typing import Union

    schema = {}
    for k, t in get_type_hints(td).items():
        origin = getattr(t, "__origin__", None)

        # Skip NoneType
        if origin is type(None):
            continue

        # Handle Union types (including Optional[T] which is Union[T, None])
        # In Python 3.10+, `X | Y` creates a types.UnionType instead of typing.Union
        if origin is Union or isinstance(t, types.UnionType):
            # Get non-None types from the Union
            args = [arg for arg in t.__args__ if arg is not type(None)]
            if not args:
                continue
            # Use the first non-None type
            t = args[0]
            origin = getattr(t, "__origin__", None)

        # Handle dict types
        if origin is dict:
            raise NotImplementedError

        # Handle list types: list[str] → List(Utf8)
        if origin is list:
            if hasattr(t, "__args__") and len(t.__args__) > 0:
                inner_type = t.__args__[0]
                schema[k] = pl.List(PY_TO_POLARS.get(inner_type, pl.Utf8))
            else:
                schema[k] = pl.List(pl.Utf8)
            continue

        # Handle simple types
        schema[k] = PY_TO_POLARS[t]
    return schema


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

    def create_measure(self, row: dict):
        """
        Create a single measure from a row of clean data.
        Subclasses must implement this method.
        """
        raise NotImplementedError("Subclasses must implement create_measure method")

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
                    measures.append(self.create_measure(row))
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

    def create_save_period_dto(self, measure: dict) -> SavePeriodDTO:
        """
        Create a SavePeriodDTO from a measure with period_ prefixed fields.
        Any field starting with 'period_' will be mapped to SavePeriodDTO,
        with the prefix stripped (e.g., period_start_date -> start_date).
        """
        period_fields = {}
        for key, value in measure.items():
            if key.startswith("period_"):
                field_name = key.replace("period_", "", 1)
                period_fields[field_name] = value

        return SavePeriodDTO(**period_fields)

    def create_save_location_dto(self, measure: dict) -> SaveLocationDTO:
        """
        Create a SaveLocationDTO from a measure with location_ prefixed fields.
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

    def create_save_vehicle_dto(self, measure: dict) -> SaveVehicleSetDTO:
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
