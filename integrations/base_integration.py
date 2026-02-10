import json
from importlib import util as importlib_util

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
from integrations.base_data_source_integration import BaseDataSourceIntegration, RegulationMeasure
from settings import OrganizationSettings


class BaseIntegration:
    """Base integration class that orchestrates data sources and API interactions."""

    client: Client
    status: PostApiRegulationsAddBodyStatus = PostApiRegulationsAddBodyStatus.DRAFT
    organization_settings: OrganizationSettings
    data_sources: list[type[BaseDataSourceIntegration]]

    def __init__(self, organization_settings: OrganizationSettings, client: Client):  # type: ignore
        self.organization_settings = organization_settings
        self.client = client

    @property
    def organization(self) -> str:
        return self.organization_settings.organization

    @classmethod
    def from_organization(cls, organization: str, env: str = "dev") -> "BaseIntegration":
        """Create Integration from organization name and environment."""
        organization_settings = OrganizationSettings.from_env(organization, env=env)
        return cls.from_settings(organization_settings)

    @classmethod
    def from_settings(cls, organization_settings: OrganizationSettings) -> "BaseIntegration":
        """Create Integration from pre-configured settings."""
        client = Client(
            base_url=organization_settings.base_url,  # type: ignore
            raise_on_unexpected_status=True,
            headers={
                "X-Client-Id": organization_settings.client_id,
                "X-Client-Secret": organization_settings.client_secret,
                "Accept": "application/json",
            },  # type: ignore
        )

        # Import the Integration class from the organization's module
        integration_module = f"integrations.{organization_settings.organization}.integration"
        spec = importlib_util.find_spec(integration_module)
        if spec is None:
            raise ImportError(f"Cannot find module {integration_module}")

        module = importlib_util.module_from_spec(spec)
        if spec.loader is None:
            raise ImportError(f"Cannot load {integration_module}")
        spec.loader.exec_module(module)

        if not hasattr(module, "Integration"):
            raise AttributeError("Integration class not found in module")

        return getattr(module, "Integration")(organization_settings, client)

    def integrate_regulations(self) -> None:
        """
        Integrate regulations from all data sources.
        Iterates over data sources, collects data, and integrates.
        """
        # Get all data sources
        # Collect clean data from all sources
        all_clean_data = []
        for data_source in self.data_sources:
            logger.info(f"Processing data source: {data_source.name}")
            clean_data = data_source(
                self.organization_settings, self.client
            ).compute_data_regulations()
            all_clean_data.append(clean_data)

        # Concatenate all data
        combined_data = pl.concat(all_clean_data, how="vertical")

        logger.info(f"Total records from all sources: {combined_data.shape[0]}")

        # Create regulations from combined data
        regulations = self.create_regulations(combined_data)
        for regulation in regulations:
            regulation.identifier = f"{regulation.identifier}-0"
            regulation.status = self.status
        num_measures = sum([len(regulation.measures or []) for regulation in regulations])
        logger.info(f"Created {len(regulations)} regulations with a total of {num_measures}")

        # Get existing regulation IDs
        integrated_regulation_ids = self.fetch_regulation_ids()

        # Filter to only new regulations
        regulation_ids_to_integrate = set([r.identifier for r in regulations]) - set(
            integrated_regulation_ids
        )
        regulations_to_integrate = [
            regulation
            for regulation in regulations
            if regulation.identifier in regulation_ids_to_integrate
        ]
        logger.info(f"Found {len(regulations_to_integrate)} new regulations to integrate")

        # Integrate new regulations
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
            logger.info(f"Contains {len(regulation.measures)} measures.")  # type: ignore
            try:
                resp = add_regulation(client=self.client, body=regulation)
            except Exception as e:
                logger.error(f"Failed to create: {regulation.identifier} - {e}")
                count_error += 1
            else:
                if resp.status_code != 201:
                    logger.error(
                        f"Failed to create: {regulation.identifier} - got status {resp.status_code}"
                    )
                    logger.error(json.loads(resp.content))
                    count_error += 1

        count_success = len(regulations) - count_error
        logger.success(
            f"Finished integrating {count_success}/{len(regulations)} regulations successfully"
        )

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
                    status=PostApiRegulationsAddBodyStatus(self.status),
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
