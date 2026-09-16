"""One organization: its data sources, and what it does with their regulations.

The class is the orchestrator. Building the payloads lives in `payloads.py`, talking to
DiaLog in `api.py`; an organization's `integration.py` subclasses this and declares its
data sources and default status.
"""

from importlib import util as importlib_util

import polars as pl
from loguru import logger

from api.dia_log_client import Client
from api.dia_log_client.models import (
    PostApiRegulationsAddBody,
    PostApiRegulationsAddBodyStatus,
    SaveLocationDTO,
    SaveMeasureDTO,
    SavePeriodDTO,
    SaveVehicleSetDTO,
)
from integrations import payloads
from integrations.api import DialogApi, build_client
from integrations.base_data_source_integration import BaseDataSourceIntegration, RegulationMeasure
from integrations.shared.zone_sections import MAX_SECTIONS_PER_LENGTH, MIN_SECTION_LENGTH_M
from integrations.zone_flow import create_zone_regulation, has_zone
from settings import OrganizationSettings


class BaseIntegration:
    """Base integration class that orchestrates data sources and API interactions."""

    client: Client
    api: DialogApi
    status: PostApiRegulationsAddBodyStatus = PostApiRegulationsAddBodyStatus.DRAFT
    organization_settings: OrganizationSettings
    data_sources: list[type[BaseDataSourceIntegration]]

    # When True, a regulation whose locations are zones is created through
    # `zone_flow.create_zone_regulation`: four calls that republish only the street
    # sections worth publishing, and refuse a zone covering several parallel roads.
    resolve_zones_to_sections: bool = False
    min_section_length_m: float = MIN_SECTION_LENGTH_M
    max_sections_per_length: float = MAX_SECTIONS_PER_LENGTH

    def __init__(self, organization_settings: OrganizationSettings, client: Client):  # type: ignore
        self.organization_settings = organization_settings
        self.client = client
        self.api = DialogApi(client)

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
        client = build_client(organization_settings)

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

    # --- orchestration ------------------------------------------------------------

    def integrate_regulations(self, limit_to=[], update_existing: bool | None = None) -> None:
        """
        Integrate regulations from all data sources.
        Iterates over data sources, collects data, and integrates.
        """
        # Get existing regulation IDs
        try:
            integrated_regulation_ids = self.fetch_regulation_ids()
        except Exception as e:
            logger.info(
                f"Could not fetch existing regulation ids"
                f", defaulting to update_existing=False - {e}"
            )
            integrated_regulation_ids = []
            update_existing = False

        for data_source in self.data_sources:
            logger.info(f"Processing data source: {data_source.name}")
            clean_data = data_source(
                self.organization_settings, self.client
            ).compute_data_regulations()

            # Only process whitelisted identifiers
            if limit_to and len(limit_to) > 0:
                logger.info(f"Limiting processing to following ids : {limit_to}")
                clean_data = clean_data.filter(pl.col("regulation_identifier").is_in(limit_to))

            logger.info(f"Total records from all sources: {clean_data.shape[0]}")

            # Create regulations from combined data
            regulations = self.create_regulations(clean_data)
            for regulation in regulations:
                regulation.identifier = f"{regulation.identifier}"
                regulation.status = self.status
            num_measures = sum([len(regulation.measures or []) for regulation in regulations])
            logger.info(f"Created {len(regulations)} regulations with a total of {num_measures}")

            # Filter regulations to create
            regulation_ids_to_create = set([r.identifier for r in regulations]) - set(
                integrated_regulation_ids
            )
            regulations_to_create = [
                regulation
                for regulation in regulations
                if regulation.identifier in regulation_ids_to_create
            ]
            logger.info(f"Found {len(regulations_to_create)} new regulations to integrate")

            # Integrate new regulations
            self._integrate_regulations_add(regulations_to_create)

            if update_existing:
                # Filter regulations to update
                regulation_ids_to_update = set([r.identifier for r in regulations]) & set(
                    integrated_regulation_ids
                )
                regulations_to_update = [
                    regulation
                    for regulation in regulations
                    if regulation.identifier in regulation_ids_to_update
                ]
                logger.info(f"Found {len(regulations_to_update)} regulations to update")

                # Integrate updated regulations
                self._integrate_regulations_update(regulations_to_update)

    def publish_regulations(self) -> None:
        regulation_ids = self.fetch_regulation_ids()
        count_error = 0
        for index, regulation_id in enumerate(regulation_ids):
            if self.api.publish(regulation_id):
                logger.success(
                    f"Measure {index}/{len(regulation_ids)} successfully published: {regulation_id}"
                )
            else:
                logger.error(
                    f"Measure {index}/{len(regulation_ids)} failed to publish: {regulation_id}"
                )
                count_error += 1

        if count_error > 0:
            logger.error(f"Failed to publish {count_error} identifier(s)")
        logger.success(
            f"Finished publishing {len(regulation_ids) - count_error} measures successfully"
        )

    def fetch_regulation_ids(self) -> list[str]:
        logger.info(f"Fetching identifiers for organization: {self.organization}")
        identifiers = self.api.identifiers()
        logger.info(f"Found {len(identifiers)} identifier(s) for organization {self.organization}")
        return identifiers

    def _integrate_regulations_add(self, regulations: list[PostApiRegulationsAddBody]) -> None:
        count_error = count_refused = 0
        for index, regulation in enumerate(regulations):
            logger.info(
                f"Creating regulation {index + 1}/{len(regulations)}: {regulation.identifier}"
            )
            logger.info(f"Contains {len(regulation.measures)} measures.")  # type: ignore
            outcome = self._create_regulation(regulation)
            if outcome == "failed":
                count_error += 1
            elif outcome == "refused":
                count_refused += 1

        count_success = len(regulations) - count_error - count_refused
        if count_refused:
            logger.warning(f"{count_refused} regulation(s) refused by the pipeline itself")
        logger.success(
            f"Finished integrating {count_success}/{len(regulations)} regulations successfully"
        )

    def _create_regulation(self, regulation: PostApiRegulationsAddBody) -> str:
        """One regulation, through the zone flow when it applies."""
        if self.resolve_zones_to_sections and has_zone(regulation):
            return create_zone_regulation(
                self.api,
                regulation,
                min_length_m=self.min_section_length_m,
                max_sections_per_length=self.max_sections_per_length,
            )
        return "created" if self.api.add(regulation) else "failed"

    def _integrate_regulations_update(self, regulations: list[PostApiRegulationsAddBody]) -> None:
        """DELETE then POST. If the POST fails the regulation is gone (D-06); unchanged here."""
        count_error = 0
        for index, regulation in enumerate(regulations):
            logger.info(
                f"Updating regulation {index + 1}/{len(regulations)}: {regulation.identifier}"
            )
            logger.info(f"Contains {len(regulation.measures)} measures.")  # type: ignore
            identifier = str(regulation.identifier)
            if not (self.api.delete(identifier) and self.api.add(regulation)):
                count_error += 1

        count_success = len(regulations) - count_error
        logger.success(
            f"Finished updating {count_success}/{len(regulations)} regulations successfully"
        )

    # --- payloads: kept as methods so an organization can override one ------------

    def create_regulations(self, clean_data: pl.DataFrame) -> list[PostApiRegulationsAddBody]:
        return payloads.build_regulations(clean_data, self.status, self.create_measure)

    def create_measure(self, measure: RegulationMeasure) -> SaveMeasureDTO:
        return payloads.build_measure(measure)

    def create_save_period_dto(self, measure: RegulationMeasure) -> SavePeriodDTO:
        return payloads.build_period(measure)

    def create_save_location_dto(self, measure: RegulationMeasure) -> SaveLocationDTO:
        return payloads.build_location(measure)

    def create_save_vehicle_dto(self, measure: RegulationMeasure) -> SaveVehicleSetDTO:
        return payloads.build_vehicle_set(measure)
