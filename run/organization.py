"""One organization: what its `integration.py` declares, and the entry points that run it.

An organization's `integration.py` subclasses `BaseIntegration` and declares its data
sources, default status and synchronization options. The run itself is `integrate.py`.
"""

from importlib import util as importlib_util
from urllib.parse import urlparse

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
from canonical.regulation_measure import RegulationMeasure
from canonical.source import BaseDataSourceIntegration
from dialog.calls import DialogApi
from dialog.client import build_client
from dialog.zone_sections import MAX_SECTIONS_PER_LENGTH, MIN_SECTION_LENGTH_M
from payloads.measures import build_location, build_measure, build_period, build_vehicle_set
from payloads.regulations import build_regulations
from run import integrate, writes
from settings import OrganizationSettings
from sync.outcome import IntegrationOutcome


class BaseIntegration:
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

    # --- Synchronization, opt-in per organization ---------------------------------
    # Defaults are purely additive: create what is missing, never update, never delete.
    #
    # `identifier_prefix` bounds every destructive operation to the regulations this
    # pipeline owns; without it deletion is refused, not merely disabled. An organization
    # without one sets `legacy_additive`: what it creates stays in DiaLog for good, which
    # has to be a decision, not an omission.
    identifier_prefix: str | None = None
    legacy_additive: bool = False
    # Set from `integrations.toml`, never in an `integration.py`.
    paused_sources: frozenset[str] = frozenset()
    max_deletions_per_run: int | None = None
    max_closures_per_run: int | None = None
    max_updates_per_run: int | None = None
    max_creations_per_run: int | None = None
    # What happens to a regulation that left the source: deleted, or closed — its end
    # date brought back to the day before the run, the regulation itself kept
    # (`sync/closure.py`). At most one of the two.
    delete_missing: bool = False
    close_missing: bool = False
    update_changed: bool = False

    def __init__(self, organization_settings: OrganizationSettings, client: Client):  # type: ignore
        self.organization_settings = organization_settings
        self.client = client
        self.api = DialogApi(client)

    @property
    def organization(self) -> str:
        return self.organization_settings.organization

    @property
    def target(self) -> str | None:
        """The DiaLog host written to; it keys the state kept between runs."""
        base_url = getattr(self.organization_settings, "base_url", None) or ""
        return urlparse(base_url).hostname or base_url.strip("/") or None

    @classmethod
    def from_organization(cls, organization: str, env: str = "dev") -> "BaseIntegration":
        """Create Integration from organization name and environment."""
        organization_settings = OrganizationSettings.from_env(organization, env=env)
        return cls.from_settings(organization_settings)

    @classmethod
    def from_settings(cls, organization_settings: OrganizationSettings) -> "BaseIntegration":
        """Create Integration from pre-configured settings."""
        # Which instance we are about to write to: nothing else in the logs says it.
        logger.info(
            f"DiaLog target for {organization_settings.organization}: "
            f"{organization_settings.base_url}"
        )
        client = build_client(organization_settings)

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

    # --- running it ---------------------------------------------------------------

    def integrate_regulations(
        self,
        limit_to=[],
        update_existing: bool | None = None,
        dry_run: bool = False,
        force_deletions: bool = False,
    ) -> IntegrationOutcome:
        """Make DiaLog match what the data sources produce today.

        Four operations: create what DiaLog does not have, update what changed since
        the snapshot of our last send, delete or close what left the source (inside
        our prefix only). `dry_run` computes everything and writes nothing — neither
        to DiaLog nor to the snapshot.
        """
        return integrate.run(self, limit_to, update_existing, dry_run, force_deletions)

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

    # Called through the instance, so that a test can replace one of them.

    def _integrate_regulations_add(
        self, regulations: list[PostApiRegulationsAddBody]
    ) -> tuple[list[str], list[str]]:
        return writes.create_all(self, regulations)

    def _integrate_regulations_update(
        self, regulations: list[PostApiRegulationsAddBody]
    ) -> tuple[list[str], list[str]]:
        return writes.update_all(self, regulations)

    def _create_regulation(self, regulation: PostApiRegulationsAddBody) -> str:
        return writes.create_one(self, regulation)

    # --- payloads -------------------------------------------------------------------
    # `create_regulations` and `create_measure` are the override points. The three
    # `create_save_*_dto` below are called by nobody: `build_measure` calls
    # `build_period` & co. directly, so overriding them has no effect.

    def create_regulations(
        self,
        clean_data: pl.DataFrame,
        source: BaseDataSourceIntegration | type[BaseDataSourceIntegration] | None = None,
        unbuilt: set[str] | None = None,
    ) -> list[PostApiRegulationsAddBody]:
        """The source decides how its rows fold into measures and regulations.

        Only class attributes are read, so the class itself does as well as an instance
        (tests build payloads without instantiating a source).
        """
        return build_regulations(
            clean_data,
            self.status,
            self.create_measure,
            group_locations_by_measure=bool(source and source.group_locations_by_measure),
            max_locations_per_regulation=source.max_locations_per_regulation if source else None,
            unbuilt=unbuilt,
        )

    def create_measure(
        self, measure: RegulationMeasure, locations: list[RegulationMeasure] | None = None
    ) -> SaveMeasureDTO:
        return build_measure(measure, locations)

    def create_save_period_dto(self, measure: RegulationMeasure) -> SavePeriodDTO:
        return build_period(measure)

    def create_save_location_dto(self, measure: RegulationMeasure) -> SaveLocationDTO:
        return build_location(measure)

    def create_save_vehicle_dto(self, measure: RegulationMeasure) -> SaveVehicleSetDTO:
        return build_vehicle_set(measure)
