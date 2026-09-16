"""One organization: its data sources, and what it does with their regulations.

The class is the orchestrator. Building the payloads lives in `payloads.py`, talking to
DiaLog in `api.py`, zones in `zone_flow.py`, and deciding what to create, update and
delete in `sync/`; an organization's `integration.py` subclasses this and declares its
data sources, default status and synchronization options.
"""

from collections.abc import Sequence
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
from integrations.sync.reconciliation import IntegrationOutcome, UpdateMode, reconcile
from integrations.sync.report import SourceFunnel, render_report
from integrations.sync.state import Digest, SnapshotStore, compute_regulation_digest
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

    # --- Synchronization, opt-in per organization ---------------------------------
    # These defaults reproduce the historical behavior: purely additive, create what is
    # missing, never update, never delete. An organization opts in by overriding them.
    #
    # `identifier_prefix` bounds every destructive operation to the regulations this
    # pipeline owns; without it deletion is refused, not merely disabled.
    identifier_prefix: str | None = None
    max_deletions_per_run: int | None = None
    max_updates_per_run: int | None = None
    max_creations_per_run: int | None = None
    delete_missing: bool = False
    update_changed: bool = False

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

    def integrate_regulations(
        self,
        limit_to=[],
        update_existing: bool | None = None,
        dry_run: bool = False,
        force_deletions: bool = False,
    ) -> IntegrationOutcome:
        """Make DiaLog match what the data sources produce today.

        Three operations: create what DiaLog does not have, update what changed since
        the snapshot of our last send, delete what left the source (inside our prefix
        only). `dry_run` computes everything and writes nothing — neither to DiaLog nor
        to the snapshot.
        """
        outcome = IntegrationOutcome(organization=self.organization, dry_run=dry_run)

        # A failure here used to fall back to "this organization holds nothing", which
        # re-created the whole corpus as duplicates. It now stops the run.
        remote_identifiers = self.fetch_regulation_ids()

        update_mode = self._update_mode(update_existing)
        regulations: dict[str, PostApiRegulationsAddBody] = {}
        digests: dict[str, Digest] = {}
        source_of: dict[str, str] = {}
        funnels: list[SourceFunnel] = []
        stores: dict[str, SnapshotStore] = {}

        for data_source in self.data_sources:
            name = data_source.name or data_source.__name__
            logger.info(f"Processing data source: {name}")
            source = data_source(self.organization_settings, self.client)
            clean_data, raw_rows = self._compute_clean_data(source)

            # Only process whitelisted identifiers
            if limit_to and len(limit_to) > 0:
                logger.info(f"Limiting processing to following ids : {limit_to}")
                clean_data = clean_data.filter(pl.col("regulation_identifier").is_in(limit_to))

            logger.info(f"Total records from source {name}: {clean_data.shape[0]}")

            source_regulations = self.create_regulations(clean_data)
            for regulation in source_regulations:
                regulation.identifier = f"{regulation.identifier}"
                regulation.status = self.status
            num_measures = sum(
                [len(regulation.measures or []) for regulation in source_regulations]
            )
            logger.info(
                f"Created {len(source_regulations)} regulations with a total of {num_measures}"
            )

            metrics = self._source_metrics(source)
            outcome.source.update(metrics)
            funnels.append(
                SourceFunnel(
                    name=name,
                    raw_rows=raw_rows,
                    clean_rows=clean_data.shape[0],
                    regulations=len(source_regulations),
                    measures=num_measures,
                    metrics=metrics,
                )
            )

            for regulation in source_regulations:
                identifier = str(regulation.identifier)
                if identifier in regulations:
                    logger.warning(
                        f"Identifier {identifier} is produced by two data sources; "
                        f"keeping the one from {name}"
                    )
                regulations[identifier] = regulation
                digests[identifier] = compute_regulation_digest(regulation)
                source_of[identifier] = name
            stores[name] = SnapshotStore(self.organization, name)

        # The snapshot only exists for organizations that opted into update detection.
        snapshot = self._load_snapshot(stores) if self.update_changed else None

        plan = reconcile(
            digests,
            remote_identifiers,
            snapshot,
            identifier_prefix=self.identifier_prefix,
            update_mode=update_mode,
            delete_missing=self.delete_missing,
            max_creations=self.max_creations_per_run,
            max_updates=self.max_updates_per_run,
            max_deletions=self.max_deletions_per_run,
            force_deletions=force_deletions,
        )
        outcome.planned = plan.planned
        outcome.held = plan.held
        outcome.regulations = len(regulations)
        outcome.measures = sum(len(r.measures or []) for r in regulations.values())
        counted = [funnel.raw_rows for funnel in funnels if funnel.raw_rows is not None]
        outcome.raw_rows = sum(counted) if counted else None
        outcome.report = render_report(
            organization=self.organization,
            environment=getattr(self.organization_settings, "env", "dev"),
            dry_run=dry_run,
            plan=plan,
            funnels=funnels,
            produced=digests,
            snapshot=snapshot,
            force_deletions=force_deletions,
        )
        # Logged before writing anything, so a real run leaves the same trace as a
        # dry run in the CI log.
        logger.info(outcome.report)

        if dry_run:
            logger.info("Dry run: nothing sent to DiaLog, snapshot left untouched")
            return outcome

        created, refused = self._integrate_regulations_add(
            [regulations[identifier] for identifier in plan.creations.applicable]
        )
        updated = self._integrate_regulations_update(
            [regulations[identifier] for identifier in plan.updates.applicable]
        )
        deleted = self._delete_regulations(plan.deletions.applicable)

        outcome.created = len(created)
        outcome.updated = len(updated)
        outcome.deleted = len(deleted)
        outcome.refused = len(refused)
        attempted = (
            len(plan.creations.applicable)
            + len(plan.updates.applicable)
            + len(plan.deletions.applicable)
        )
        outcome.errors = (
            attempted - outcome.created - outcome.updated - outcome.deleted - outcome.refused
        )
        # Not in DiaLog after the run: the creations that were held, refused or failed.
        not_integrated = set(plan.creations.identifiers) - set(created)
        outcome.regulations -= len(not_integrated)
        outcome.measures -= sum(len(regulations[i].measures or []) for i in not_integrated)

        if self.update_changed:
            self._save_snapshots(
                stores=stores,
                source_of=source_of,
                digests=digests,
                snapshot=snapshot,
                # Held or failed updates keep their previous fingerprint, so tomorrow
                # detects them again, identically.
                keep_previous=set(plan.updates.identifiers) - set(updated),
                # Nothing was written for these: leave them out so they are retried.
                skipped=set(plan.creations.identifiers) - set(created),
            )

        return outcome

    def _update_mode(self, update_existing: bool | None) -> UpdateMode:
        """Reconcile the CLI flag with the organization's own setting.

        `--update-existing` keeps its historical meaning — replace everything already in
        DiaLog, whatever the snapshot says — because it is used by hand together with
        `--identifiers`. `update_changed` is the daily, snapshot-driven mode.
        """
        if update_existing is True:
            return "all"
        if update_existing is False:
            return "none"
        return "changed" if self.update_changed else "none"

    @staticmethod
    def _compute_clean_data(source: BaseDataSourceIntegration) -> tuple[pl.DataFrame, int | None]:
        """Run the source pipeline, counting the raw rows it read on the way.

        The count is taken by wrapping the instance's `fetch_raw_data`, so a source that
        overrides `compute_data_regulations` is measured just the same, and no data
        source has to know about the report.
        """
        raw_rows: list[int] = []
        original_fetch = source.fetch_raw_data

        def counting_fetch(*args, **kwargs):
            raw = original_fetch(*args, **kwargs)
            raw_rows.append(int(getattr(raw, "height", 0)))
            return raw

        setattr(source, "fetch_raw_data", counting_fetch)
        try:
            clean_data = source.compute_data_regulations()
        finally:
            # The wrapper only ever lived on the instance; the class is left alone.
            source.__dict__.pop("fetch_raw_data", None)

        return clean_data, sum(raw_rows) if raw_rows else None

    @staticmethod
    def _source_metrics(source: BaseDataSourceIntegration) -> dict[str, int]:
        """Read the optional `metrics` a data source may expose.

        A source that measures its own volumes sets `self.metrics: dict[str, int]` while
        fetching. Keys are the French labels shown in the Tchap message. Sources that
        define nothing are unaffected.
        """
        metrics = getattr(source, "metrics", None)
        if not isinstance(metrics, dict):
            return {}
        return {
            str(name): int(value)
            for name, value in metrics.items()
            if isinstance(value, (int, float)) and not isinstance(value, bool)
        }

    @staticmethod
    def _load_snapshot(stores: dict[str, SnapshotStore]) -> dict[str, Digest] | None:
        """Merge every source snapshot, or None when not a single one exists."""
        merged: dict[str, Digest] = {}
        found = False
        for store in stores.values():
            loaded = store.load()
            if loaded is not None:
                found = True
                merged.update(loaded)
        return merged if found else None

    @staticmethod
    def _save_snapshots(
        *,
        stores: dict[str, SnapshotStore],
        source_of: dict[str, str],
        digests: dict[str, Digest],
        snapshot: dict[str, Digest] | None,
        keep_previous: set[str],
        skipped: set[str],
    ) -> None:
        """Record what is now in DiaLog, one snapshot per data source."""
        previous = snapshot or {}
        for name, store in stores.items():
            state: dict[str, Digest] = {}
            for identifier, owner in source_of.items():
                if owner != name or identifier in skipped:
                    continue
                if identifier in keep_previous:
                    if identifier in previous:
                        state[identifier] = previous[identifier]
                    continue
                state[identifier] = digests[identifier]
            store.save(state)

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

    # --- the three write passes ----------------------------------------------------

    def _integrate_regulations_add(
        self, regulations: list[PostApiRegulationsAddBody]
    ) -> tuple[list[str], list[str]]:
        """Create every regulation; return (created, refused by the pipeline itself)."""
        created: list[str] = []
        refused: list[str] = []
        for index, regulation in enumerate(regulations):
            identifier = str(regulation.identifier)
            logger.info(f"Creating regulation {index + 1}/{len(regulations)}: {identifier}")
            logger.info(f"Contains {len(regulation.measures)} measures.")  # type: ignore
            outcome = self._create_regulation(regulation)
            if outcome == "created":
                created.append(identifier)
            elif outcome == "refused":
                refused.append(identifier)

        if refused:
            logger.warning(f"{len(refused)} regulation(s) refused by the pipeline itself")
        logger.success(
            f"Finished integrating {len(created)}/{len(regulations)} regulations successfully"
        )
        return created, refused

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

    def _integrate_regulations_update(
        self, regulations: list[PostApiRegulationsAddBody]
    ) -> list[str]:
        """Replace every regulation; return the identifiers actually updated.

        `PUT /api/regulations` replaces the regulation as a whole and supersedes the
        former DELETE-then-POST, which lost the regulation when the POST failed (D-06).
        A regulation carrying zones cannot go through PUT — the API answers 500 on any
        regulation holding a zone, and the zone must be converted to sections again
        anyway — so it is deleted and recreated through the zone flow.
        """
        updated: list[str] = []
        for index, regulation in enumerate(regulations):
            identifier = str(regulation.identifier)
            logger.info(f"Updating regulation {index + 1}/{len(regulations)}: {identifier}")
            logger.info(f"Contains {len(regulation.measures)} measures.")  # type: ignore

            if self.resolve_zones_to_sections and has_zone(regulation):
                if self.api.delete(identifier) and self._create_regulation(regulation) == "created":
                    updated.append(identifier)
            elif self.api.update(regulation):
                updated.append(identifier)

        logger.success(
            f"Finished updating {len(updated)}/{len(regulations)} regulations successfully"
        )
        return updated

    def _delete_regulations(self, identifiers: Sequence[str]) -> list[str]:
        """DELETE every identifier; return those that are gone from DiaLog.

        Callers must have checked the prefix first: this method deletes what it is
        given. An identifier already absent (404) counts as gone: the goal state is
        reached and it leaves the snapshot.
        """
        deleted: list[str] = []
        for index, identifier in enumerate(identifiers):
            logger.info(f"Deleting regulation {index + 1}/{len(identifiers)}: {identifier}")
            if self.api.delete(str(identifier), missing_is_gone=True):
                deleted.append(str(identifier))

        if identifiers:
            logger.success(
                f"Finished deleting {len(deleted)}/{len(identifiers)} regulations successfully"
            )
        return deleted

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
