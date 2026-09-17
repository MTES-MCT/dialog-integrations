"""One organization: its data sources, and what it does with their regulations.

The class is the orchestrator. Building the payloads lives in `payloads.py`, talking to
DiaLog in `api.py`, zones in `zone_flow.py`, and deciding what to create, update,
close and delete in `sync/`; an organization's `integration.py` subclasses this and declares its
data sources, default status and synchronization options.
"""

from collections.abc import Sequence
from datetime import datetime
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
from integrations.sync.closure import (
    ClosureNotRebuildable,
    close_payload,
    closing_instant,
    save_payload_from_read,
)
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

        Four operations: create what DiaLog does not have, update what changed since
        the snapshot of our last send, delete or close what left the source (inside
        our prefix only). `dry_run` computes everything and writes nothing — neither
        to DiaLog nor to the snapshot.
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

            source_regulations = self.create_regulations(clean_data, source)
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

        # The snapshot only exists for organizations that opted into update detection
        # or closure: both need to know what was last sent.
        snapshot_by_source: dict[str, dict[str, Digest]] = {}
        snapshot = (
            self._load_snapshot(stores, snapshot_by_source)
            if self.update_changed or self.close_missing
            else None
        )
        closed_at = closing_instant()

        plan = reconcile(
            digests,
            remote_identifiers,
            snapshot,
            identifier_prefix=self.identifier_prefix,
            update_mode=update_mode,
            delete_missing=self.delete_missing,
            close_missing=self.close_missing,
            closed_at=closed_at,
            max_creations=self.max_creations_per_run,
            max_updates=self.max_updates_per_run,
            max_deletions=self.max_deletions_per_run,
            max_closures=self.max_closures_per_run,
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
        closed_digests: dict[str, Digest] = {}
        if plan.closures is not None:
            closed_digests = self._close_regulations(plan.closures.applicable, closed_at)
            outcome.closed = len(closed_digests)

        outcome.created = len(created)
        outcome.updated = len(updated)
        outcome.deleted = len(deleted)
        outcome.refused = len(refused)
        attempted = (
            len(plan.creations.applicable)
            + len(plan.updates.applicable)
            + len(plan.deletions.applicable)
            + (len(plan.closures.applicable) if plan.closures is not None else 0)
        )
        outcome.errors = (
            attempted
            - outcome.created
            - outcome.updated
            - outcome.deleted
            - (outcome.closed or 0)
            - outcome.refused
        )
        # Not in DiaLog after the run: the creations that were held, refused or failed.
        not_integrated = set(plan.creations.identifiers) - set(created)
        outcome.regulations -= len(not_integrated)
        outcome.measures -= sum(len(regulations[i].measures or []) for i in not_integrated)

        if self.update_changed or self.close_missing:
            # Closed today, or missing from the source and still in DiaLog: these stay
            # in the snapshot so tomorrow knows they have ended (or retries a closure
            # that failed or was held).
            carried: dict[str, Digest] = dict(closed_digests)
            if self.close_missing and snapshot:
                remaining = set(plan.already_ended) | (
                    set(plan.closures.identifiers) - set(closed_digests)
                    if plan.closures is not None
                    else set()
                )
                carried.update(
                    {i: snapshot[i] for i in remaining if i in snapshot and i not in carried}
                )
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
                carried=carried,
                snapshot_by_source=snapshot_by_source,
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
    def _load_snapshot(
        stores: dict[str, SnapshotStore],
        by_source: dict[str, dict[str, Digest]] | None = None,
    ) -> dict[str, Digest] | None:
        """Merge every source snapshot, or None when not a single one exists.

        `by_source`, when given, receives each source's own digests: a regulation that
        left its source is written back to the snapshot it came from.
        """
        merged: dict[str, Digest] = {}
        found = False
        for name, store in stores.items():
            loaded = store.load()
            if loaded is not None:
                found = True
                merged.update(loaded)
                if by_source is not None:
                    by_source[name] = loaded
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
        carried: dict[str, Digest] | None = None,
        snapshot_by_source: dict[str, dict[str, Digest]] | None = None,
    ) -> None:
        """Record what is now in DiaLog, one snapshot per data source.

        `carried` holds regulations absent from today's production that must stay in
        the snapshot (closed, or already ended): each goes back to the source snapshot
        that held it, or to the first source when no snapshot remembers it.
        """
        previous = snapshot or {}
        carried = carried or {}
        by_source = snapshot_by_source or {}
        first = next(iter(stores), None)
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
            for identifier, digest in carried.items():
                if identifier in source_of:
                    continue
                owners = [n for n, loaded in by_source.items() if identifier in loaded]
                if name == (owners[0] if owners else first):
                    state[identifier] = digest
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

    # --- the write passes ----------------------------------------------------

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

    def _close_regulations(
        self, identifiers: Sequence[str], closed_at: datetime
    ) -> dict[str, Digest]:
        """Bring the end date of every identifier back to `closed_at`, keeping it in DiaLog.

        The source no longer has the row, so each regulation is read back from DiaLog,
        rebuilt as a write payload and sent through `PUT` with its periods clamped
        (`sync/closure.py`). One that already ended on its own needs no write. Returns,
        for every regulation now known to be closed, the digest of what DiaLog holds —
        it goes into the snapshot so tomorrow skips it without a call.
        """
        closed: dict[str, Digest] = {}
        for index, identifier in enumerate(identifiers):
            logger.info(f"Closing regulation {index + 1}/{len(identifiers)}: {identifier}")
            read = self.api.get(identifier)
            if read is None:
                continue
            try:
                payload = save_payload_from_read(read)
            except ClosureNotRebuildable as e:
                logger.error(f"Cannot close {identifier}, it would lose a detail: {e}")
                continue

            closed_payload, changed = close_payload(payload, closed_at)
            regulation = PostApiRegulationsAddBody.from_dict(closed_payload)
            digest = compute_regulation_digest(regulation)
            if not changed:
                logger.info(f"{identifier} had already ended, nothing to write")
                closed[identifier] = digest
                continue
            if not self.api.update(regulation):
                continue
            logger.success(
                f"{identifier} closed: its periods now end on "
                f"{closed_at.isoformat(timespec='seconds')} at the latest"
            )
            closed[identifier] = digest

        if identifiers:
            logger.success(f"Finished closing {len(closed)}/{len(identifiers)} regulations")
        return closed

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

    def create_regulations(
        self, clean_data: pl.DataFrame, source: BaseDataSourceIntegration | None = None
    ) -> list[PostApiRegulationsAddBody]:
        """The source decides how its rows fold into measures and regulations."""
        return payloads.build_regulations(
            clean_data,
            self.status,
            self.create_measure,
            group_locations_by_measure=bool(source and source.group_locations_by_measure),
            max_locations_per_regulation=source.max_locations_per_regulation if source else None,
        )

    def create_measure(
        self, measure: RegulationMeasure, locations: list[RegulationMeasure] | None = None
    ) -> SaveMeasureDTO:
        return payloads.build_measure(measure, locations)

    def create_save_period_dto(self, measure: RegulationMeasure) -> SavePeriodDTO:
        return payloads.build_period(measure)

    def create_save_location_dto(self, measure: RegulationMeasure) -> SaveLocationDTO:
        return payloads.build_location(measure)

    def create_save_vehicle_dto(self, measure: RegulationMeasure) -> SaveVehicleSetDTO:
        return payloads.build_vehicle_set(measure)
