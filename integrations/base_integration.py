import json
from collections.abc import Sequence
from importlib import util as importlib_util

import polars as pl
from loguru import logger

from api.dia_log_client import Client
from api.dia_log_client.api.private.delete_api_regulations_delete import (
    sync_detailed as delete_regulation,
)
from api.dia_log_client.api.private.get_api_organization_identifiers import (
    sync_detailed as _get_identifiers,
)
from api.dia_log_client.api.private.post_api_regulations_add import (
    sync_detailed as add_regulation,
)
from api.dia_log_client.api.private.put_api_regulations_publish import (
    sync_detailed as publish_regulation,
)
from api.dia_log_client.api.private.put_api_regulations_update import (
    sync_detailed as update_regulation,
)
from api.dia_log_client.models import (
    MeasureTypeEnum,
    PostApiRegulationsAddBody,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodyStatus,
    PostApiRegulationsAddBodySubject,
    PutApiRegulationsUpdateBody,
    RoadTypeEnum,
    SaveLocationDTO,
    SaveMeasureDTO,
    SaveNamedStreetDTO,
    SaveNumberedRoadDTO,
    SavePeriodDTO,
    SaveRawGeoJSONDTO,
    SaveVehicleSetDTO,
)
from integrations.base_data_source_integration import BaseDataSourceIntegration, RegulationMeasure
from integrations.diff_report import SourceFunnel, render_report
from integrations.reconciliation import IntegrationOutcome, UpdateMode, reconcile
from integrations.state import Digest, SnapshotStore, compute_regulation_digest
from settings import OrganizationSettings


class BaseIntegration:
    """Base integration class that orchestrates data sources and API interactions."""

    client: Client
    status: PostApiRegulationsAddBodyStatus = PostApiRegulationsAddBodyStatus.DRAFT
    organization_settings: OrganizationSettings
    data_sources: list[type[BaseDataSourceIntegration]]

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

        created = self._integrate_regulations_add(
            [regulations[identifier] for identifier in plan.creations.applicable]
        )
        updated = self._integrate_regulations_update(
            [regulations[identifier] for identifier in plan.updates.applicable]
        )
        deleted = self._delete_regulations(plan.deletions.applicable)

        outcome.created = len(created)
        outcome.updated = len(updated)
        outcome.deleted = len(deleted)
        attempted = (
            len(plan.creations.applicable)
            + len(plan.updates.applicable)
            + len(plan.deletions.applicable)
        )
        outcome.errors = attempted - outcome.created - outcome.updated - outcome.deleted

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

        A source that measures its own volumes (how many regulations the source holds
        in scope, how many measures) sets `self.metrics: dict[str, int]` while fetching.
        Keys are the French labels shown in the Tchap message. Sources that define
        nothing are unaffected.
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

    def _integrate_regulations_add(self, regulations: list[PostApiRegulationsAddBody]) -> list[str]:
        """POST every regulation; return the identifiers actually created."""
        created: list[str] = []
        for index, regulation in enumerate(regulations):
            logger.info(
                f"Creating regulation {index + 1}/{len(regulations)}: {regulation.identifier}"
            )
            logger.info(f"Contains {len(regulation.measures)} measures.")  # type: ignore
            try:
                resp = add_regulation(client=self.client, body=regulation)
            except Exception as e:
                logger.error(f"Failed to create: {regulation.identifier} - {e}")
            else:
                if resp.status_code == 201:
                    created.append(str(regulation.identifier))
                else:
                    logger.error(
                        f"Failed to create: {regulation.identifier} - got status {resp.status_code}"
                    )
                    logger.error(json.loads(resp.content))

        logger.success(
            f"Finished integrating {len(created)}/{len(regulations)} regulations successfully"
        )
        return created

    def _integrate_regulations_update(
        self, regulations: list[PostApiRegulationsAddBody]
    ) -> list[str]:
        """PUT every regulation; return the identifiers actually updated.

        `PUT /api/regulations` replaces the regulation as a whole. It supersedes the
        former DELETE-then-POST, which lost the regulation when the POST failed (F-05).
        """
        updated: list[str] = []
        for index, regulation in enumerate(regulations):
            logger.info(
                f"Updating regulation {index + 1}/{len(regulations)}: {regulation.identifier}"
            )
            logger.info(f"Contains {len(regulation.measures)} measures.")  # type: ignore

            body = PutApiRegulationsUpdateBody.from_dict(regulation.to_dict())
            try:
                resp = update_regulation(client=self.client, body=body)
            except Exception as e:
                logger.error(f"Failed to update: {regulation.identifier} - {e}")
            else:
                if resp.status_code in (200, 201, 204):
                    updated.append(str(regulation.identifier))
                else:
                    logger.error(
                        f"Failed to update: {regulation.identifier} - got status {resp.status_code}"
                    )
                    logger.error(json.loads(resp.content))

        logger.success(
            f"Finished updating {len(updated)}/{len(regulations)} regulations successfully"
        )
        return updated

    def _delete_regulations(self, identifiers: Sequence[str]) -> list[str]:
        """DELETE every identifier; return those that are gone from DiaLog.

        Callers must have checked the prefix first: this method deletes what it is
        given.
        """
        deleted: list[str] = []
        for index, identifier in enumerate(identifiers):
            logger.info(f"Deleting regulation {index + 1}/{len(identifiers)}: {identifier}")
            try:
                resp = delete_regulation(identifier=str(identifier), client=self.client)
            except Exception as e:
                logger.error(f"Failed to delete: {identifier} - {e}")
                continue

            if resp.status_code in (200, 204):
                deleted.append(str(identifier))
            elif resp.status_code == 404:
                # Already gone: the goal state is reached, drop it from the snapshot.
                logger.warning(f"{identifier} was already absent from DiaLog (404)")
                deleted.append(str(identifier))
            else:
                logger.error(f"Failed to delete: {identifier} - got status {resp.status_code}")

        if identifiers:
            logger.success(
                f"Finished deleting {len(deleted)}/{len(identifiers)} regulations successfully"
            )
        return deleted

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
            if measure.get("measure_max_speed"):
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

            regulation = PostApiRegulationsAddBody(
                identifier=first_row["regulation_identifier"],
                category=PostApiRegulationsAddBodyCategory(first_row["regulation_category"]),
                status=PostApiRegulationsAddBodyStatus(self.status),
                subject=PostApiRegulationsAddBodySubject(first_row["regulation_subject"]),
                title=first_row["regulation_title"],
                other_category_text=first_row.get("regulation_other_category_text"),
                measures=measures,  # type: ignore
            )

            # Add document URL to additional_properties if present
            if first_row.get("regulation_document_url"):
                regulation.additional_properties["documentUrl"] = first_row[
                    "regulation_document_url"
                ]

            regulations.append(regulation)

        return regulations

    def create_save_period_dto(self, measure: RegulationMeasure) -> SavePeriodDTO:
        """
        Create a SavePeriodDTO from a RegulationMeasure with period_ prefixed fields.
        Any field starting with 'period_' will be mapped to SavePeriodDTO,
        with the prefix stripped (e.g., period_start_date -> start_date).

        `startTime` and `endTime` are mirrored from the dates. The API splits a single
        instant across two fields: it takes the day from `startDate` and the clock from
        `startTime`, and reads that clock in Europe/Paris.
        """
        period_fields = {}
        for key, value in measure.items():
            if key.startswith("period_"):
                field_name = key.replace("period_", "", 1)
                period_fields[field_name] = value

        period_fields["start_time"] = period_fields.get("start_date")
        period_fields["end_time"] = period_fields.get("end_date")

        return SavePeriodDTO(**period_fields)

    def create_save_location_dto(
        self, measure: RegulationMeasure
    ) -> SaveLocationDTO | SaveNumberedRoadDTO:
        """
        Create a SaveLocationDTO from a RegulationMeasure with location_ prefixed fields.
        Expects location_road_type (string), location_label, and location_geometry fields.
        """
        road_type_value = measure["location_road_type"]
        road_type = RoadTypeEnum(road_type_value)

        location_fields = {}
        for key, value in measure.items():
            if key.startswith("location_") and key != "location_road_type":
                field_name = key.replace("location_", "", 1)
                location_fields[field_name] = value
        if road_type == RoadTypeEnum.RAWGEOJSON:
            return SaveLocationDTO(
                road_type=road_type,
                raw_geo_json=SaveRawGeoJSONDTO(**location_fields),
            )
        elif road_type in [RoadTypeEnum.DEPARTMENTALROAD, RoadTypeEnum.NATIONALROAD]:
            payload = {
                "road_type": road_type,
                (
                    "national_road"
                    if road_type == RoadTypeEnum.NATIONALROAD
                    else "departmental_road"
                ): SaveNumberedRoadDTO(**location_fields),
            }
            return SaveLocationDTO(**payload)
        elif road_type == RoadTypeEnum.LANE:
            # Named street: DiaLog geocodes cityCode + roadName (+ house numbers or
            # intersections) itself. No geometry is sent.
            return SaveLocationDTO(
                road_type=road_type,
                named_street=SaveNamedStreetDTO(**location_fields),
            )
        else:
            raise Exception(f"Location saving not implemented for RoadType {road_type.value}")

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
