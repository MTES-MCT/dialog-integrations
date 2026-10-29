"""What `dialog integrate` does for one organization, step by step."""

from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

from report.sync_report import render_report
from run import datasets, sources, state, writes
from run.freeze import freeze_collapsed_sources, frozen_prefixes
from sync.closure import closing_instant
from sync.digest import Digest
from sync.outcome import IntegrationOutcome
from sync.plan import MissingPrefixError, UpdateMode, reconcile
from sync.totals import TotalsStore

if TYPE_CHECKING:
    from run.organization import BaseIntegration


def run(
    integration: BaseIntegration,
    limit_to,
    update_existing: bool | None,
    dry_run: bool,
    force_deletions: bool,
) -> IntegrationOutcome:
    if not integration.identifier_prefix and not integration.legacy_additive:
        raise MissingPrefixError(
            f"{integration.organization} declares no identifier_prefix: declare one, or set "
            "legacy_additive to say that what it creates is never updated nor removed."
        )
    outcome = IntegrationOutcome(organization=integration.organization, dry_run=dry_run)
    # With `--identifiers`, the rest of the prefix is filtered out, not gone from the
    # source: nothing is deleted or closed, and the snapshot is left as it was.
    whole_organization = not limit_to

    # Must raise on failure: falling back to "this organization holds nothing"
    # would re-create the whole corpus as duplicates.
    remote_identifiers = integration.fetch_regulation_ids()

    production = sources.run_all(integration, outcome, limit_to)

    # Only update detection and closure need to know what was last sent.
    snapshot_by_source: dict[str, dict[str, Digest]] = {}
    snapshot = (
        state.load_snapshot(production.stores, snapshot_by_source)
        if integration.update_changed or integration.close_missing
        else None
    )
    closed_at = closing_instant()

    totals = TotalsStore(integration.organization, target=integration.target)
    previous_totals = totals.load() if whole_organization else None
    freeze_collapsed_sources(
        outcome, previous_totals, production.produced_by_source, force_deletions
    )

    plan = reconcile(
        production.digests,
        remote_identifiers,
        snapshot,
        identifier_prefix=integration.identifier_prefix,
        update_mode=update_mode(integration, update_existing),
        delete_missing=integration.delete_missing and whole_organization,
        close_missing=integration.close_missing and whole_organization,
        closed_at=closed_at,
        max_creations=integration.max_creations_per_run,
        max_updates=integration.max_updates_per_run,
        max_deletions=integration.max_deletions_per_run,
        max_closures=integration.max_closures_per_run,
        force_deletions=force_deletions,
        unbuilt=production.unbuilt,
        frozen_prefixes=frozen_prefixes(outcome.frozen_sources, production.prefix_of),
    )
    if plan.frozen:
        logger.warning(
            f"{len(plan.frozen)} regulations no longer produced are left as DiaLog has "
            f"them, their source being frozen: {list(plan.frozen)[:10]}"
        )
    outcome.planned = plan.planned
    outcome.held = plan.held
    if not_built := production.unbuilt:
        logger.warning(
            f"{len(not_built)} regulations could not be built, left as DiaLog has them: "
            f"{sorted(not_built)[:10]}"
        )
        outcome.refused += len(not_built)
    regulations = production.regulations
    outcome.regulations = outcome.produced = len(regulations)
    outcome.measures = sum(len(r.measures or []) for r in regulations.values())
    counted = [funnel.raw_rows for funnel in production.funnels if funnel.raw_rows is not None]
    outcome.raw_rows = sum(counted) if counted else None
    outcome.datasets = datasets.summaries(
        production.funnels, regulations, production.source_of, set()
    )
    outcome.report = render_report(
        organization=integration.organization,
        environment=getattr(integration.organization_settings, "env", "dev"),
        dry_run=dry_run,
        plan=plan,
        funnels=production.funnels,
        produced=production.digests,
        snapshot=snapshot,
        force_deletions=force_deletions,
        frozen_sources=outcome.frozen_sources,
        volume_drops=outcome.volume_drops,
    )
    # Logged before writing anything, so a real run leaves the same trace as a
    # dry run in the CI log.
    logger.info(outcome.report)

    if dry_run:
        logger.info("Dry run: nothing sent to DiaLog, snapshot left untouched")
        return outcome

    written = writes.apply(integration, plan, regulations, outcome, closed_at, force_deletions)
    # Not in DiaLog after the run: the creations that were held, refused or failed.
    not_integrated = set(plan.creations.identifiers) - set(written.created)
    outcome.regulations -= len(not_integrated)
    outcome.measures -= sum(len(regulations[i].measures or []) for i in not_integrated)
    outcome.datasets = datasets.summaries(
        production.funnels, regulations, production.source_of, not_integrated
    )

    if whole_organization and (integration.update_changed or integration.close_missing):
        state.save_snapshots(production, plan, snapshot, snapshot_by_source, written)
    if whole_organization:
        state.save_totals(totals, previous_totals, production, plan, outcome)

    return outcome


def update_mode(integration: BaseIntegration, update_existing: bool | None) -> UpdateMode:
    """Reconcile the CLI flag with the organization's own setting.

    `--update-existing` replaces everything already in DiaLog, whatever the snapshot
    says: it is used by hand with `--identifiers`.
    """
    if update_existing is True:
        return "all"
    if update_existing is False:
        return "none"
    return "changed" if integration.update_changed else "none"
