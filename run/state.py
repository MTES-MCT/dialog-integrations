"""What a night leaves for the next one: the snapshot of what was sent, and the volumes."""

from loguru import logger

from payloads.regulations import is_part_of
from run.sources import Production
from run.writes import Written
from sync.digest import Digest
from sync.outcome import IntegrationOutcome
from sync.plan import ReconciliationPlan
from sync.snapshot import SnapshotStore
from sync.totals import Totals, TotalsStore, total_shift


def load_snapshot(
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


def save_snapshots(
    production: Production,
    plan: ReconciliationPlan,
    snapshot: dict[str, Digest] | None,
    snapshot_by_source: dict[str, dict[str, Digest]],
    written: Written,
) -> None:
    # Closed today, missing from the source and still in DiaLog, or not built
    # today: these stay in the snapshot so tomorrow knows they have ended (or
    # retries a closure that failed or was held, or an update the build missed).
    carried: dict[str, Digest] = dict(written.closed_digests)
    previous = snapshot or {}
    remaining = {i for i in previous if is_part_of(i, production.unbuilt)}
    remaining |= set(plan.frozen)
    if plan.closures is not None:
        remaining |= set(plan.already_ended) | (
            set(plan.closures.identifiers) - set(written.closed_digests)
        )
    carried.update({i: previous[i] for i in remaining if i in previous and i not in carried})
    write_snapshots(
        stores=production.stores,
        source_of=production.source_of,
        digests=production.digests,
        snapshot=snapshot,
        # Held or failed updates keep their previous fingerprint, so tomorrow
        # detects them again, identically.
        keep_previous=set(plan.updates.identifiers) - set(written.updated),
        # Nothing was written for these: leave them out so they are retried.
        skipped=set(plan.creations.identifiers) - set(written.created),
        carried=carried,
        snapshot_by_source=snapshot_by_source,
    )


def write_snapshots(
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


def save_totals(
    totals: TotalsStore,
    previous_totals: Totals | None,
    production: Production,
    plan: ReconciliationPlan,
    outcome: IntegrationOutcome,
) -> None:
    # A frozen source keeps its previous count as the baseline: a collapse is
    # judged against the last volume that was trusted, until it is released.
    counts = dict(production.produced_by_source)
    for name in outcome.frozen_sources:
        before = previous_totals.sources.get(name) if previous_totals else None
        if before is not None:
            counts[name] = before
        else:
            counts.pop(name, None)
    current = Totals(
        dialog_in_prefix=plan.remote_in_prefix + outcome.created - outcome.deleted,
        produced=len(production.regulations),
        sources=counts,
    )
    outcome.total_shift = total_shift(previous_totals, current)
    if outcome.total_shift:
        logger.warning(f"DiaLog total moved without production: {outcome.total_shift}")
    totals.save(current)
