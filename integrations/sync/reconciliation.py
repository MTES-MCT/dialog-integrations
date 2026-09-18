"""Decide what to create, update and delete on a given run.

Four operations, three sources of truth (`ai/docs/synchronisation.md`):

| Operation | Source of truth                                                          |
|-----------|--------------------------------------------------------------------------|
| create    | `GET /api/organization/identifiers` — identifier missing from DiaLog      |
| delete    | same endpoint — present in DiaLog, **inside our prefix**, absent today    |
| close     | same as delete, but the regulation stays and its end date is brought back |
| update    | the snapshot of what we sent last time (`integrations/state.py`)          |

An organization chooses what happens to a regulation that left its source: nothing
(the default), deletion, or closure (`integrations/closure.py`) — never both. A closed
regulation that has already ended, according to the snapshot, is left alone.

Two guard rails:

- `identifier_prefix` bounds every destructive operation. Without a prefix deletion
  and closure are *refused*, not merely disabled: an organization usually also
  receives regulations from channels outside this repository.
- a batch over its cap is held **whole** and flagged for manual review; its previous
  digest stays in the snapshot so it is detected again, identically, the next day.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

from integrations.sync.closure import is_ended
from integrations.sync.state import Digest, fingerprint

CREATE = "create"
UPDATE = "update"
DELETE = "delete"
CLOSE = "close"

# "none": never update. "changed": update what differs from the snapshot (the class
# attribute `update_changed`). "all": update everything already in DiaLog — what the
# historical `--update-existing` flag did, kept for manual runs.
UpdateMode = Literal["none", "changed", "all"]


class SynchronizationError(Exception):
    """A synchronization guard rail refused to run."""


class DeletionsWithoutPrefixError(SynchronizationError):
    """Deletion or closure was requested without an identifier prefix to bound it."""


class ConflictingMissingPoliciesError(SynchronizationError):
    """An organization asked to both delete and close what left its source."""


class IdentifierOutsidePrefixError(SynchronizationError):
    """A produced identifier does not carry the declared prefix."""


@dataclass(frozen=True)
class Batch:
    """A set of identifiers to apply one operation to, and its cap."""

    operation: str
    identifiers: tuple[str, ...] = ()
    limit: int | None = None
    held: bool = False

    @property
    def size(self) -> int:
        return len(self.identifiers)

    @property
    def applicable(self) -> tuple[str, ...]:
        """What actually gets written: nothing at all when the batch is held."""
        return () if self.held else self.identifiers


@dataclass(frozen=True)
class ReconciliationPlan:
    creations: Batch
    updates: Batch
    deletions: Batch
    unchanged: tuple[str, ...] = ()
    # Only for organizations that close what left their source.
    closures: Batch | None = None
    closed_at: datetime | None = None
    # Missing from the source but already ended according to the snapshot: not touched.
    already_ended: tuple[str, ...] = ()
    update_mode: "UpdateMode" = "none"
    snapshot_present: bool = False
    identifier_prefix: str | None = None
    remote_total: int = 0
    remote_in_prefix: int = 0

    @property
    def batches(self) -> tuple[Batch, ...]:
        batches: tuple[Batch, ...] = (self.creations, self.updates, self.deletions)
        if self.closures is not None:
            batches += (self.closures,)
        return batches

    @property
    def held(self) -> dict[str, int]:
        """Held batches, by operation — what needs a human decision."""
        return {batch.operation: batch.size for batch in self.batches if batch.held}

    @property
    def planned(self) -> dict[str, int]:
        return {batch.operation: batch.size for batch in self.batches}


@dataclass
class IntegrationOutcome:
    """What one `dialog integrate` run did, and what it plans to do."""

    organization: str
    dry_run: bool = False
    created: int = 0
    updated: int = 0
    deleted: int = 0
    # None for organizations that do not close what left their source.
    closed: int | None = None
    # Regulations the API refused, one by one. They do not fail the run: the historical
    # behavior is to log them and carry on, and the CI marks a run failed only when the
    # command itself exits non-zero.
    errors: int = 0
    # Regulations the pipeline itself refused (a zone covering parallel roads): not an
    # API failure, retried every day until the source changes.
    refused: int = 0
    planned: dict[str, int] = field(default_factory=dict)
    held: dict[str, int] = field(default_factory=dict)
    source: dict[str, int] = field(default_factory=dict)
    # The corpus, for the Tchap message: rows read from the sources, and the
    # regulations and measures that are in DiaLog after the run — what was produced,
    # minus what could not be created (held, refused, or failed).
    raw_rows: int | None = None
    # One entry per dataset ("permanent", "temporaire"): the regulations and measures
    # in DiaLog after the run, and the restrictions the dataset states versus those the
    # pipeline retained, whose ratio is the share of the dataset retained.
    datasets: list[dict] = field(default_factory=list)
    regulations: int = 0
    measures: int = 0
    report: str = ""

    def to_result(self) -> dict:
        """The JSON payload handed to the CI step and to the Tchap notifier."""
        result: dict = {
            "success": True,
            "created": self.created,
            "updated": self.updated,
            "deleted": self.deleted,
        }
        if self.closed is not None:
            result["closed"] = self.closed
        if self.dry_run:
            result["dry_run"] = True
            result["planned"] = self.planned
        if self.errors:
            result["errors"] = self.errors
        if self.refused:
            result["refused"] = self.refused
        if self.held:
            result["held"] = self.held
        if self.source:
            result["source"] = self.source
        result["integrated"] = {"regulations": self.regulations, "measures": self.measures}
        if self.raw_rows is not None:
            result["integrated"]["rows"] = self.raw_rows
        if self.datasets:
            result["datasets"] = self.datasets
        return result


def assert_identifiers_in_prefix(identifiers: Iterable[str], prefix: str | None) -> None:
    """Refuse to write anything when a produced identifier misses the prefix.

    Catches the two mistakes that would otherwise be silent: a prefix applied twice,
    and a prefix forgotten in one branch of the transformation. Both would make the
    deletion pass blind to regulations we own.
    """
    if not prefix:
        return
    offenders = sorted({str(i) for i in identifiers if not str(i).startswith(prefix)})
    if offenders:
        raise IdentifierOutsidePrefixError(
            f"{len(offenders)} identifier(s) do not start with the declared prefix "
            f"{prefix!r}: {offenders[:5]}"
        )


def reconcile(
    produced: Mapping[str, Digest],
    remote_identifiers: Iterable[str],
    snapshot: Mapping[str, Digest] | None,
    *,
    identifier_prefix: str | None = None,
    update_mode: UpdateMode = "none",
    delete_missing: bool = False,
    close_missing: bool = False,
    closed_at: datetime | None = None,
    max_creations: int | None = None,
    max_updates: int | None = None,
    max_deletions: int | None = None,
    max_closures: int | None = None,
    force_deletions: bool = False,
) -> ReconciliationPlan:
    """Split today's production into its batches.

    `force_deletions` releases the batch of what left the source — deletions or
    closures, whichever the organization chose — when its cap holds it.
    """
    assert_identifiers_in_prefix(produced.keys(), identifier_prefix)
    if delete_missing and close_missing:
        raise ConflictingMissingPoliciesError(
            "delete_missing and close_missing are both set: a regulation that left the "
            "source is either deleted or closed, not both."
        )

    remote = {str(identifier) for identifier in remote_identifiers}
    in_prefix = (
        {identifier for identifier in remote if identifier.startswith(identifier_prefix)}
        if identifier_prefix
        else remote
    )

    to_create = tuple(identifier for identifier in produced if identifier not in remote)
    already_there = tuple(identifier for identifier in produced if identifier in remote)

    if update_mode == "all":
        to_update = already_there
    elif update_mode == "changed" and snapshot is not None:
        to_update = tuple(
            identifier
            for identifier in already_there
            # Absent from the snapshot means "we do not know what we sent": no update,
            # the snapshot is rebuilt from today's digest instead.
            if identifier in snapshot
            and fingerprint(snapshot[identifier]) != fingerprint(produced[identifier])
        )
    else:
        to_update = ()

    updated_set = set(to_update)
    unchanged = tuple(identifier for identifier in already_there if identifier not in updated_set)

    if (delete_missing or close_missing) and not identifier_prefix:
        raise DeletionsWithoutPrefixError(
            "Refusing to compute deletions or closures without an identifier_prefix: "
            "an unbounded pass would touch regulations this pipeline does not own."
        )
    missing = tuple(sorted(in_prefix - set(produced)))
    to_delete = missing if delete_missing else ()

    closures = None
    already_ended: tuple[str, ...] = ()
    if close_missing:
        if closed_at is None:
            raise SynchronizationError("close_missing needs the closing instant")
        already_ended = tuple(
            identifier
            for identifier in missing
            if snapshot is not None
            and identifier in snapshot
            and is_ended(snapshot[identifier], closed_at)
        )
        ended = set(already_ended)
        to_close = tuple(identifier for identifier in missing if identifier not in ended)
        closures = _batch(CLOSE, to_close, max_closures, released=force_deletions)

    return ReconciliationPlan(
        creations=_batch(CREATE, to_create, max_creations),
        updates=_batch(UPDATE, to_update, max_updates),
        # --force-deletions releases the deletion batch only.
        deletions=_batch(DELETE, to_delete, max_deletions, released=force_deletions),
        unchanged=unchanged,
        closures=closures,
        closed_at=closed_at if close_missing else None,
        already_ended=already_ended,
        update_mode=update_mode,
        snapshot_present=snapshot is not None,
        identifier_prefix=identifier_prefix,
        remote_total=len(remote),
        remote_in_prefix=len(in_prefix),
    )


def _batch(
    operation: str,
    identifiers: tuple[str, ...],
    limit: int | None,
    released: bool = False,
) -> Batch:
    held = bool(limit is not None and len(identifiers) > limit and not released)
    return Batch(operation=operation, identifiers=identifiers, limit=limit, held=held)
