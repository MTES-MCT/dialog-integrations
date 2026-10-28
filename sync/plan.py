"""Decide what to create, update, close and delete on a given run.

Four operations, three sources of truth (`ai/docs/synchronisation.md`):

| Operation | Source of truth                                                          |
|-----------|--------------------------------------------------------------------------|
| create    | `GET /api/organization/identifiers` — identifier missing from DiaLog      |
| delete    | same endpoint — present in DiaLog, **inside our prefix**, absent today    |
| close     | same as delete, but the regulation stays and its end date is brought back |
| update    | the snapshot of what we sent last time (`sync/snapshot.py`)                  |

An organization chooses what happens to a regulation that left its source: nothing
(the default), deletion, or closure (`sync/closure.py`) — never both. A closed
regulation that has already ended, according to the snapshot, is left alone.

Two guard rails:

- `identifier_prefix` bounds every destructive operation. Without a prefix deletion
  and closure are *refused*, not merely disabled: an organization usually also
  receives regulations from channels outside this repository.
- a batch over its cap is held **whole** and flagged for manual review; its previous
  digest stays in the snapshot so it is detected again, identically, the next day.
  While creations or updates are held, what left the source is held too: they may
  carry its segments, and removing it first would leave them without a limit.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from payloads.regulations import is_part_of
from sync.closure import is_ended
from sync.digest import Digest, fingerprint

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


class MissingPrefixError(SynchronizationError):
    """An organization declares neither an identifier prefix nor that it only adds."""


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
    def over_cap(self) -> bool:
        return self.limit is not None and self.size > self.limit

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
    # Missing, but under the prefix of a frozen source: neither deleted nor closed.
    frozen: tuple[str, ...] = ()

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
    unbuilt: Iterable[str] = (),
    frozen_prefixes: Iterable[str] = (),
) -> ReconciliationPlan:
    """Split today's production into its batches.

    `force_deletions` releases the batch of what left the source — deletions or
    closures, whichever the organization chose — when it is held. What is missing because
    it could not be built (`unbuilt`, slices included) or sits under a frozen prefix is
    not taken for gone, and `--force-deletions` does not release the latter.
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
    unbuilt = set(unbuilt)
    frozen_prefixes = tuple(frozen_prefixes)
    absent = sorted(i for i in in_prefix - set(produced) if not is_part_of(i, unbuilt))
    frozen = tuple(i for i in absent if frozen_prefixes and i.startswith(frozen_prefixes))
    missing = tuple(i for i in absent if not (frozen_prefixes and i.startswith(frozen_prefixes)))
    to_delete = missing if delete_missing else ()

    creations = _batch(CREATE, to_create, max_creations)
    updates = _batch(UPDATE, to_update, max_updates)
    waiting = creations.held or updates.held

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
        closures = _batch(CLOSE, to_close, max_closures, force_deletions, waiting)

    return ReconciliationPlan(
        creations=creations,
        updates=updates,
        # --force-deletions releases the deletion batch only.
        deletions=_batch(DELETE, to_delete, max_deletions, force_deletions, waiting),
        unchanged=unchanged,
        closures=closures,
        closed_at=closed_at if close_missing else None,
        already_ended=already_ended,
        update_mode=update_mode,
        snapshot_present=snapshot is not None,
        identifier_prefix=identifier_prefix,
        remote_total=len(remote),
        remote_in_prefix=len(in_prefix),
        frozen=frozen,
    )


def _batch(
    operation: str,
    identifiers: tuple[str, ...],
    limit: int | None,
    released: bool = False,
    waiting: bool = False,
) -> Batch:
    over_cap = limit is not None and len(identifiers) > limit
    held = bool(identifiers) and (over_cap or waiting) and not released
    return Batch(operation=operation, identifiers=identifiers, limit=limit, held=held)
