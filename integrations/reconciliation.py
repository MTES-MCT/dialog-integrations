"""Decide what to create, update and delete on a given run.

Three operations, three sources of truth (`ai/docs/synchronisation.md`):

| Operation | Source of truth                                                          |
|-----------|--------------------------------------------------------------------------|
| create    | `GET /api/organization/identifiers` — identifier missing from DiaLog      |
| delete    | same endpoint — present in DiaLog, **inside our prefix**, absent today    |
| update    | the snapshot of what we sent last time (`integrations/state.py`)          |

Two guard rails:

- `identifier_prefix` bounds every destructive operation. Without a prefix deletion is
  *refused*, not merely disabled: an organization usually also receives regulations from
  channels outside this repository.
- a batch over its cap is held **whole** and flagged for manual review; its previous
  digest stays in the snapshot so it is detected again, identically, the next day.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Literal

from integrations.state import Digest, fingerprint

CREATE = "create"
UPDATE = "update"
DELETE = "delete"

# "none": never update. "changed": update what differs from the snapshot (the class
# attribute `update_changed`). "all": update everything already in DiaLog — what the
# historical `--update-existing` flag did, kept for manual runs.
UpdateMode = Literal["none", "changed", "all"]


class SynchronizationError(Exception):
    """A synchronization guard rail refused to run."""


class DeletionsWithoutPrefixError(SynchronizationError):
    """Deletion was requested without an identifier prefix to bound it."""


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
    update_mode: "UpdateMode" = "none"
    snapshot_present: bool = False
    identifier_prefix: str | None = None
    remote_total: int = 0
    remote_in_prefix: int = 0

    @property
    def batches(self) -> tuple[Batch, Batch, Batch]:
        return (self.creations, self.updates, self.deletions)

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
    # Regulations the API refused, one by one. They do not fail the run: the historical
    # behavior is to log them and carry on, and the CI marks a run failed only when the
    # command itself exits non-zero.
    errors: int = 0
    planned: dict[str, int] = field(default_factory=dict)
    held: dict[str, int] = field(default_factory=dict)
    source: dict[str, int] = field(default_factory=dict)
    report: str = ""

    def to_result(self) -> dict:
        """The JSON payload handed to the CI step and to the Tchap notifier."""
        result: dict = {
            "success": True,
            "created": self.created,
            "updated": self.updated,
            "deleted": self.deleted,
        }
        if self.dry_run:
            result["dry_run"] = True
            result["planned"] = self.planned
        if self.errors:
            result["errors"] = self.errors
        if self.held:
            result["held"] = self.held
        if self.source:
            result["source"] = self.source
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
    max_creations: int | None = None,
    max_updates: int | None = None,
    max_deletions: int | None = None,
    force_deletions: bool = False,
) -> ReconciliationPlan:
    """Split today's production into the three batches."""
    assert_identifiers_in_prefix(produced.keys(), identifier_prefix)

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

    if delete_missing:
        if not identifier_prefix:
            raise DeletionsWithoutPrefixError(
                "Refusing to compute deletions without an identifier_prefix: an "
                "unbounded deletion pass would remove regulations this pipeline "
                "does not own."
            )
        to_delete = tuple(sorted(in_prefix - set(produced)))
    else:
        to_delete = ()

    return ReconciliationPlan(
        creations=_batch(CREATE, to_create, max_creations),
        updates=_batch(UPDATE, to_update, max_updates),
        # --force-deletions releases the deletion batch only.
        deletions=_batch(DELETE, to_delete, max_deletions, released=force_deletions),
        unchanged=unchanged,
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
