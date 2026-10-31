"""The writes to DiaLog: create, update, close, delete, and the accounting of what failed."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

from loguru import logger

from api.dia_log_client.models import PostApiRegulationsAddBody
from dialog.failures import split_failures
from dialog.zone_flow import create_zone_regulation, has_zone
from sync.closure import ClosureNotRebuildable, close_payload, save_payload_from_read
from sync.dating import date_creation, date_update, has_undated_period, run_day
from sync.digest import Digest, compute_regulation_digest
from sync.outcome import IntegrationOutcome
from sync.plan import CLOSE, DELETE, ReconciliationPlan

if TYPE_CHECKING:
    from run.organization import BaseIntegration

# A zone regulation is updated by DELETE then POST: when the POST fails, it is gone.
LOST_ON_UPDATE = "arrêté supprimé, nouvelle version non créée"


@dataclass
class Written:
    created: list[str]
    updated: list[str]
    deleted: list[str]
    closed_digests: dict[str, Digest]


def apply(
    integration: BaseIntegration,
    plan: ReconciliationPlan,
    regulations: dict[str, PostApiRegulationsAddBody],
    outcome: IntegrationOutcome,
    closed_at: datetime,
    force_deletions: bool,
) -> Written:
    created, refused = integration._integrate_regulations_add(
        [regulations[identifier] for identifier in plan.creations.applicable]
    )
    updated, lost = integration._integrate_regulations_update(
        [regulations[identifier] for identifier in plan.updates.applicable]
    )
    write_failures = getattr(integration.api, "write_failures", {})
    unwritten = (set(plan.creations.applicable) - set(created) - set(refused)) | (
        set(plan.updates.applicable) - set(updated)
    )
    outages = [i for i in unwritten if not getattr(write_failures.get(i), "is_rejection", False)]
    deletions = plan.deletions.applicable
    closures = plan.closures.applicable if plan.closures is not None else ()
    if outages and not force_deletions:
        # What was not written may have replaced what left the source. A refusal will
        # be refused again tomorrow and does not wait; an outage does.
        logger.warning(
            f"{len(outages)} creations or updates got no answer: "
            "deletions and closures wait for the next run"
        )
        waiting = {DELETE: len(deletions), CLOSE: len(closures)}
        outcome.held |= {operation: n for operation, n in waiting.items() if n}
        deletions = closures = ()
    deleted = delete_all(integration, deletions)
    closed_digests: dict[str, Digest] = {}
    if plan.closures is not None:
        closed_digests, unclosable = close_all(integration, closures, closed_at)
        outcome.closed = len(closed_digests)
        # Left open, it would stay in force in DiaLog although it left the source.
        deleted += delete_all(integration, unclosable)

    outcome.created = len(created)
    outcome.updated = len(updated)
    outcome.deleted = len(deleted)
    outcome.refused += len(refused)
    failed = (
        (unwritten - set(lost))
        | (set(deletions) - set(deleted))
        | (set(closures) - set(closed_digests) - set(deleted))
    )
    outcome.rejections, causes = split_failures(sorted(failed), write_failures)
    outcome.error_causes |= causes
    if lost:
        outcome.error_causes[LOST_ON_UPDATE] = len(lost)
    return Written(created, updated, deleted, closed_digests)


def create_all(
    integration: BaseIntegration, regulations: list[PostApiRegulationsAddBody]
) -> tuple[list[str], list[str]]:
    """Create every regulation; return (created, refused by the pipeline itself)."""
    created: list[str] = []
    refused: list[str] = []
    day = run_day()
    for index, regulation in enumerate(regulations):
        identifier = str(regulation.identifier)
        logger.info(f"Creating regulation {index + 1}/{len(regulations)}: {identifier}")
        logger.info(f"Contains {len(regulation.measures)} measures.")  # type: ignore
        # An undated permanent period starts the day it is published.
        date_creation(regulation, day)
        outcome = integration._create_regulation(regulation)
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


def create_one(integration: BaseIntegration, regulation: PostApiRegulationsAddBody) -> str:
    """One regulation, through the zone flow when it applies."""
    if integration.resolve_zones_to_sections and has_zone(regulation):
        return create_zone_regulation(
            integration.api,
            regulation,
            min_length_m=integration.min_section_length_m,
            max_sections_per_length=integration.max_sections_per_length,
        )
    return "created" if integration.api.add(regulation) else "failed"


def update_all(
    integration: BaseIntegration, regulations: list[PostApiRegulationsAddBody]
) -> tuple[list[str], list[str]]:
    """Replace every regulation; return (updated, deleted without their new version).

    PUT, not DELETE then POST, which would lose the regulation when the POST fails. A
    zone regulation is the exception: the API answers 500 to its PUT, so it is deleted
    and recreated through the zone flow.
    """
    updated: list[str] = []
    lost: list[str] = []
    day = run_day()
    for index, regulation in enumerate(regulations):
        identifier = str(regulation.identifier)
        logger.info(f"Updating regulation {index + 1}/{len(regulations)}: {identifier}")
        logger.info(f"Contains {len(regulation.measures)} measures.")  # type: ignore

        # A PUT replaces the whole regulation: an undated period must keep the date DiaLog
        # holds, or it restarts today. Unreadable, it waits for tomorrow.
        if has_undated_period(regulation):
            read = integration.api.get(identifier)
            if read is None:
                logger.error(
                    f"Cannot update {identifier}: its current dates could not be read back"
                )
                continue
            date_update(regulation, read, day)

        if integration.resolve_zones_to_sections and has_zone(regulation):
            if not integration.api.delete(identifier):
                continue
            if integration._create_regulation(regulation) == "created":
                updated.append(identifier)
            else:
                logger.error(f"{identifier} deleted for its update, new version not created")
                lost.append(identifier)
        elif integration.api.update(regulation):
            updated.append(identifier)

    logger.success(f"Finished updating {len(updated)}/{len(regulations)} regulations successfully")
    return updated, lost


def close_all(
    integration: BaseIntegration, identifiers: Sequence[str], closed_at: datetime
) -> tuple[dict[str, Digest], list[str]]:
    """Bring the end date of every identifier back to `closed_at`, keeping it in DiaLog.

    The source no longer has it, so it is read back from DiaLog and rebuilt. Returns the
    digest of every regulation now closed, for the snapshot: tomorrow skips it unread; and
    those the read cannot rebuild faithfully, to delete instead.
    """
    closed: dict[str, Digest] = {}
    unclosable: list[str] = []
    for index, identifier in enumerate(identifiers):
        logger.info(f"Closing regulation {index + 1}/{len(identifiers)}: {identifier}")
        read = integration.api.get(identifier)
        if read is None:
            continue
        try:
            closed_payload, changed = close_payload(save_payload_from_read(read), closed_at)
            regulation = PostApiRegulationsAddBody.from_dict(closed_payload)
            digest = compute_regulation_digest(regulation)
        except ClosureNotRebuildable as e:
            logger.warning(f"Cannot close {identifier}, it would lose a detail: {e}; deleting it")
            unclosable.append(identifier)
            continue
        except Exception as e:
            # A read the rebuild does not expect: this one waits, the others go on.
            logger.error(f"Cannot close {identifier}, unexpected read: {e!r}")
            continue
        if not changed:
            logger.info(f"{identifier} had already ended, nothing to write")
            closed[identifier] = digest
            continue
        if not integration.api.update(regulation):
            continue
        logger.success(
            f"{identifier} closed: its periods now end on "
            f"{closed_at.isoformat(timespec='seconds')} at the latest"
        )
        closed[identifier] = digest

    if identifiers:
        logger.success(f"Finished closing {len(closed)}/{len(identifiers)} regulations")
    return closed, unclosable


def delete_all(integration: BaseIntegration, identifiers: Sequence[str]) -> list[str]:
    """DELETE every identifier, already checked against the prefix; return those gone.

    An identifier already absent (404) counts as gone: it leaves the snapshot.
    """
    deleted: list[str] = []
    for index, identifier in enumerate(identifiers):
        logger.info(f"Deleting regulation {index + 1}/{len(identifiers)}: {identifier}")
        if integration.api.delete(str(identifier), missing_is_gone=True):
            deleted.append(str(identifier))

    if identifiers:
        logger.success(
            f"Finished deleting {len(deleted)}/{len(identifiers)} regulations successfully"
        )
    return deleted
