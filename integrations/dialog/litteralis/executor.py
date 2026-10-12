"""Port of the back end's `LitteralisExecutor` / `LitteralisCommunicationExecutor`, over
the DiaLog API instead of the database.

PHP, for one organization, inside one database transaction:

1. delete the organization's published `litteralis` regulations that are permanent or
   whose last period ends after the start of the run;
2. extract and transform every regulation, and handle each import command: create the
   regulation, its measures, its document, then publish it. A command refused by
   validation is reported and skipped; any other failure rolls everything back.

Here, step 2's transformations run first, before anything is written, so that a
failure PHP would have rolled back (a fatal error in the data) leaves DiaLog untouched.
Then come the deletions, then the creations. What the API cannot reproduce:

- no transaction: a run interrupted after its deletions leaves the organization
  without the regulations it was about to recreate;
- the API records `source = api`, not `litteralis`: the regulations this import owns
  are recognised by the `#` of their identifier (`{collectiviteagenceid}#{arretesrcid}`);
- the competence check runs against the organization of the API key, not the one
  PHP is configured with;
- an empty document URL is stored by PHP and refused by the API: the regulation is
  created without it.
"""

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone

from loguru import logger

from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.api import DialogApi
from integrations.dialog.litteralis import reporter as rec
from integrations.dialog.litteralis.client import LitteralisClient
from integrations.dialog.litteralis.commands import ImportCommand
from integrations.dialog.litteralis.extractor import extract_communication, extract_standard
from integrations.dialog.litteralis.payloads import (
    STORAGE_TITLE,
    regulation_body,
    storage_verdict,
)
from integrations.dialog.litteralis.reporter import Reporter
from integrations.dialog.litteralis.transformer import LitteralisTransformer

STANDARD = "standard"
COMMUNICATION = "communication"
EXTRACTORS: dict[str, Callable] = {STANDARD: extract_standard, COMMUNICATION: extract_communication}

PERMANENT_REGULATION = "permanentRegulation"
PUBLISHED = "published"


@dataclass
class TransformedRegulation:
    identifier: str
    features: list[dict]
    command: ImportCommand | None


@dataclass
class LitteralisRun:
    """One organization through one flow: what was read, built, deleted and created."""

    name: str
    flow: str
    reporter: Reporter
    regulations: list[TransformedRegulation] = field(default_factory=list)
    cleanup: list[str] = field(default_factory=list)
    deleted: list[str] = field(default_factory=list)
    created: list[str] = field(default_factory=list)
    # Not sent: the document URL would have failed the whole regulation in PHP.
    refused: list[str] = field(default_factory=list)
    # Sent, and refused by the API or left without an answer.
    failed: list[str] = field(default_factory=list)
    documents_attached: int = 0
    documents_missing: list[str] = field(default_factory=list)

    @property
    def commands(self) -> list[ImportCommand]:
        return [r.command for r in self.regulations if r.command is not None]


def transform_all(
    name: str,
    flow: str,
    client: LitteralisClient,
    transformer: LitteralisTransformer,
    later_than: datetime,
) -> LitteralisRun:
    """Extract and transform, without writing anything.

    Raises `PhpFatalError` (or an HTTP error) wherever PHP would abort the import.
    """
    reporter = Reporter()
    run = LitteralisRun(name=name, flow=flow, reporter=reporter)
    features_by_regulation = EXTRACTORS[flow](client, later_than, reporter)

    for identifier, features in features_by_regulation.items():
        command = transformer.transform(reporter, identifier, features)
        run.regulations.append(TransformedRegulation(identifier, features, command))
        reporter.acknowledge_new_errors()

    return run


def is_owned(identifier: str) -> bool:
    """Whether a regulation of the organization is one this import manages."""
    return "#" in identifier


def select_cleanup(api: DialogApi, later_than: datetime, limit_to: list[str] | None) -> list[str]:
    """What PHP deletes before importing (`findRegulationOrdersForLitteralisCleanUp`).

    Published, and either permanent or with a period ending at or after `later_than`
    (the latest end of all its periods; a regulation without any end is kept).
    """
    selected = []
    for identifier in api.identifiers():
        if not is_owned(identifier) or (limit_to and identifier not in limit_to):
            continue
        regulation = api.get(identifier)
        if regulation is None:
            raise RuntimeError(f"Cannot read {identifier}: cleanup aborted, nothing written")
        if _to_clean_up(regulation, later_than):
            selected.append(identifier)
    return selected


def _to_clean_up(regulation: dict, later_than: datetime) -> bool:
    if regulation.get("status") != PUBLISHED:
        return False
    if regulation.get("category") == PERMANENT_REGULATION:
        return True
    ends = [
        _aware(datetime.fromisoformat(period["endDateTime"]))
        for measure in regulation.get("measures") or []
        for period in measure.get("periods") or []
        if period.get("endDateTime")
    ]
    return bool(ends) and max(ends) >= later_than


def _aware(moment: datetime) -> datetime:
    return moment if moment.tzinfo is not None else moment.replace(tzinfo=timezone.utc)


def write(
    run: LitteralisRun,
    api: DialogApi,
    later_than: datetime,
    limit_to: list[str] | None = None,
    dry_run: bool = False,
) -> None:
    """Delete, then create, as the PHP transaction does in that order.

    A failed deletion stops the run before any creation: PHP would have rolled back.
    """
    run.cleanup = select_cleanup(api, later_than, limit_to)
    logger.info(f"Litteralis {run.name} ({run.flow}): {len(run.cleanup)} regulations to clean up")
    if dry_run:
        return

    for identifier in run.cleanup:
        if not api.delete(identifier):
            raise RuntimeError(f"Cleanup failed on {identifier}: import stopped before creating")
        run.deleted.append(identifier)

    reporter = run.reporter
    imported_features = 0

    for regulation in run.regulations:
        command = regulation.command
        if command is None or (limit_to and regulation.identifier not in limit_to):
            continue

        verdict = storage_verdict(command)
        if verdict == "invalid":
            # PHP validates the document with the regulation: both fail together.
            _import_failed(reporter, regulation, "url: Cette valeur n'est pas une URL valide.")
            run.refused.append(regulation.identifier)
            continue

        body = regulation_body(command, PostApiRegulationsAddBodyStatus.PUBLISHED)
        if not api.add(body):
            failure = api.write_failures.get(regulation.identifier)
            _import_failed(reporter, regulation, failure.cause if failure else "sans réponse")
            run.failed.append(regulation.identifier)
            continue

        run.created.append(regulation.identifier)
        imported_features += len(regulation.features)

        if verdict == "attach":
            url = command.download_url or ""
            if api.attach_url(regulation.identifier, url, STORAGE_TITLE):
                run.documents_attached += 1
            else:
                run.documents_missing.append(regulation.identifier)
        elif verdict == "empty":
            logger.warning(
                f"{regulation.identifier}: empty document URL, stored by PHP, refused by the API"
            )
            run.documents_missing.append(regulation.identifier)

    reporter.add_count(
        rec.COUNT_IMPORTED_FEATURES, imported_features, {"regulationsCount": len(run.created)}
    )


def _import_failed(reporter: Reporter, regulation: TransformedRegulation, message: str) -> None:
    properties = regulation.features[0]["properties"]
    reporter.add_error(
        rec.ERROR_IMPORT_COMMAND_FAILED,
        {
            rec.ATTR_REGULATION_ID: properties.get("arretesrcid"),
            rec.ATTR_URL: properties.get("shorturl"),
            rec.ATTR_DETAILS: {"message": message},
        },
    )
    reporter.acknowledge_new_errors()
