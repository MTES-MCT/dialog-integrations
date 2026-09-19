import json
from typing import Annotated
from urllib.parse import urlparse

import typer
from loguru import logger

from integrations.base_integration import BaseIntegration
from notifications.notifier import Notifier
from notifications.summary import render_summary
from notifications.tchap_bot import TchapBot
from settings import Organization

app = typer.Typer(help="Dialog CLI")


def comma_list(raw: str) -> list[str]:
    return raw.split(",")


IdsOption = Annotated[
    list | None, typer.Option(parser=comma_list, help="List of ids to restrict to.")
]
UpdateOption = Annotated[bool | None, typer.Option(help="Update existing regulations")]
EnvOption = Annotated[str, typer.Option(help="Environment: dev or prod")]
DryRunOption = Annotated[
    bool,
    typer.Option("--dry-run", help="Compute everything, write nothing, print the report."),
]
ForceDeletionsOption = Annotated[
    bool,
    typer.Option(
        "--force-deletions",
        help="Release a deletion or closure batch held by its cap.",
    ),
]
JsonOption = Annotated[
    bool,
    typer.Option("--json", help="Print the run result as JSON on stdout (for CI)."),
]
SummaryOption = Annotated[
    str | None,
    typer.Option(
        "--summary",
        help="Append a markdown summary of the run to this file (CI: $GITHUB_STEP_SUMMARY).",
    ),
]


@app.command()
def integrate(
    organization: Organization,  # type: ignore[valid-type]
    identifiers: IdsOption = None,
    update_existing: UpdateOption = None,
    env: EnvOption = "dev",
    dry_run: DryRunOption = False,
    force_deletions: ForceDeletionsOption = False,
    json_output: JsonOption = False,
    summary: SummaryOption = None,
):
    """Sync data for a specific organization to Dialog API."""
    dialog_integration = BaseIntegration.from_organization(organization.name, env=env)
    logger.info(f"Integrating measures for organization: {organization.name} (env: {env})")
    # The host actually written to, so the Tchap report can file the run under its
    # target. Scheme and trailing slash are dropped; a URL without scheme is kept as is.
    base_url = dialog_integration.organization_settings.base_url or ""
    target = urlparse(base_url).hostname or base_url.strip("/")

    # The summary keeps every alert and error the run logs, without reading any log
    # file: a sink collects them while the run lasts.
    records: list = []
    sink = logger.add(lambda m: records.append(m.record), level="WARNING") if summary else None
    try:
        outcome = dialog_integration.integrate_regulations(
            limit_to=identifiers,
            update_existing=update_existing,
            dry_run=dry_run,
            force_deletions=force_deletions,
        )
    except Exception as error:
        if sink is not None:
            logger.remove(sink)
        if summary:
            _append_summary(
                summary, render_summary(organization.name, target, None, records, repr(error))
            )
        if not json_output:
            raise
        # A crashed run still reports its target: without it the Tchap report would
        # file a staging failure under production.
        logger.exception(f"Integration failed for {organization.name}")
        typer.echo(json.dumps({"success": False, "target": target}, ensure_ascii=False))
        raise typer.Exit(code=1)
    if sink is not None:
        logger.remove(sink)
    if summary:
        _append_summary(summary, render_summary(organization.name, target, outcome, records))

    # Logs go to stderr, so `dialog integrate ... --json > result.json` stays clean.
    if json_output:
        result = outcome.to_result()
        result["target"] = target
        typer.echo(json.dumps(result, ensure_ascii=False))
    elif dry_run:
        typer.echo(outcome.report)


def _append_summary(path: str, markdown: str) -> None:
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(markdown)


@app.command()
def publish(
    organization: Organization,  # type: ignore[valid-type]
    env: EnvOption = "dev",
):
    """Publish all measures"""
    dialog_integration = BaseIntegration.from_organization(organization.name, env=env)
    logger.info(f"Publishing measures for organization: {organization.name} (env: {env})")
    dialog_integration.publish_regulations()


@app.command()
def notify(
    results: Annotated[str, typer.Option(help="JSON results from integration step")],
    dry_run: Annotated[bool, typer.Option(help="Render the message without posting it")] = False,
):
    """Notify Tchap with integration results."""
    try:
        results_data = json.loads(results)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON results: {e}")
        raise typer.Exit(code=1)

    logger.info(f"Processing integration results: {results_data}")
    body, formatted_body = Notifier().format_message(results_data)
    if dry_run:
        typer.echo(body)
        typer.echo("\n--- formatted_body (HTML) ---\n")
        typer.echo(formatted_body)
        return

    TchapBot().send(body, formatted_body)
