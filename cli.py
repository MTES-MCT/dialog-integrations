import json
from typing import Annotated

import typer
from loguru import logger

from integrations.base_integration import BaseIntegration
from notifications.notifier import TchapNotifier
from settings import Organization

app = typer.Typer(help="Dialog CLI")


# Shared CLI parameter
def comma_list(raw: str) -> list[str]:
    return raw.split(",")


IdsOption = Annotated[
    list | None, typer.Option(parser=comma_list, help="List of ids to restrict to.")
]
UpdateOption = Annotated[bool | None, typer.Option(help="Update existing regulations")]
EnvOption = Annotated[str, typer.Option(help="Environment: dev or prod")]


@app.command()
def integrate(
    organization: Organization,  # type: ignore[valid-type]
    identifiers: IdsOption = None,
    update_existing: UpdateOption = None,
    env: EnvOption = "dev",
):
    """Sync data for a specific organization to Dialog API."""
    dialog_integration = BaseIntegration.from_organization(organization.name, env=env)
    logger.info(f"Integrating measures for organization: {organization.name} (env: {env})")
    dialog_integration.integrate_regulations(limit_to=identifiers, update_existing=update_existing)


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
    notifier = TchapNotifier()
    if dry_run:
        body, formatted_body = notifier.format_message(results_data)
        typer.echo(body)
        typer.echo("\n--- formatted_body (HTML) ---\n")
        typer.echo(formatted_body)
        return

    notifier.send_notification(results_data)
