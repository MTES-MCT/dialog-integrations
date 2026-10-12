"""The `dialog` organization: what the back end imported by itself, moved here.

Today, the Litteralis import (`app:litteralis:import` and
`app:litteralis:import-communication` in the back end), ported line for line so that
both can run side by side and be compared. It does not follow this repository's
data-source pipeline: its rules are the back end's, bugs included, and its
synchronization is the back end's too — delete what it owns, then recreate everything.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

from loguru import logger

from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.api import split_failures
from integrations.base_integration import BaseIntegration
from integrations.dialog.litteralis.config import LitteralisConfig
from integrations.dialog.litteralis.executor import LitteralisRun, transform_all, write
from integrations.dialog.litteralis.period_parser import LitteralisPeriodParser
from integrations.dialog.litteralis.transformer import LitteralisTransformer
from integrations.sync.reconciliation import IntegrationOutcome

CLIENT_TIMEZONE = ZoneInfo("Europe/Paris")


class Integration(BaseIntegration):
    # The back end publishes every regulation it imports.
    status = PostApiRegulationsAddBodyStatus.PUBLISHED
    data_sources = []

    def integrate_regulations(
        self,
        limit_to=[],
        update_existing: bool | None = None,
        dry_run: bool = False,
        force_deletions: bool = False,
    ) -> IntegrationOutcome:
        """One run of the back end's two Litteralis commands, standard flow first.

        As in PHP, an organization that fails is reported and the next one runs.
        `--update-existing` and `--force-deletions` have no meaning here: every
        regulation is deleted and recreated, without cap.
        """
        if update_existing or force_deletions:
            logger.warning("Litteralis: --update-existing and --force-deletions are ignored")

        config = LitteralisConfig.from_settings(self.organization_settings)
        outcome = IntegrationOutcome(organization=self.organization, dry_run=dry_run)
        runs: list[LitteralisRun] = []
        failures: list[str] = []
        lines = []

        clock: dict[str, datetime] = {}
        for flow, name in config.runs():
            # PHP reads the clock once per command, before its first organization.
            later_than = clock.setdefault(flow, datetime.now(CLIENT_TIMEZONE))
            transformer = LitteralisTransformer(LitteralisPeriodParser())
            api = config.api(name, self.api, self.organization_settings.base_url or "")
            try:
                run = transform_all(name, flow, config.client(name, flow), transformer, later_than)
                write(run, api, later_than, limit_to=limit_to or None, dry_run=dry_run)
            except Exception as error:
                logger.exception(f"Litteralis {name} ({flow}): import failed")
                failures.append(name)
                lines.append(f"❌ {name} ({flow}) : {error}")
                continue
            runs.append(run)
            rejections, outages = split_failures(run.failed, api.write_failures)
            outcome.rejections += rejections
            for cause, count in outages.items():
                outcome.error_causes[cause] = outcome.error_causes.get(cause, 0) + count
            lines.append(_summary_line(run, dry_run))

        outcome.created = sum(len(run.created) for run in runs)
        outcome.deleted = sum(len(run.deleted) for run in runs)
        outcome.refused = sum(len(run.refused) for run in runs)
        outcome.produced = sum(len(run.commands) for run in runs)
        outcome.regulations = outcome.created
        outcome.measures = sum(
            len(command.measures)
            for run in runs
            for command in run.commands
            if command.general_info.identifier in run.created
        )
        outcome.planned = {
            "deletions": sum(len(run.cleanup) for run in runs),
            "creations": outcome.produced,
        }
        if failures:
            outcome.error_causes["import Litteralis en échec"] = len(failures)
        outcome.report = "\n".join(["Import Litteralis", *lines])
        logger.info(outcome.report)
        return outcome


def _summary_line(run: LitteralisRun, dry_run: bool) -> str:
    if dry_run:
        return (
            f"{run.name} ({run.flow}) : {len(run.commands)} arrêtés à créer, "
            f"{len(run.cleanup)} à supprimer avant (simulation, rien d'écrit)"
        )
    line = (
        f"✅ {run.name} ({run.flow}) : {len(run.created)} créés, {len(run.deleted)} supprimés "
        f"avant, {len(run.failed)} en échec, {len(run.refused)} écartés (URL du document), "
        f"{run.documents_attached} documents attachés"
    )
    if run.documents_missing:
        line += f", {len(run.documents_missing)} créés sans leur document"
    return line
