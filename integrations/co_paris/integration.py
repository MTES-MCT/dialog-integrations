import copy

import polars as pl
from loguru import logger

from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from dialog.client import build_client
from run.organization import BaseIntegration
from sync.outcome import FAILED, IntegrationOutcome

from .eudonet.data_source_integration import PUBLISH_POLICE_PREFECTURE
from .eudonet.data_source_integration import DataSourceIntegration as Eudonet
from .eudonet.decision import Decision
from .eudonet.regulations import CITY, IDENTIFIER_PREFIX, PREFECTURE

# The police prefecture's acts go to its own DiaLog organization, under its own client
# (`PARIS_PREFECTURE_CLIENT_ID`, `PARIS_PREFECTURE_CLIENT_SECRET`); its state is kept under
# this name.
PREFECTURE_ORGANIZATION = "co_paris_prefecture"
PREFECTURE_SUFFIX = " (Préfecture de Police)"
PREFECTURE_LABEL = f"{Eudonet.name}{PREFECTURE_SUFFIX}"


class Integration(BaseIntegration):
    status = PostApiRegulationsAddBodyStatus.PUBLISHED

    data_sources = [Eudonet]

    identifier_prefix = IDENTIFIER_PREFIX
    update_changed = True
    close_missing = True

    max_closures_per_run = 50
    max_updates_per_run = 300
    max_creations_per_run = None

    def integrate_regulations(
        self,
        limit_to=[],
        update_existing: bool | None = None,
        dry_run: bool = False,
        force_deletions: bool = False,
    ) -> IntegrationOutcome:
        """The city's acts under this organization, the prefecture's under its own: one Eudonet
        read, one decision, two synchronizations. The city's alone without the prefecture's
        credentials, or with `PUBLISH_POLICE_PREFECTURE` off."""
        run = BaseIntegration.integrate_regulations
        shared = SharedRun()
        city = self.sibling(self.organization_settings, self.client, shared.view(CITY))
        outcome = run(city, limit_to, update_existing, dry_run, force_deletions)
        if not PUBLISH_POLICE_PREFECTURE:
            return outcome
        prefecture = self.prefecture(shared)
        if prefecture is None:
            logger.warning(
                "No credentials for the police prefecture's organization: its acts are left "
                "out, rather than published under the city's name"
            )
            return outcome
        try:
            other = run(prefecture, limit_to, update_existing, dry_run, force_deletions)
        except Exception as error:
            # The city's run went through: the prefecture's acts stay as DiaLog has them.
            logger.exception("The police prefecture's synchronization failed")
            outcome.frozen_sources[PREFECTURE_LABEL] = FAILED
            outcome.source_errors[PREFECTURE_LABEL] = repr(error)[:300]
            return outcome
        return combine(outcome, other)

    def prefecture(self, shared: "SharedRun") -> "Integration | None":
        settings = self.organization_settings
        if not (settings.pref_client_id and settings.pref_client_secret):
            return None
        own = copy.copy(settings)
        own.organization = PREFECTURE_ORGANIZATION
        own.client_id, own.client_secret = settings.pref_client_id, settings.pref_client_secret
        logger.info(f"DiaLog target for {PREFECTURE_ORGANIZATION}: {own.base_url}")
        return self.sibling(own, build_client(own), shared.view(PREFECTURE, PREFECTURE_LABEL))

    def sibling(self, settings, client, source: type[Eudonet]) -> "Integration":
        """This organization's synchronization options, on one view of the shared run."""
        sibling = type(self)(settings, client)
        sibling.data_sources = [source]
        sibling.paused_sources = self.paused_sources
        return sibling


class SharedRun:
    """One Eudonet read and one decision for both organizations: Eudonet refuses a second
    session, and the acts are judged together, the citations crossing authorities."""

    def __init__(self) -> None:
        self.raw: pl.DataFrame | None = None
        self.elsewhere: dict[str, str] | None = None
        # A failed read is not tried again for the other organization.
        self.error: Exception | None = None
        self.decided: pl.DataFrame | None = None
        self.decision: Decision | None = None

    def view(self, authority: str, label: str | None = None) -> type[Eudonet]:
        shared = self

        class View(Eudonet):
            def fetch_raw_data(self) -> pl.DataFrame:
                if shared.error is not None:
                    raise shared.error
                if shared.raw is None:
                    try:
                        shared.raw = super().fetch_raw_data()
                    except Exception as error:
                        shared.error = error
                        raise
                    shared.elsewhere = self.numbers_published_elsewhere
                self.numbers_published_elsewhere = shared.elsewhere
                return shared.raw

            def decided_rows(self, raw_data: pl.DataFrame) -> pl.DataFrame:
                if shared.decided is None:
                    shared.decided = super().decided_rows(raw_data)
                    shared.decision = self.decision
                self.decision = shared.decision
                return shared.decided

        View.authority = authority
        if label:
            View.report_label = label  # type: ignore[attr-defined]
        View.__name__ = Eudonet.__name__
        return View


def combine(city: IntegrationOutcome, prefecture: IntegrationOutcome) -> IntegrationOutcome:
    """One report for the organization: counts added up, the prefecture's apart."""
    out = copy.copy(city)
    for name in ("created", "updated", "deleted", "rejections", "refused"):
        setattr(out, name, getattr(city, name) + getattr(prefecture, name))
    for name in ("regulations", "measures", "produced"):
        setattr(out, name, getattr(city, name) + getattr(prefecture, name))
    if city.closed is not None or prefecture.closed is not None:
        out.closed = (city.closed or 0) + (prefecture.closed or 0)
    for name in ("error_causes", "planned", "held"):
        merged = dict(getattr(city, name))
        for key, count in getattr(prefecture, name).items():
            merged[key] = merged.get(key, 0) + count
        setattr(out, name, merged)
    # The same read and the same texts: its alerts and metrics are the city's.
    out.source_alerts = list(dict.fromkeys([*city.source_alerts, *prefecture.source_alerts]))
    out.datasets = [*city.datasets, *prefecture.datasets]
    out.total_shift = city.total_shift or prefecture.total_shift
    out.frozen_sources = {**city.frozen_sources, **_apart(prefecture.frozen_sources)}
    out.volume_drops = {**city.volume_drops, **_apart(prefecture.volume_drops)}
    out.source_errors = {**city.source_errors, **_apart(prefecture.source_errors)}
    out.report = f"{city.report}\n\n{PREFECTURE_LABEL}\n{prefecture.report}"
    return out


def _apart(by_source: dict) -> dict:
    return {f"{name}{PREFECTURE_SUFFIX}": value for name, value in by_source.items()}
