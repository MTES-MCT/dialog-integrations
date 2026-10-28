"""Which sources are frozen tonight, and the prefixes where nothing is closed or deleted."""

from loguru import logger

from sync.outcome import VOLUME, IntegrationOutcome
from sync.totals import Totals, volume_drops


def freeze_collapsed_sources(
    outcome: IntegrationOutcome,
    previous_totals: Totals | None,
    produced_by_source: dict[str, int],
    force_deletions: bool,
) -> None:
    drops = volume_drops(previous_totals, produced_by_source)
    if drops:
        for name, (before, today) in drops.items():
            logger.warning(
                f"Data source {name} produced {today} regulations against {before} on the "
                "previous run"
                + (": released by --force-deletions" if force_deletions else ": frozen")
            )
        outcome.volume_drops = drops
        if not force_deletions:
            outcome.frozen_sources |= {name: VOLUME for name in drops}


def frozen_prefixes(frozen_sources: dict[str, str], prefix_of: dict[str, str | None]) -> list[str]:
    """The prefixes inside which nothing is closed or deleted this run.

    A source without its own prefix freezes the organization's, and with it every
    other source: the pipeline cannot tell their regulations apart.
    """
    prefixes = {prefix_of[name] for name in frozen_sources}
    return sorted(p for p in prefixes if p)
