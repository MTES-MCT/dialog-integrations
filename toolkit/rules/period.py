"""Rules on the period: when a regulation applies."""

import polars as pl

from toolkit.rules.rule import Nature, Outcome, Rule
from toolkit.transforms.local_time import today_in_paris

NOT_TEMPORARY = pl.col("regulation_category") != "temporaryRegulation"


def _not_ended() -> pl.Expr:
    # On the Paris calendar: a regulation ending today is in force until midnight.
    return NOT_TEMPORARY | (pl.col("period_end_date").dt.date() >= today_in_paris())


EXPIRED = Rule(
    code="EXPIRED",
    label="arrêté temporaire terminé",
    nature=Nature.OUT_OF_SCOPE,
    outcome=Outcome.DROP,
    valid=_not_ended,
)

# Without an end nothing would ever close it; without a start the write date would stand in.
DATES_MISSING = Rule(
    code="DATES_MISSING",
    label="arrêté temporaire sans date de début ou de fin",
    nature=Nature.DATA_DEFECT,
    outcome=Outcome.DROP,
    valid=NOT_TEMPORARY
    | (pl.col("period_start_date").is_not_null() & pl.col("period_end_date").is_not_null()),
)

# Multi-year "temporary" restrictions have degraded DiaLog's feed before, but some are real,
# and a 31 December end may be a default: a case-by-case call for whoever reads the log.
LONG_DURATION = Rule(
    code="LONG_DURATION",
    label="arrêté temporaire de plus d'un an",
    nature=Nature.SUSPICIOUS,
    outcome=Outcome.FLAG,
    # In calendar days: instants on either side of a clock change are an hour apart.
    valid=NOT_TEMPORARY
    | (
        (
            pl.col("period_end_date").dt.date() - pl.col("period_start_date").dt.date()
        ).dt.total_days()
        <= 365
    ),
)
