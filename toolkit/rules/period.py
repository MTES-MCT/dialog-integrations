"""Rules on the period: when a regulation applies."""

import polars as pl

from toolkit.rules.rule import Nature, Outcome, Rule
from toolkit.transforms.local_time import today_in_paris

TEMPORARY = pl.col("regulation_category") == "temporaryRegulation"


def _ended_before_today() -> pl.Expr:
    # On the Paris calendar: a regulation ending today is in force until midnight.
    return TEMPORARY & (pl.col("period_end_date").dt.date() < today_in_paris())


EXPIRED = Rule(
    code="EXPIRED",
    label="arrêté temporaire terminé",
    nature=Nature.OUT_OF_SCOPE,
    outcome=Outcome.DROP,
    applies=_ended_before_today,
)

# Without an end nothing would ever close it; without a start the write date would stand in.
DATES_MISSING = Rule(
    code="DATES_MISSING",
    label="arrêté temporaire sans date de début ou de fin",
    nature=Nature.DATA_DEFECT,
    outcome=Outcome.DROP,
    applies=TEMPORARY
    & (pl.col("period_start_date").is_null() | pl.col("period_end_date").is_null()),
)
