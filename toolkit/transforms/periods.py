"""Period columns shared by the sources."""

import polars as pl

from api.dia_log_client.models import PeriodRecurrenceTypeEnum


def permanent_period(start_date: pl.Expr | None = None) -> list[pl.Expr]:
    """A permanent period: every day, no end.

    Without a start date, `sync/dating.py` dates it when DiaLog is written; the run day
    would make every regulation look modified each morning.
    """
    start = pl.lit(None, dtype=pl.String) if start_date is None else start_date
    return [
        start.alias("period_start_date"),
        pl.lit(None).alias("period_end_date"),
        pl.lit(PeriodRecurrenceTypeEnum.EVERYDAY.value).alias("period_recurrence_type"),
        pl.lit(True).alias("period_is_permanent"),
    ]
