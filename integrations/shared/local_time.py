"""The one date format the DiaLog API reads correctly: ISO 8601, French local time,
carrying the real offset of that day.

    2026-09-01T00:00:00+02:00    summer
    2026-01-15T00:00:00+01:00    winter

Anything else is read as UTC and shifts the regulation by one or two hours : a `Z`
suffix, a fixed offset, or no offset at all.
"""

import polars as pl

PARIS = "Europe/Paris"
ISO_WITH_OFFSET = "%Y-%m-%dT%H:%M:%S%:z"


def _to_paris(df: pl.DataFrame, column: str) -> pl.Expr:
    """The column as an instant in Paris, whatever shape the producer used.

    A bare date or a naive datetime is read as French local time.
    """
    dtype = df.schema[column]
    value = pl.col(column)
    if dtype == pl.String:
        # ISO 8601 text, possibly with mixed offsets across rows.
        return value.str.to_datetime(time_zone="UTC").dt.convert_time_zone(PARIS)
    if isinstance(dtype, pl.Datetime) and dtype.time_zone is not None:
        # Casting an aware column would rebase it on UTC and move midnight.
        return value.dt.convert_time_zone(PARIS)
    return value.cast(pl.Datetime("us")).dt.replace_time_zone(PARIS)


def _naive_local_midnight(df: pl.DataFrame, column: str) -> pl.Expr:
    """Midnight of the column's day, on the French calendar, still without a zone.

    Truncating before attaching the zone is what keeps the two DST transition days
    right: 2026-03-29 then opens at `+01:00` and closes at `+02:00`.
    """
    return _to_paris(df, column).dt.replace_time_zone(None).dt.truncate("1d")


def start_of_local_day(df: pl.DataFrame, column: str) -> pl.Expr:
    """First instant of the day: `2026-09-01T00:00:00+02:00`."""
    return (
        _naive_local_midnight(df, column).dt.replace_time_zone(PARIS).dt.strftime(ISO_WITH_OFFSET)
    )


def end_of_local_day(df: pl.DataFrame, column: str) -> pl.Expr:
    """Last second of the day: `2026-09-01T23:59:59+02:00`.

    Closing on midnight instead would end the regulation as its final day begins.
    """
    return (
        (_naive_local_midnight(df, column) + pl.duration(hours=23, minutes=59, seconds=59))
        .dt.replace_time_zone(PARIS)
        .dt.strftime(ISO_WITH_OFFSET)
    )


def local_instant(df: pl.DataFrame, column: str) -> pl.Expr:
    """The column's own time of day, re-expressed in Paris. Nothing truncated."""
    return _to_paris(df, column).dt.strftime(ISO_WITH_OFFSET)


def from_epoch_ms(column: str) -> pl.Expr:
    """Epoch milliseconds, re-expressed in Paris.

    An epoch is an instant, so it is anchored on UTC rather than read as local time.
    """
    return (
        pl.from_epoch(column, time_unit="ms")
        .dt.replace_time_zone("UTC")
        .dt.convert_time_zone(PARIS)
        .dt.strftime(ISO_WITH_OFFSET)
    )
