"""Period bounds must leave as French local time, with the offset of the day.

Regression guard for the encoding bug measured on staging: `%Y-%m-%dT00:00:00Z`
published every regulation two hours late in summer, one in winter, and ended it
as the last day began.
"""

from datetime import date, datetime

import polars as pl
import pytest

from integrations.local_time import end_of_local_day, start_of_local_day


def naive_frame(*days: date) -> pl.DataFrame:
    """A plain date column, as co_issy-les-moulineaux publishes it."""
    return pl.DataFrame({"day": list(days)})


def aware_frame(*moments: datetime) -> pl.DataFrame:
    """A time-zone-aware column, as co_rennes and dp_sarthe declare it."""
    return pl.DataFrame({"day": list(moments)}).with_columns(
        pl.col("day").cast(pl.Datetime("ms")).dt.replace_time_zone("Europe/Berlin")
    )


def start(df: pl.DataFrame) -> str:
    return df.select(start_of_local_day(df, "day")).item()


def end(df: pl.DataFrame) -> str:
    return df.select(end_of_local_day(df, "day")).item()


@pytest.mark.parametrize(
    "day, expected",
    [
        (date(2026, 9, 1), "2026-09-01T00:00:00+02:00"),
        (date(2026, 1, 15), "2026-01-15T00:00:00+01:00"),
    ],
)
def test_the_day_starts_at_local_midnight(day, expected):
    assert start(naive_frame(day)) == expected


@pytest.mark.parametrize(
    "day, expected",
    [
        (date(2026, 9, 1), "2026-09-01T23:59:59+02:00"),
        (date(2026, 1, 15), "2026-01-15T23:59:59+01:00"),
    ],
)
def test_the_last_day_is_covered_to_its_final_second(day, expected):
    assert end(naive_frame(day)) == expected


def test_the_offset_follows_the_season_rather_than_being_fixed():
    summer = start(naive_frame(date(2026, 9, 1)))
    winter = start(naive_frame(date(2026, 1, 15)))
    assert summer.endswith("+02:00")
    assert winter.endswith("+01:00")


@pytest.mark.parametrize(
    "day, expected_start, expected_end",
    [
        # Spring forward: the day opens on winter time and closes on summer time.
        (date(2026, 3, 29), "2026-03-29T00:00:00+01:00", "2026-03-29T23:59:59+02:00"),
        # Fall back: the reverse.
        (date(2026, 10, 25), "2026-10-25T00:00:00+02:00", "2026-10-25T23:59:59+01:00"),
    ],
)
def test_the_two_transition_days_change_offset_mid_day(day, expected_start, expected_end):
    df = naive_frame(day)
    assert start(df) == expected_start
    assert end(df) == expected_end


def test_an_aware_column_keeps_its_own_calendar_day():
    """Casting an aware column straight to Datetime rebases it on UTC.

    14:30 CEST would become a naive 12:30, which is harmless here — but 00:30 CEST
    would become 22:30 the day before, moving the regulation to the wrong day.
    """
    assert start(aware_frame(datetime(2026, 9, 1, 14, 30))) == "2026-09-01T00:00:00+02:00"
    assert start(aware_frame(datetime(2026, 9, 1, 0, 30))) == "2026-09-01T00:00:00+02:00"
    assert end(aware_frame(datetime(2026, 9, 1, 0, 30))) == "2026-09-01T23:59:59+02:00"


def test_nothing_leaves_labelled_as_utc():
    """The old encoding appended `Z`, which DiaLog reads as a genuine UTC instant."""
    for value in (start(naive_frame(date(2026, 7, 1))), end(naive_frame(date(2026, 7, 1)))):
        assert not value.endswith("Z")
        assert value.endswith("+02:00")
