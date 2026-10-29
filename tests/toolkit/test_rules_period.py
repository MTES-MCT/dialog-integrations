"""Rules on the period, applied to canonical rows."""

from datetime import date

import polars as pl
import pytest

from tests.sync_fixtures import measure_rows
from toolkit.rules import period
from toolkit.rules.apply import apply_rules
from toolkit.rules.period import EXPIRED

TODAY = date(2026, 1, 2)


@pytest.fixture(autouse=True)
def frozen_today(monkeypatch):
    monkeypatch.setattr(period, "today_in_paris", lambda: TODAY)


def ending(*ends: str | None, category: str = "temporaryRegulation") -> pl.DataFrame:
    identifiers = [f"R{i}" for i in range(len(ends))]
    return measure_rows(identifiers).with_columns(
        period_end_date=pl.Series(list(ends), dtype=pl.String),
        regulation_category=pl.lit(category),
    )


def kept(df: pl.DataFrame) -> list[str]:
    return apply_rules(df, [EXPIRED]).frame["regulation_identifier"].to_list()


def test_a_temporary_regulation_that_ended_yesterday_is_left_out():
    df = ending("2026-01-01T23:59:59+01:00", "2026-01-02T23:59:59+01:00")
    assert kept(df) == ["R1"]


def test_the_end_day_is_read_on_the_paris_calendar():
    """23:30 UTC on the 1st is half past midnight on the 2nd in Paris: still today."""
    assert kept(ending("2026-01-01T23:30:00Z")) == ["R0"]


def test_a_permanent_regulation_is_never_expired():
    assert kept(ending("2025-06-01T23:59:59+02:00", category="permanentRegulation")) == ["R0"]


def test_a_temporary_regulation_without_an_end_is_kept_and_counted_unread():
    result = apply_rules(ending(None), [EXPIRED])
    assert result.frame.height == 1
    assert result.unreadable == {"EXPIRED": 1}


def test_today_is_read_at_each_run_not_at_import(monkeypatch):
    df = ending("2026-01-02T23:59:59+01:00")
    assert kept(df) == ["R0"]
    monkeypatch.setattr(period, "today_in_paris", lambda: date(2026, 1, 3))
    assert kept(df) == []
