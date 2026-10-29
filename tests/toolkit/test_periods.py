import polars as pl

from toolkit.transforms.periods import permanent_period


def test_a_permanent_period_runs_every_day_without_end():
    row = pl.DataFrame({"start": ["2026-01-01T00:00:00+01:00"]})

    period = row.select(permanent_period(pl.col("start"))).row(0, named=True)

    assert period == {
        "period_start_date": "2026-01-01T00:00:00+01:00",
        "period_end_date": None,
        "period_recurrence_type": "everyDay",
        "period_is_permanent": True,
    }


def test_a_permanent_period_without_start_is_left_undated():
    period = pl.DataFrame({"x": [1]}).select(permanent_period())

    assert period["period_start_date"].dtype == pl.String
    assert period["period_start_date"][0] is None
