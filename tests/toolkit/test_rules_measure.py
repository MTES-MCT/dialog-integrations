"""Rules on the measure, applied to canonical rows."""

import polars as pl
import pytest

from tests.sync_fixtures import measure_rows
from toolkit.rules.apply import apply_rules
from toolkit.rules.measure import SPEED_MISSING


@pytest.mark.parametrize(
    "measure_type,speed,left_out",
    [("speedLimitation", None, True), ("speedLimitation", 30, False), ("noEntry", None, False)],
)
def test_a_speed_limitation_needs_a_speed(measure_type, speed, left_out):
    df = measure_rows(["R0"]).with_columns(
        measure_type_=pl.lit(measure_type), measure_max_speed=pl.Series([speed], dtype=pl.Int64)
    )
    assert apply_rules(df, [SPEED_MISSING]).frame.is_empty() is left_out
