"""Rules on the location, applied to canonical rows."""

import json

import polars as pl
import pytest

from tests.sync_fixtures import measure_rows
from toolkit.rules.apply import apply_rules
from toolkit.rules.location import POINT_NOT_PARKING

POINT = json.dumps({"type": "Point", "coordinates": [2.26, 48.83]})
LINE = json.dumps({"type": "LineString", "coordinates": [[2.26, 48.83], [2.27, 48.84]]})


def located(geometry: str | None, measure_type: str) -> pl.DataFrame:
    return measure_rows(["R0"]).with_columns(
        location_geometry=pl.Series([geometry], dtype=pl.String), measure_type_=pl.lit(measure_type)
    )


@pytest.mark.parametrize(
    "geometry,measure_type,left_out",
    [
        (POINT, "speedLimitation", True),
        (POINT, "noEntry", True),
        (POINT, "parkingProhibited", False),
        (LINE, "speedLimitation", False),
        # A named road or a numbered one carries no geometry: DiaLog draws it.
        (None, "speedLimitation", False),
    ],
)
def test_only_a_parking_ban_may_sit_at_a_point(geometry, measure_type, left_out):
    result = apply_rules(located(geometry, measure_type), [POINT_NOT_PARKING])
    assert result.frame.is_empty() is left_out
