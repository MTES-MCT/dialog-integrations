"""The checks every source's clean rows go through, and what the run does with a failure."""

import json

import polars as pl

from integrations import checks
from integrations.base_data_source_integration import unknown_pivot_columns
from tests.sync_fixtures import measure_rows


def _reasons(frame: pl.DataFrame) -> dict[str, str]:
    failing = checks.failing_rows(frame)
    return dict(zip(failing["regulation_identifier"], failing["reason"]))


def test_rows_from_the_shared_fixture_pass():
    assert checks.failing_rows(measure_rows(["A", "B"])).is_empty()


def test_an_end_before_the_start_fails_across_formats_and_offsets():
    frame = measure_rows(["early", "fine", "offset"]).with_columns(
        period_start_date=pl.Series(["2026-02-01", "2026-01-01", "2026-03-01T10:00:00+02:00"]),
        period_end_date=pl.Series(["2026-01-01", "2026-02-01", "2026-03-01T09:30:00+01:00"]),
    )

    assert _reasons(frame) == {"early": checks.END_BEFORE_START}


def test_a_speed_limitation_needs_a_speed():
    frame = measure_rows(["none", "fine"]).with_columns(
        measure_max_speed=pl.Series([None, 30], dtype=pl.Int64)
    )

    assert _reasons(frame) == {"none": checks.SPEED_WITHOUT_VALUE}


def test_a_raw_geojson_location_needs_a_geometry():
    frame = measure_rows(["empty", "fine"]).with_columns(
        location_geometry=pl.Series([" ", measure_rows(["x"])["location_geometry"][0]])
    )

    assert _reasons(frame) == {"empty": checks.MISSING_GEOMETRY}


def test_coordinates_out_of_bounds_or_in_latitude_longitude_order_fail():
    def line(*points):
        return json.dumps({"type": "LineString", "coordinates": [list(p) for p in points]})

    frame = measure_rows(["brest", "swapped", "outside", "reunion"]).with_columns(
        location_geometry=pl.Series(
            [
                line((-4.48, 48.39), (-4.47, 48.40)),
                line((48.39, -4.48), (48.40, -4.47)),
                line((200.0, 48.0), (201.0, 48.0)),
                line((55.45, -20.88), (55.46, -20.89)),
            ]
        )
    )

    assert _reasons(frame) == {
        "swapped": checks.BAD_COORDINATES,
        "outside": checks.BAD_COORDINATES,
    }


def test_a_column_named_like_the_pivot_but_missing_from_it_is_reported():
    assert unknown_pivot_columns(
        ["regulation_identifier", "vehicle_critair_types", "locaton_label", "location_lable"]
    ) == ["location_lable", "vehicle_critair_types"]
