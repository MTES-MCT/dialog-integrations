"""Rules on the location, applied to canonical rows."""

import json

import polars as pl
import pytest

from tests.sync_fixtures import measure_rows
from toolkit.rules.apply import apply_rules
from toolkit.rules.location import (
    COORDINATES_OUT_OF_BOUNDS,
    GEOMETRY_MISSING,
    ONE_DIRECTION,
    POINT_NOT_PARKING,
    one_direction,
    one_way_street_named,
)

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


@pytest.mark.parametrize(
    "direction, left_out", [("BOTH", False), ("A_TO_B", True), ("B_TO_A", True), (None, False)]
)
def test_a_restriction_holding_one_way_only_is_left_out(direction, left_out):
    df = pl.DataFrame({"location_direction": [direction]}, schema={"location_direction": pl.String})

    result = apply_rules(df, [ONE_DIRECTION], canonical=True)

    assert result.frame.is_empty() is left_out


def test_a_source_names_its_own_direction_and_both_ways():
    df = pl.DataFrame({"sens": [0, 1, 2, None]})

    result = apply_rules(df, [one_direction(pl.col("sens"), 0)])

    assert result.frame["sens"].to_list() == [0, None]
    assert result.applied == {"ONE_DIRECTION": 2}


@pytest.mark.parametrize(
    "label, left_out",
    [
        ("Sens unique", True),
        ("Circulation sens unique", True),
        ("Sens interdit / Sens unique", True),
        ("sens interdit (ou sens unique)", True),
        ("Interdit dans les 2 sens", False),
        ("rétablissement double sens", False),
        ("interdiction double sens pour les cyclistes", False),
        (None, False),
    ],
)
def test_a_label_naming_a_one_way_street_is_left_out(label, left_out):
    df = pl.DataFrame({"label": [label]}, schema={"label": pl.String})

    result = apply_rules(df, [one_way_street_named(pl.col("label"))])

    assert result.frame.is_empty() is left_out


@pytest.mark.parametrize(
    "road_type,geometry,left_out",
    [
        ("rawGeoJSON", " ", True),
        ("rawGeoJSON", None, True),
        ("rawGeoJSON", LINE, False),
        ("lane", None, False),
    ],
)
def test_a_raw_geojson_location_needs_a_geometry(road_type, geometry, left_out):
    df = measure_rows(["R0"]).with_columns(
        location_road_type=pl.lit(road_type),
        location_geometry=pl.Series([geometry], dtype=pl.String),
    )
    assert apply_rules(df, [GEOMETRY_MISSING]).frame.is_empty() is left_out


def line(*points) -> str:
    return json.dumps({"type": "LineString", "coordinates": [list(p) for p in points]})


@pytest.mark.parametrize(
    "geometry,left_out",
    [
        (line((-4.48, 48.39), (-4.47, 48.40)), False),
        (line((48.39, -4.48), (48.40, -4.47)), True),
        (line((200.0, 48.0), (201.0, 48.0)), True),
        # La Réunion, correctly ordered, is outside the metropolitan box.
        (line((55.45, -20.88), (55.46, -20.89)), False),
        ("not json", False),
        (None, False),
    ],
)
def test_coordinates_out_of_bounds_or_in_latitude_longitude_order_are_left_out(geometry, left_out):
    df = measure_rows(["R0"]).with_columns(location_geometry=pl.Series([geometry], dtype=pl.String))
    assert apply_rules(df, [COORDINATES_OUT_OF_BOUNDS]).frame.is_empty() is left_out
