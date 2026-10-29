"""Rules on the location: where a regulation applies."""

import json

import polars as pl

from api.dia_log_client.models import DirectionEnum
from toolkit.rules.rule import Nature, Outcome, Rule

# DiaLog draws a parking ban at a point, but nothing else: a speed limit or a closure needs
# a section of road. A named or numbered road carries no geometry: DiaLog draws it.
POINT_NOT_PARKING = Rule(
    code="POINT_NOT_PARKING",
    label="emprise ponctuelle hors stationnement",
    nature=Nature.DIALOG_LIMIT,
    outcome=Outcome.DROP,
    valid=pl.col("location_geometry").is_null()
    | (pl.col("location_geometry").str.json_path_match("$.type") != "Point")
    | (pl.col("measure_type_") == "parkingProhibited"),
)


def one_direction(direction: pl.Expr, both_ways: str | int) -> Rule:
    """The restriction holds one way only: a direction is given, and it is not both ways.

    A missing direction is not judged here: a source reads it as both ways, or leaves the
    row out itself.
    """
    # Production ignores the direction of a numbered road (2026-09-17), and a named street or a
    # raw geometry carries none: a one-way restriction would be published both ways.
    return Rule(
        code="ONE_DIRECTION",
        label="restriction d'un seul sens",
        nature=Nature.DIALOG_LIMIT,
        outcome=Outcome.DROP,
        valid=direction.is_null() | (direction == both_ways),
    )


ONE_DIRECTION = one_direction(pl.col("location_direction"), DirectionEnum.BOTH.value)


# Word-bounded: "Interdit dans les 2 sens" and "double sens" do not match.
NAMES_ONE_WAY_STREET = r"(?i)\bsens\s+(?:unique|interdit)\b"


def one_way_street_named(label: pl.Expr) -> Rule:
    """The measure's label names a one-way street, whether or not a direction comes with it."""
    # DiaLog cannot publish a one-way street yet: given a direction or not, it would come
    # out closed both ways.
    return Rule(
        code="ONE_WAY_STREET",
        label="sens unique ou sens interdit",
        nature=Nature.DIALOG_LIMIT,
        outcome=Outcome.DROP,
        valid=~label.str.contains(NAMES_ONE_WAY_STREET).fill_null(False),
    )


GEOMETRY_MISSING = Rule(
    code="GEOMETRY_MISSING",
    label="géométrie manquante",
    nature=Nature.DATA_DEFECT,
    outcome=Outcome.DROP,
    valid=(pl.col("location_road_type") != "rawGeoJSON").fill_null(True)
    | (pl.col("location_geometry").str.strip_chars() != "").fill_null(False),
)


# Metropolitan France: a (lat, lon) pair sent in GeoJSON's (lon, lat) order lands in this
# box, where no correctly ordered point of a territory DiaLog serves falls.
LATITUDES = (41.0, 52.0)
LONGITUDES = (-6.0, 10.0)


def _coordinates_in_bounds(geometry: str | None) -> bool:
    # Unreadable JSON is the API's to refuse, not this rule's.
    try:
        coordinates = json.loads(geometry or "")["coordinates"]
    except (TypeError, ValueError, KeyError):
        return True
    while isinstance(coordinates, list) and coordinates and isinstance(coordinates[0], list):
        coordinates = coordinates[0]
    if not (
        isinstance(coordinates, list)
        and len(coordinates) >= 2
        and all(isinstance(c, (int, float)) for c in coordinates[:2])
    ):
        return True
    x, y = coordinates[:2]
    if not (-180 <= x <= 180 and -90 <= y <= 90):
        return False
    return not (LATITUDES[0] <= x <= LATITUDES[1] and LONGITUDES[0] <= y <= LONGITUDES[1])


COORDINATES_OUT_OF_BOUNDS = Rule(
    code="COORDINATES_OUT_OF_BOUNDS",
    label="coordonnées hors bornes ou inversées",
    nature=Nature.DATA_DEFECT,
    outcome=Outcome.DROP,
    valid=pl.col("location_geometry").map_elements(
        _coordinates_in_bounds, return_dtype=pl.Boolean, skip_nulls=False
    ),
)
