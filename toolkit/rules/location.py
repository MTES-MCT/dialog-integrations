"""Rules on the location: where a regulation applies."""

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
    """The measure's label names a one-way street, which it says without its direction."""
    return Rule(
        code="ONE_WAY_STREET",
        label="sens unique ou sens interdit",
        nature=Nature.DIALOG_LIMIT,
        outcome=Outcome.DROP,
        valid=~label.str.contains(NAMES_ONE_WAY_STREET).fill_null(False),
    )
