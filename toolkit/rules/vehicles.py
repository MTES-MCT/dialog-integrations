"""Rules on the vehicles a measure targets."""

import polars as pl

from toolkit.rules.rule import Nature, Outcome, Rule
from toolkit.transforms.vehicle_types import DIMENSIONS, HEAVY_GOODS_VEHICLE

# Word-bounded: "aire piétonne", which is published, and "cyclomoteurs" or "cyclos", which
# are motor vehicles, do not match.
NAMES_PEDESTRIANS_OR_CYCLES = r"(?i)\b(?:piétons?|cycles?|cyclistes?|cyclables?)\b"
# B9a bars pedestrians, B9b cycles.
PEDESTRIAN_OR_CYCLE_SIGNS = ("B9a", "B9b")


def pedestrians_or_cycles_named(label: pl.Expr) -> Rule:
    """The measure's label, or that of the vehicles it restricts, names pedestrians or cycles.

    A missing label says nothing of who is targeted: the source's own mapping judges it.
    """
    return _pedestrians_or_cycles(label.str.contains(NAMES_PEDESTRIANS_OR_CYCLES).fill_null(False))


def pedestrian_or_cycle_sign(sign: pl.Expr) -> Rule:
    """The road sign bars pedestrians or cycles."""
    return _pedestrians_or_cycles(sign.is_in(PEDESTRIAN_OR_CYCLE_SIGNS).fill_null(False))


def _pedestrians_or_cycles(targeted: pl.Expr) -> Rule:
    # The API has no restricted type for them, nor a measure for a lane kept for them, so
    # they never reach the canonical columns: read in the source's own terms. `other` would
    # describe a restriction on traffic as one on vehicles.
    return Rule(
        code="PEDESTRIANS_OR_CYCLES_TARGETED",
        label="piétons ou cycles visés, ou voie qui leur est réservée",
        nature=Nature.DIALOG_LIMIT,
        outcome=Outcome.DROP,
        valid=~targeted,
    )


# 44 t is the legal maximum of a road vehicle: above it, the weight is a structure's load
# capacity (Lyon, 50 to 230 t), and exceptional transport goes through permits of its own.
HEAVYWEIGHT_ABOVE_LIMIT = Rule(
    code="HEAVYWEIGHT_ABOVE_LIMIT",
    label="tonnage au-delà de 44 t",
    nature=Nature.OUT_OF_SCOPE,
    outcome=Outcome.DROP,
    valid=pl.col("vehicle_heavyweight_max_weight").is_null()
    | (pl.col("vehicle_heavyweight_max_weight") <= 44),
)


_RESTRICTED = pl.col("vehicle_restricted_types")
_SIZES = ("vehicle_max_height", "vehicle_max_width", "vehicle_max_length")


def _positive(column: str) -> pl.Expr:
    # A limit of 0 or less is no limit: sources write 0 for none.
    return (pl.col(column) > 0).fill_null(False)


# A threshold is never invented (no 3.5 t by default): without it, the restriction would
# read as aimed at every heavy goods vehicle, or at nobody.
THRESHOLD_MISSING = Rule(
    code="THRESHOLD_MISSING",
    label="gabarit ou poids lourds sans seuil",
    nature=Nature.DATA_DEFECT,
    outcome=Outcome.DROP,
    valid=~(
        # Not every vehicle, yet none named: a gauge whose limits were all missing.
        (
            ~pl.col("vehicle_all_vehicles").fill_null(True)
            & (_RESTRICTED.list.len().fill_null(0) == 0)
        )
        | (
            _RESTRICTED.list.contains(HEAVY_GOODS_VEHICLE).fill_null(False)
            & ~_positive("vehicle_heavyweight_max_weight")
        )
        | (
            _RESTRICTED.list.contains(DIMENSIONS).fill_null(False)
            & ~pl.any_horizontal(_positive(size) for size in _SIZES)
        )
    ),
)
