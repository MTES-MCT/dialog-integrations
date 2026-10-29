"""Rules on the vehicles a measure targets."""

import polars as pl

from toolkit.rules.rule import Nature, Outcome, Rule

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
