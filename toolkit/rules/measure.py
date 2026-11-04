"""Rules on the measure: what a regulation imposes."""

import polars as pl

from toolkit.rules.rule import Nature, Outcome, Rule

SPEED_MISSING = Rule(
    code="SPEED_MISSING",
    label="limitation sans vitesse",
    nature=Nature.DATA_DEFECT,
    outcome=Outcome.DROP,
    valid=(pl.col("measure_type_") != "speedLimitation").fill_null(True)
    | pl.col("measure_max_speed").is_not_null(),
)


def contradicted_value(contradicted: pl.Expr) -> Rule:
    """Another stretch of the same road sets another value for the same traffic, over the
    flagged one (`overlaps.contradicted_geometry`)."""
    return Rule(
        code="CONTRADICTED_VALUE",
        label="valeurs différentes pour un même sens d'un même tronçon",
        nature=Nature.DATA_DEFECT,
        outcome=Outcome.DROP,
        valid=~contradicted,
    )
