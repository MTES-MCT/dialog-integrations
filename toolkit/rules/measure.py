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
