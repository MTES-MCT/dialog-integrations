"""Rules on the location: where a regulation applies."""

import polars as pl

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
