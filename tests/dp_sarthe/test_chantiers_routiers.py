"""Tests for Sarthe chantiers_routiers measure mapping."""

import polars as pl

from api.dia_log_client.models import MeasureTypeEnum
from integrations.dp_sarthe.chantiers_routiers.data_source_integration import (
    compute_measure_fields,
)


def _df(*mode_exp: str | None) -> pl.DataFrame:
    return pl.DataFrame(
        {"objectid": list(range(len(mode_exp))), "mode_exp": list(mode_exp)},
        schema={"objectid": pl.Int64, "mode_exp": pl.Utf8},
    )


def test_alternat_variants_map_to_alternate_road():
    df = compute_measure_fields(_df("Alternat", "alternat", "Alternat feux", "Alternat manuel"))

    assert df.height == 4
    assert df["measure_type_"].unique().to_list() == [MeasureTypeEnum.ALTERNATEROAD.value]


def test_road_closures_map_to_no_entry():
    df = compute_measure_fields(_df("Route barrée avec déviation", "Déviation 2 sens"))

    assert df.height == 2
    assert df["measure_type_"].unique().to_list() == [MeasureTypeEnum.NOENTRY.value]


def test_unknown_values_are_dropped_not_forced_to_no_entry():
    """A speed limitation must never be published as a road closure (R-02)."""
    df = compute_measure_fields(
        _df("Limitation de vitesse", "Neutralisation d'une voie", "Chaussée rétrécie", None)
    )

    assert df.height == 0


def test_known_and_unknown_values_mixed():
    df = compute_measure_fields(
        _df("Alternat", "Limitation de vitesse", "Route barrée avec déviation")
    )

    assert df["objectid"].to_list() == [0, 2]
    assert df["measure_type_"].to_list() == [
        MeasureTypeEnum.ALTERNATEROAD.value,
        MeasureTypeEnum.NOENTRY.value,
    ]
