import polars as pl
import pytest

from toolkit.rules.apply import apply_rules
from toolkit.rules.vehicles import (
    HEAVYWEIGHT_ABOVE_LIMIT,
    THRESHOLD_MISSING,
    pedestrian_or_cycle_sign,
    pedestrians_or_cycles_named,
)


@pytest.mark.parametrize(
    "label, left_out",
    [
        ("Interdit aux cycles", True),
        ("Interdit aux piétons", True),
        ("Feux piéton", True),
        ("interdiction double sens pour les cyclistes", True),
        ("Piste / Bande / Trottoir cyclable", True),
        ("des  véhicules de transports en commun et cycles", True),
        # Published: a pedestrian area is a ban on vehicles.
        ("aire piétonne", False),
        ("Zone à priorité piétonne", False),
        # Motor vehicles.
        ("Interdit aux cyclomoteurs", False),
        ("Interdit aux véhicules à moteur sauf cyclos", False),
        ("aux véhicules de plus de 3,5 T", False),
        (None, False),
    ],
)
def test_a_label_naming_pedestrians_or_cycles_is_left_out(label, left_out):
    df = pl.DataFrame({"label": [label]}, schema={"label": pl.String})

    result = apply_rules(df, [pedestrians_or_cycles_named(pl.col("label"))])

    assert result.frame.is_empty() is left_out


def test_signs_barring_pedestrians_or_cycles_are_left_out():
    df = pl.DataFrame({"sign": ["B9a", "B9b", "B13", None]})

    result = apply_rules(df, [pedestrian_or_cycle_sign(pl.col("sign"))])

    assert result.frame["sign"].to_list() == ["B13", None]
    assert result.applied == {"PEDESTRIANS_OR_CYCLES_TARGETED": 2}


@pytest.mark.parametrize(
    "weight, left_out", [(44.0, False), (44.5, True), (230.0, True), (None, False)]
)
def test_a_tonnage_above_the_legal_maximum_is_left_out(weight, left_out):
    df = pl.DataFrame(
        {"vehicle_heavyweight_max_weight": [weight]},
        schema={"vehicle_heavyweight_max_weight": pl.Float64},
    )

    result = apply_rules(df, [HEAVYWEIGHT_ABOVE_LIMIT], canonical=True)

    assert result.frame.is_empty() is left_out


@pytest.mark.parametrize(
    "all_vehicles, types, weight, height, left_out",
    [
        (True, None, None, None, False),
        (False, ["heavyGoodsVehicle"], 3.5, None, False),
        (False, ["dimensions"], None, 4.1, False),
        (False, ["hazardousMaterials"], None, None, False),
        (False, ["heavyGoodsVehicle"], None, None, True),
        (False, ["heavyGoodsVehicle"], 0.0, None, True),
        (False, ["dimensions"], None, None, True),
        (False, ["heavyGoodsVehicle", "dimensions"], 7.5, None, True),
        # Not every vehicle, yet none named.
        (False, None, None, None, True),
        (False, [], None, None, True),
    ],
)
def test_a_gauge_or_heavy_goods_limit_needs_its_threshold(
    all_vehicles, types, weight, height, left_out
):
    df = pl.DataFrame(
        {
            "vehicle_all_vehicles": [all_vehicles],
            "vehicle_restricted_types": [types],
            "vehicle_heavyweight_max_weight": [weight],
            "vehicle_max_height": [height],
        },
        schema={
            "vehicle_all_vehicles": pl.Boolean,
            "vehicle_restricted_types": pl.List(pl.String),
            "vehicle_heavyweight_max_weight": pl.Float64,
            "vehicle_max_height": pl.Float64,
        },
    )

    result = apply_rules(df, [THRESHOLD_MISSING], canonical=True)

    assert result.frame.is_empty() is left_out
