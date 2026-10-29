import polars as pl
import pytest

from toolkit.rules.apply import apply_rules
from toolkit.rules.vehicles import pedestrian_or_cycle_sign, pedestrians_or_cycles_named


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
