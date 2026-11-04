import polars as pl
import pytest

from toolkit.transforms.vehicle_types import one_measure_per_limit, restricted_types_from_limits


@pytest.mark.parametrize(
    "weight, height, length, expected",
    [
        (3.5, None, None, ["heavyGoodsVehicle"]),
        (None, 4.1, None, ["dimensions"]),
        (None, None, 12.0, ["dimensions"]),
        (19.0, 4.1, None, ["heavyGoodsVehicle", "dimensions"]),
        (None, None, None, None),
    ],
)
def test_types_follow_the_limits(weight, height, length, expected):
    df = pl.DataFrame(
        {"weight": [weight], "height": [height], "length": [length]},
        schema={"weight": pl.Float64, "height": pl.Float64, "length": pl.Float64},
    )

    types = df.select(
        restricted_types_from_limits(pl.col("weight"), pl.col("height"), pl.col("length"))
    ).item()

    assert (types.to_list() if types is not None else None) == expected


def test_a_row_of_two_limits_becomes_one_measure_per_limit_and_one_of_none_stays():
    df = pl.DataFrame(
        {
            "vehicle_heavyweight_max_weight": [7.5, None, None],
            "vehicle_max_height": [3.5, None, None],
            "vehicle_max_length": [None, 8.0, None],
        }
    )

    result = one_measure_per_limit(df)

    assert result.select("measure_group_key", "vehicle_restricted_types").rows() == [
        ("T7.5", ["heavyGoodsVehicle"]),
        ("H3.5", ["dimensions"]),
        ("L8", ["dimensions"]),
        (None, None),
    ]
    assert result["vehicle_max_height"].to_list() == [None, 3.5, None, None]


def test_a_limit_given_as_an_integer_splits_like_any_other():
    df = pl.DataFrame({"vehicle_heavyweight_max_weight": [3.5, None], "vehicle_max_length": [8, 8]})

    keys = one_measure_per_limit(df)["measure_group_key"].to_list()

    assert sorted(keys) == ["L8", "L8", "T3.5"]
