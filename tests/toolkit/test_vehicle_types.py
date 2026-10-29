import polars as pl
import pytest

from toolkit.transforms.vehicle_types import restricted_types_from_limits


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
