"""Vehicle types read from the limits a measure carries."""

import polars as pl

from api.dia_log_client.models import (
    PostApiRegulationsAddBodyMeasuresItemVehicleSetType0RestrictedTypesType0Item as VehicleRestrictedTypeEnum,  # noqa: E501
)

HEAVY_GOODS_VEHICLE = VehicleRestrictedTypeEnum.HEAVYGOODSVEHICLE.value
DIMENSIONS = VehicleRestrictedTypeEnum.DIMENSIONS.value


def restricted_types_from_limits(weight: pl.Expr, *sizes: pl.Expr) -> pl.Expr:
    """`heavyGoodsVehicle` for a weight limit, `dimensions` for a height, width or length
    limit, both for both; null without any limit."""
    has_weight = weight.is_not_null()
    has_size = pl.any_horizontal(size.is_not_null() for size in sizes)
    return (
        pl.when(has_weight & has_size)
        .then(pl.lit([HEAVY_GOODS_VEHICLE, DIMENSIONS]))
        .when(has_weight)
        .then(pl.lit([HEAVY_GOODS_VEHICLE]))
        .when(has_size)
        .then(pl.lit([DIMENSIONS]))
        .otherwise(pl.lit(None, dtype=pl.List(pl.String)))
    )


GAUGE_LETTERS = {
    "vehicle_heavyweight_max_weight": "T",
    "vehicle_max_height": "H",
    "vehicle_max_width": "W",
    "vehicle_max_length": "L",
}


def limit_text(limit: pl.Expr) -> pl.Expr:
    """`3.5`, and `8` rather than `8.0`: a limit as keys and titles write it."""
    return limit.cast(pl.String).str.replace(r"\.0$", "")


def one_measure_per_limit(df: pl.DataFrame) -> pl.DataFrame:
    """One row per limit a row sets, keyed `T7.5`, `H3.5`, `W2.2` or `L8`, with its types.
    A row setting none stays, without a key."""
    df = df.with_columns(
        (pl.col(column) if column in df.columns else pl.lit(None)).cast(pl.Float64).alias(column)
        for column in GAUGE_LETTERS
    )
    parts = [
        df.filter(pl.col(limit).is_not_null()).with_columns(
            *(
                pl.lit(None, dtype=pl.Float64).alias(other)
                for other in GAUGE_LETTERS
                if other != limit
            ),
            measure_group_key=pl.lit(GAUGE_LETTERS[limit]) + limit_text(pl.col(limit)),
        )
        for limit in GAUGE_LETTERS
    ]
    unlimited = df.filter(pl.all_horizontal(pl.col(c).is_null() for c in GAUGE_LETTERS))
    split = pl.concat([*parts, unlimited.with_columns(measure_group_key=pl.lit(None, pl.String))])
    weight, *sizes = (pl.col(column) for column in GAUGE_LETTERS)
    return split.with_columns(vehicle_restricted_types=restricted_types_from_limits(weight, *sizes))
