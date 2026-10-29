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
