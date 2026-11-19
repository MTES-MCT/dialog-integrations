"""The walking-pace limit that comes with a pedestrian area."""

import polars as pl
from loguru import logger

from api.dia_log_client.models import MeasureTypeEnum

PEDESTRIAN_AREA_SPEED = 5


def add_pedestrian_area_speed(
    df: pl.DataFrame,
    is_ban: pl.Expr,
    twin_group_key: pl.Expr,
    speed: pl.Expr = pl.lit(PEDESTRIAN_AREA_SPEED),
) -> pl.DataFrame:
    """Give every pedestrian-area ban a walking-pace twin, in the same regulation.

    The Code de la route (R. 110-2) defines the area by both (team decision, 2026-09-14).
    A copy of the ban, it inherits its filters; no exemption, as the vehicles let in still
    drive at walking pace. `twin_group_key` keeps it a measure of its own. `speed`: where the
    act sets another limit for its area.
    """
    bans = df.filter(is_ban)
    if not bans.height:
        return df
    speeds = bans.with_columns(
        pl.lit(MeasureTypeEnum.SPEEDLIMITATION.value).alias("measure_type_"),
        speed.cast(df.schema["measure_max_speed"]).alias("measure_max_speed"),
        twin_group_key.alias("measure_group_key"),
        pl.lit(None, dtype=pl.List(pl.String)).alias("vehicle_exempted_types"),
        pl.lit(None, dtype=pl.String).alias("vehicle_other_exempted_type_text"),
    )
    counts = speeds["measure_max_speed"].value_counts(sort=True).iter_rows()
    logger.info(
        f"{speeds.height} pedestrian-area locations get their speed twin: "
        + ", ".join(f"{count} at {kmh} km/h" for kmh, count in counts)
    )
    return pl.concat([df, speeds], how="vertical")
