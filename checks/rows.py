"""Checks every source's canonical rows must pass before a payload is built.

A regulation with one failing row is left out whole, as DiaLog has it: sending the rest
would drop a measure, taking it for gone would close or delete it. What the API refuses
on its own is left to it.
"""

from __future__ import annotations

import json

import polars as pl

from canonical.regulation_measure import DEFECT_COLUMN
from toolkit.transforms.local_time import parse_instant

END_BEFORE_START = "fin avant début"
SPEED_WITHOUT_VALUE = "limitation sans vitesse"
MISSING_GEOMETRY = "géométrie manquante"
BAD_COORDINATES = "coordonnées hors bornes ou inversées"

# Metropolitan France: a (lat, lon) pair sent in GeoJSON's (lon, lat) order lands in this
# box, where no correctly ordered point of a territory DiaLog serves falls.
LATITUDES = (41.0, 52.0)
LONGITUDES = (-6.0, 10.0)


def failing_rows(df: pl.DataFrame) -> pl.DataFrame:
    """One row per (regulation_identifier, reason) that fails a check; empty when all pass."""
    reasons: list[pl.DataFrame] = []
    if "regulation_identifier" not in df.columns or df.is_empty():
        return pl.DataFrame(schema={"regulation_identifier": pl.Utf8, "reason": pl.Utf8})

    if {"period_start_date", "period_end_date"} <= set(df.columns):
        # An unreadable date is the API's to refuse, not this check's.
        ends_early = []
        for start, end in zip(df["period_start_date"].to_list(), df["period_end_date"].to_list()):
            begins, ends = parse_instant(start), parse_instant(end)
            ends_early.append(begins is not None and ends is not None and ends < begins)
        reasons.append(_flag(df, pl.Series(ends_early), END_BEFORE_START))

    if "measure_type_" in df.columns:
        speed = (
            df["measure_max_speed"]
            if "measure_max_speed" in df.columns
            else pl.Series([None] * df.height)
        )
        mask = (df["measure_type_"] == "speedLimitation") & speed.is_null()
        reasons.append(_flag(df, mask.fill_null(False), SPEED_WITHOUT_VALUE))

    if "location_road_type" in df.columns:
        geometry = (
            df["location_geometry"].cast(pl.Utf8)
            if "location_geometry" in df.columns
            else pl.Series([None] * df.height, dtype=pl.Utf8)
        )
        mask = (df["location_road_type"] == "rawGeoJSON") & (
            geometry.is_null() | (geometry.str.strip_chars() == "")
        )
        reasons.append(_flag(df, mask.fill_null(False), MISSING_GEOMETRY))

    if "location_geometry" in df.columns:
        bad = [_bad_coordinates(value) for value in df["location_geometry"].to_list()]
        reasons.append(_flag(df, pl.Series(bad), BAD_COORDINATES))

    if DEFECT_COLUMN in df.columns:
        reasons.append(
            df.filter(pl.col(DEFECT_COLUMN).is_not_null()).select(
                pl.col("regulation_identifier").cast(pl.Utf8),
                reason=pl.col(DEFECT_COLUMN).cast(pl.Utf8),
            )
        )

    return pl.concat(reasons).unique(maintain_order=True)


def _flag(df: pl.DataFrame, mask: pl.Series, reason: str) -> pl.DataFrame:
    return (
        df.filter(mask)
        .select(pl.col("regulation_identifier").cast(pl.Utf8))
        .with_columns(reason=pl.lit(reason))
    )


def _bad_coordinates(geometry) -> bool:
    if geometry is None:
        return False
    if isinstance(geometry, str):
        try:
            geometry = json.loads(geometry)
        except ValueError:
            return False
    pair = _first_pair(geometry.get("coordinates") if isinstance(geometry, dict) else None)
    if pair is None:
        return False
    x, y = pair
    if not (-180 <= x <= 180 and -90 <= y <= 90):
        return True
    return LATITUDES[0] <= x <= LATITUDES[1] and LONGITUDES[0] <= y <= LONGITUDES[1]


def _first_pair(coordinates) -> tuple[float, float] | None:
    while isinstance(coordinates, list) and coordinates and isinstance(coordinates[0], list):
        coordinates = coordinates[0]
    if (
        isinstance(coordinates, list)
        and len(coordinates) >= 2
        and all(isinstance(c, (int, float)) for c in coordinates[:2])
    ):
        return float(coordinates[0]), float(coordinates[1])
    return None
