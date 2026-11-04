"""Geometries as DiaLog takes them: GeoJSON, 2D, longitude first."""

from typing import Literal

import polars as pl
import shapely
from loguru import logger

_READERS = {"wkb": shapely.from_wkb, "wkt": shapely.from_wkt, "geojson": shapely.from_geojson}


def to_geojson(values: pl.Series, encoding: Literal["wkb", "wkt", "geojson"]) -> pl.Series:
    """`values`, already in EPSG:4326, as GeoJSON strings without their Z; null stays null,
    and so does an unreadable value, left for the geometry rule to count."""
    geometries = shapely.force_2d(_READERS[encoding](values.to_numpy(), on_invalid="ignore"))
    result = pl.Series(values.name, shapely.to_geojson(geometries).tolist(), dtype=pl.String)
    if unreadable := result.null_count() - values.null_count():
        logger.warning(f"{unreadable}/{values.len()} geometries unreadable as {encoding}")
    return result
