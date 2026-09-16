"""Minimal WFS 2.0 reader, for the sources a producer serves through a GeoServer.

Many open data catalogues (the Grand Lyon one among them) expose every layer through a
single OGC endpoint, so one reader covers all of a producer's data sources. Features
come back as GeoJSON and are flattened into one Polars row per feature: the feature
properties, plus a `geometry` column holding the GeoJSON geometry serialized as a
string, so the frame stays a plain table that Pandera can validate.

Coordinates are requested in EPSG:4326. GeoServer honours `SRSNAME` and answers in
longitude/latitude order, which is what DiaLog expects (R-52) — `assert_lon_lat_bbox`
makes that assumption fail loudly instead of silently shipping swapped coordinates.
"""

import json

import polars as pl
import requests
from loguru import logger

# Connect timeout, then read timeout: a large layer can weigh tens of megabytes.
HTTP_TIMEOUT = (10, 600)

BBox = tuple[float, float, float, float]  # min_lon, min_lat, max_lon, max_lat


def fetch_wfs_features(url: str, layer: str, *, srs: str = "EPSG:4326") -> pl.DataFrame:
    """Download one WFS layer as GeoJSON and return it as a Polars DataFrame.

    `layer` is the qualified type name, `namespace:layer` as the GetCapabilities lists
    it. The `geometry` column holds each feature's GeoJSON geometry as a string.
    """
    params = {
        "SERVICE": "WFS",
        "VERSION": "2.0.0",
        "REQUEST": "GetFeature",
        "typeNames": layer,
        "outputFormat": "application/json",
        "SRSNAME": srs,
    }
    logger.info(f"Downloading WFS layer {layer} from {url}")
    response = requests.get(url, params=params, timeout=HTTP_TIMEOUT)
    response.raise_for_status()
    payload = response.json()

    features = payload.get("features", [])
    logger.info(f"WFS layer {layer}: {len(features)} feature(s)")
    return features_to_dataframe(features)


def features_to_dataframe(features: list[dict]) -> pl.DataFrame:
    """Flatten GeoJSON features into one row per feature."""
    rows = []
    for feature in features:
        row = dict(feature.get("properties") or {})
        geometry = feature.get("geometry")
        row["geometry"] = json.dumps(geometry) if geometry else None
        rows.append(row)
    return pl.DataFrame(rows, infer_schema_length=None)


def assert_lon_lat_bbox(df: pl.DataFrame, bbox: BBox) -> None:
    """Fail loudly if the geometries do not sit inside the expected lon/lat box.

    Swapped axes are the single most common geographic defect, and they produce data
    that passes every other check. The box should be generous — it is a sanity check
    on the axis order, not a geographic filter — and sampling a few geometries is
    enough to catch a swap.
    """
    min_lon, min_lat, max_lon, max_lat = bbox
    sample = df.filter(pl.col("geometry").is_not_null()).head(50)
    for raw in sample.get_column("geometry"):
        lon, lat = _first_coordinate(json.loads(raw))
        if not (min_lon <= lon <= max_lon and min_lat <= lat <= max_lat):
            raise ValueError(
                f"Geometry point ({lon}, {lat}) falls outside the expected "
                f"longitude/latitude box {bbox} — check the axis order of the source"
            )


def _first_coordinate(geometry: dict) -> tuple[float, float]:
    coordinates = geometry["coordinates"]
    while isinstance(coordinates[0], list):
        coordinates = coordinates[0]
    return float(coordinates[0]), float(coordinates[1])
