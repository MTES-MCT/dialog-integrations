"""Minimal WFS 2.0 reader, shared by integrations served through a GeoServer.

The Grand Lyon catalogue exposes every layer through the same OGC endpoint, so a
single reader covers all of its data sources. Features come back as GeoJSON and are
flattened into one Polars row per feature: the feature properties, plus a `geometry`
column holding the GeoJSON geometry serialized as a string.

Coordinates are requested in EPSG:4326. GeoServer honours `SRSNAME` and answers in
longitude/latitude order, which is what DiaLog expects — `assert_lon_lat_bbox` makes
that assumption fail loudly instead of silently shipping swapped coordinates.
"""

import json

import polars as pl
import requests
from loguru import logger

# Connect timeout, then read timeout: the largest Grand Lyon layer is ~65 MB.
HTTP_TIMEOUT = (10, 600)

GRAND_LYON_WFS_URL = "https://data.grandlyon.com/geoserver/metropole-de-lyon/ows"
GRAND_LYON_NAMESPACE = "metropole-de-lyon"

# Bounding box of the Métropole de Lyon, generous on purpose: it is a sanity check on
# the axis order, not a geographic filter.
LYON_BBOX = (4.4, 45.4, 5.3, 46.0)


def fetch_wfs_features(
    layer: str,
    *,
    url: str = GRAND_LYON_WFS_URL,
    namespace: str = GRAND_LYON_NAMESPACE,
    srs: str = "EPSG:4326",
) -> pl.DataFrame:
    """Download one WFS layer as GeoJSON and return it as a Polars DataFrame.

    The `geometry` column holds a GeoJSON geometry serialized as a string, so the
    frame stays a plain table that Pandera can validate.
    """
    params = {
        "SERVICE": "WFS",
        "VERSION": "2.0.0",
        "REQUEST": "GetFeature",
        "typeNames": f"{namespace}:{layer}",
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


def assert_lon_lat_bbox(df: pl.DataFrame, bbox: tuple[float, float, float, float]) -> None:
    """Fail loudly if the geometries do not sit inside the expected lon/lat box.

    Swapped axes are the single most common geographic defect, and they produce data
    that passes every other check. Sampling a few geometries is enough to catch it.
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
