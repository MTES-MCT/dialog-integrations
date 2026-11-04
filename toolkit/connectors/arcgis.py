"""ArcGIS FeatureServer layers: every feature of a layer, in EPSG:4326.

Pages are read until the count the server announces: stopping at the first short page
truncates silently on a server whose `maxRecordCount` is below the page size asked for.
"""

import geopandas as gpd
import polars as pl
import requests
from loguru import logger

from toolkit.connectors.download import DOWNLOAD_TIMEOUT

PAGE_SIZE = 2000


class IncompleteLayerError(Exception):
    """The layer returned fewer features than it announced."""


def read_layer(query_url: str, where: str = "1=1", page_size: int = PAGE_SIZE) -> pl.DataFrame:
    """Every feature behind `…/FeatureServer/{n}/query`, the geometry as WKT in `geometry`."""
    announced = _count(query_url, where)
    features: list[dict] = []
    while len(features) < announced:
        page = _page(query_url, where, offset=len(features), size=page_size)
        if not page:
            break
        features.extend(page)
        logger.info(f"Downloaded {len(features)}/{announced} features from {query_url}")
    if len(features) < announced:
        # Failing the source freezes its regulations; a shorter layer would close them.
        raise IncompleteLayerError(
            f"{query_url}: {len(features)} features read, {announced} announced"
        )
    if not features:
        return pl.DataFrame()
    gdf = gpd.GeoDataFrame.from_features(features, crs="EPSG:4326")
    gdf["geometry"] = gdf.geometry.to_wkt()
    return pl.from_pandas(gdf)


def _count(query_url: str, where: str) -> int:
    response = requests.get(
        query_url,
        params={"where": where, "returnCountOnly": "true", "f": "json"},
        timeout=DOWNLOAD_TIMEOUT,
    )
    response.raise_for_status()
    return int(response.json()["count"])


def _page(query_url: str, where: str, offset: int, size: int) -> list[dict]:
    response = requests.get(
        query_url,
        params={
            "where": where,
            "outFields": "*",
            "f": "geojson",
            "outSR": "4326",
            "resultOffset": offset,
            "resultRecordCount": size,
        },
        timeout=DOWNLOAD_TIMEOUT,
    )
    response.raise_for_status()
    return response.json().get("features", [])


def coded_values(query_url: str, field: str) -> dict[int | str, str]:
    """The names the layer's domain gives the codes of `field` (a road code and its street,
    say); empty when the field has no coded domain.

    Raises on a field gone from the layer, and on an error, which ArcGIS answers with
    HTTP 200.
    """
    layer_url = query_url.removesuffix("/query")
    response = requests.get(layer_url, params={"f": "json"}, timeout=DOWNLOAD_TIMEOUT)
    response.raise_for_status()
    payload = response.json()
    if "error" in payload:
        raise RuntimeError(f"{layer_url} answered an error: {payload['error']}")
    for described in payload.get("fields", []):
        if described.get("name") == field:
            domain = described.get("domain") or {}
            return {value["code"]: value["name"] for value in domain.get("codedValues", [])}
    raise KeyError(f"{layer_url} has no field {field!r}")
