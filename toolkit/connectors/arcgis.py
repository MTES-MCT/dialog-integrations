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
