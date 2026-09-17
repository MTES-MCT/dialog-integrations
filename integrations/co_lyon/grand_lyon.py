"""The Grand Lyon open data platform, shared by every Métropole de Lyon data source.

Every layer is served through one GeoServer WFS endpoint under the `metropole-de-lyon`
namespace; a data source only names its layer.
"""

import polars as pl

from integrations.shared.wfs import BBox, assert_lon_lat_bbox, fetch_wfs_features

WFS_URL = "https://data.grandlyon.com/geoserver/metropole-de-lyon/ows"
NAMESPACE = "metropole-de-lyon"

# Bounding box of the Métropole de Lyon, generous on purpose: it is a sanity check on
# the axis order, not a geographic filter.
BBOX: BBox = (4.4, 45.4, 5.3, 46.0)


def fetch_layer(layer: str) -> pl.DataFrame:
    """One Grand Lyon layer as a Polars frame, coordinates checked to be (lon, lat)."""
    df = fetch_wfs_features(WFS_URL, f"{NAMESPACE}:{layer}")
    assert_lon_lat_bbox(df, BBOX)
    return df
