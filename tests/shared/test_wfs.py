"""The WFS reader: flattening, the axis-order check, and the request it sends."""

import json
from types import SimpleNamespace

import polars as pl
import pytest

from integrations.shared import wfs
from integrations.shared.wfs import assert_lon_lat_bbox, features_to_dataframe, fetch_wfs_features

LYON = (4.4, 45.4, 5.3, 46.0)


def feature(properties: dict, geometry: dict | None) -> dict:
    return {"type": "Feature", "properties": properties, "geometry": geometry}


POINT = {"type": "Point", "coordinates": [4.85, 45.75]}
POLYGON = {"type": "MultiPolygon", "coordinates": [[[[4.85, 45.75], [4.86, 45.75], [4.85, 45.76]]]]}


def test_features_become_one_row_each_with_the_geometry_as_text():
    df = features_to_dataframe(
        [feature({"gid": 1, "nom": "Rue A"}, POINT), feature({"gid": 2, "nom": None}, None)]
    )

    assert df.columns == ["gid", "nom", "geometry"]
    assert df.get_column("gid").to_list() == [1, 2]
    assert json.loads(df.get_column("geometry")[0]) == POINT
    assert df.get_column("geometry")[1] is None


def test_no_feature_gives_an_empty_frame():
    assert features_to_dataframe([]).height == 0


def test_geometries_inside_the_box_pass_whatever_their_nesting():
    df = features_to_dataframe([feature({}, POINT), feature({}, POLYGON), feature({}, None)])
    assert_lon_lat_bbox(df, LYON)


def test_swapped_axes_are_caught():
    swapped = {"type": "Point", "coordinates": [45.75, 4.85]}
    df = features_to_dataframe([feature({}, swapped)])

    with pytest.raises(ValueError, match="axis order"):
        assert_lon_lat_bbox(df, LYON)


def test_an_empty_frame_passes_the_check():
    assert_lon_lat_bbox(pl.DataFrame({"geometry": []}, schema={"geometry": pl.Utf8}), LYON)


def test_fetch_asks_for_geojson_in_lon_lat_and_flattens_the_answer(monkeypatch):
    captured = {}

    def fake_get(url, params=None, timeout=None):
        captured.update(url=url, params=params, timeout=timeout)
        return SimpleNamespace(
            raise_for_status=lambda: None,
            json=lambda: {"type": "FeatureCollection", "features": [feature({"gid": 7}, POINT)]},
        )

    monkeypatch.setattr(wfs.requests, "get", fake_get)

    df = fetch_wfs_features("https://example.org/ows", "ns:layer")

    assert captured["url"] == "https://example.org/ows"
    assert captured["params"]["typeNames"] == "ns:layer"
    assert captured["params"]["outputFormat"] == "application/json"
    assert captured["params"]["SRSNAME"] == "EPSG:4326"
    assert captured["timeout"] is not None
    assert df.get_column("gid").to_list() == [7]


def test_an_http_error_propagates(monkeypatch):
    def failing():
        raise RuntimeError("503")

    monkeypatch.setattr(
        wfs.requests, "get", lambda *a, **k: SimpleNamespace(raise_for_status=failing)
    )
    with pytest.raises(RuntimeError, match="503"):
        fetch_wfs_features("https://example.org/ows", "ns:layer")
