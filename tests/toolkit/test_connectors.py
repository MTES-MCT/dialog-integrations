"""The shared readers of producer platforms, on canned answers."""

import io

import polars as pl
import pytest

from toolkit.connectors import arcgis, opendatasoft


class _Response:
    def __init__(self, content: bytes = b"", payload=None):
        self.content = content
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


EXPORT = (
    "https://data.example.fr/api/explore/v2.1/catalog/datasets/limits/exports/csv?delimiter=%3B"
)


def test_an_opendatasoft_export_is_read_and_checked_against_the_announced_count(
    monkeypatch,
):
    calls = []

    def get(url, timeout):
        calls.append(url)
        if "/records" in url:
            return _Response(payload={"total_count": 2})
        return _Response(content=b"a;b\n1;x\n2;y\n")

    monkeypatch.setattr(opendatasoft.requests, "get", get)

    df = opendatasoft.read_export(EXPORT, separator=";")

    assert df.to_dicts() == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
    assert (
        calls[1]
        == "https://data.example.fr/api/explore/v2.1/catalog/datasets/limits/records?limit=0"
    )


def test_a_parquet_export_needs_no_option(monkeypatch):
    buffer = io.BytesIO()
    pl.DataFrame({"a": [1]}).write_parquet(buffer)
    monkeypatch.setattr(
        opendatasoft.requests,
        "get",
        lambda url, timeout: _Response(buffer.getvalue(), {"total_count": 1}),
    )

    assert opendatasoft.read_export(EXPORT.replace("/csv?delimiter=%3B", "/parquet")).height == 1


def test_a_filtered_export_is_not_compared_with_the_whole_dataset():
    assert opendatasoft.records_count_url(EXPORT + "&where=annee%3D2026") is None


def test_a_url_that_is_not_an_export_is_refused():
    with pytest.raises(ValueError):
        opendatasoft.read_export("https://data.example.fr/api/explore/v2.1/catalog/datasets/x")


def _feature(gid):
    return {
        "type": "Feature",
        "properties": {"gid": gid},
        "geometry": {"type": "Point", "coordinates": [-1.55, 47.21]},
    }


def _arcgis_server(monkeypatch, announced: int, cap: int):
    """A layer of `announced` features served at most `cap` per page."""
    pages = []

    def get(url, params, timeout):
        if params.get("returnCountOnly"):
            return _Response(payload={"count": announced})
        offset, size = params["resultOffset"], min(params["resultRecordCount"], cap)
        pages.append(offset)
        features = [_feature(g) for g in range(offset, min(offset + size, announced))]
        return _Response(payload={"type": "FeatureCollection", "features": features})

    monkeypatch.setattr(arcgis.requests, "get", get)
    return pages


def test_every_page_is_read_even_on_a_server_capped_below_the_page_size(monkeypatch):
    pages = _arcgis_server(monkeypatch, announced=2500, cap=1000)

    df = arcgis.read_layer("https://example/FeatureServer/0/query")

    assert df.height == 2500 and df["gid"].n_unique() == 2500
    assert pages == [0, 1000, 2000]
    assert df["geometry"][0].startswith("POINT")


def test_a_layer_shorter_than_announced_fails(monkeypatch):
    def get(url, params, timeout):
        if params.get("returnCountOnly"):
            return _Response(payload={"count": 3})
        return _Response(payload={"features": [_feature(0)] if params["resultOffset"] == 0 else []})

    monkeypatch.setattr(arcgis.requests, "get", get)

    with pytest.raises(arcgis.IncompleteLayerError):
        arcgis.read_layer("https://example/FeatureServer/0/query")


def test_an_empty_layer_is_an_empty_frame(monkeypatch):
    _arcgis_server(monkeypatch, announced=0, cap=1000)

    assert arcgis.read_layer("https://example/FeatureServer/0/query").is_empty()
