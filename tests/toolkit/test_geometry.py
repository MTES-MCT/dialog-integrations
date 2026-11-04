import json

import polars as pl
import shapely

from toolkit.transforms.geometry import to_geojson


def test_a_geometry_goes_out_as_flat_geojson_and_a_missing_one_stays_missing():
    line = shapely.LineString([(6.98, 43.55, 12.0), (6.97, 43.54, 13.0)])
    values = pl.Series("geo_shape", [None, shapely.to_wkb(line)], dtype=pl.Binary)

    result = to_geojson(values, encoding="wkb")

    assert json.loads(result[1]) == {
        "type": "LineString",
        "coordinates": [[6.98, 43.55], [6.97, 43.54]],
    }
    assert result[0] is None


def test_an_unreadable_geometry_becomes_missing_without_failing_the_others():
    values = pl.Series("geometry", ["LINESTRING (1 2", "LINESTRING (1 2, 3 4)", None])

    result = to_geojson(values, encoding="wkt")

    assert result[0] is None and result[2] is None
    assert json.loads(result[1])["coordinates"] == [[1.0, 2.0], [3.0, 4.0]]
