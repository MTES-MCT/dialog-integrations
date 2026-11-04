import polars as pl
import shapely
from pyproj import Transformer

from toolkit.transforms.overlaps import contradicted_geometry, drawn_twice, twin_traces

_TO_LONLAT = Transformer.from_crs(2154, 4326, always_xy=True)


def trace(*points):
    """GeoJSON from Lambert-93 metres."""
    return shapely.to_geojson(shapely.LineString([_TO_LONLAT.transform(x, y) for x, y in points]))


def test_a_trace_drawn_alongside_another_of_its_name_is_a_twin_and_one_touching_it_is_not():
    df = pl.DataFrame(
        {
            "geometry": [
                trace((700000, 6600000), (700300, 6600000)),
                trace((700000, 6600012), (700100, 6600012)),
                trace((700300, 6600000), (700400, 6600000)),
                trace((700000, 6600500), (700100, 6600500)),
                trace((700000, 6600530), (700100, 6600530)),
                trace((700000, 6601000), (700100, 6601000)),
                trace((700000, 6601010), (700100, 6601010)),
                trace((700000, 6602000), (700100, 6602000)),
                trace((700095, 6602010), (700195, 6602010)),
            ],
            "street": ["A", "A", "A", "B", "B", "C", "D", "E", "E"],
        }
    )

    assert twin_traces(df, "geometry", name="street").to_list() == [
        *(True, True, False),
        *(False, False),
        *(False, False),
        *(False, False),
    ]


def test_two_values_over_one_stretch_contradict_each_other_but_a_shared_end_does_not():
    df = pl.DataFrame(
        {
            "geometry": [
                trace((700000, 6600000), (700100, 6600000)),
                trace((700050, 6600001), (700100, 6600001)),
                trace((700100, 6600000), (700200, 6600000)),
                trace((700000, 6600000), (700100, 6600000)),
            ],
            "speed": [30, 50, 50, 30],
            "kind": ["V", "V", "V", "H"],
        }
    )

    result = contradicted_geometry(df, value="speed", geometry="geometry", by="kind")

    assert result.to_list() == [True, True, False, False]


def test_only_the_shorter_trace_of_a_stretch_drawn_twice_is_flagged_within_its_group():
    df = pl.DataFrame(
        {
            "geometry": [
                trace((700050, 6600000), (700200, 6600000)),
                trace((700200, 6600001), (700000, 6600001)),
                trace((700000, 6600500), (700200, 6600500)),
                trace((700000, 6600501), (700200, 6600501)),
                trace((700000, 6601000), (700200, 6601000)),
            ],
            "act": ["A", "A", "A", "B", "A"],
        }
    )

    # At equal length, the later one.
    assert drawn_twice(df, "geometry").to_list() == [True, False, False, True, False]
    assert drawn_twice(df, "geometry", by="act").to_list() == [True, False, False, False, False]


def test_a_missing_value_contradicts_nothing():
    df = pl.DataFrame(
        {
            "geometry": [
                trace((700000, 6600000), (700200, 6600000)),
                trace((700000, 6600001), (700200, 6600001)),
            ],
            "weight": [3.5, None],
        }
    )

    assert contradicted_geometry(df, value="weight", geometry="geometry").to_list() == [
        False,
        False,
    ]


def test_a_stub_continuing_a_stretch_of_its_act_is_no_redraw():
    df = pl.DataFrame(
        {
            "geometry": [
                trace((700000, 6600000), (700100, 6600000)),
                trace((700100, 6600000), (700105, 6600000)),
            ],
            "act": ["A", "A"],
        }
    )

    assert drawn_twice(df, "geometry", by="act").to_list() == [False, False]


def test_a_redraw_not_touching_its_trace_leaves_the_longer_one_alone():
    df = pl.DataFrame(
        {
            "geometry": [
                trace((700000, 6600000), (700200, 6600000)),
                trace((700010, 6600001), (700190, 6600001)),
            ],
            "act": ["A", "A"],
        }
    )

    redrawn = drawn_twice(df, "geometry", by="act")

    assert redrawn.to_list() == [False, True]
    assert twin_traces(df, "geometry").to_list() == [True, True]
    assert twin_traces(df, "geometry", ignore=redrawn).to_list() == [False, False]


def test_rows_without_a_group_belong_to_none():
    df = pl.DataFrame(
        {
            "geometry": [
                trace((700000, 6600000), (700200, 6600000)),
                trace((700010, 6600001), (700190, 6600001)),
                trace((700000, 6600500), (700200, 6600500)),
                trace((700000, 6600512), (700200, 6600512)),
            ],
            "act": [None, None, None, None],
        },
        schema={"geometry": pl.String, "act": pl.String},
    )

    assert drawn_twice(df, "geometry", by="act").to_list() == [False] * 4
    assert twin_traces(df, "geometry", name="act").to_list() == [False] * 4
