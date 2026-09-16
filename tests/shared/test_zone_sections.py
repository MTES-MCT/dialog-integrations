import json

from integrations.shared.zone_sections import length_m, line_pieces, sections_of

# In Lyon, 0.001° of longitude is ~78 m and 0.00003° is ~2.3 m.
LONG = [[4.83, 45.76], [4.831, 45.76]]
SHORT = [[4.84, 45.76], [4.84003, 45.76]]


def multi(*lines):
    return json.dumps({"type": "MultiLineString", "coordinates": list(lines)})


def test_length_is_measured_in_metres():
    (piece,) = line_pieces(json.dumps({"type": "LineString", "coordinates": LONG}))
    assert 70 < length_m(piece) < 90


def test_drops_the_pieces_shorter_than_the_threshold():
    kept = json.loads(sections_of(multi(LONG, SHORT)) or "")
    assert kept["type"] == "MultiLineString"
    assert kept["coordinates"] == [LONG]


def test_returns_none_when_nothing_is_long_enough():
    assert sections_of(multi(SHORT)) is None
    assert sections_of(None) is None
    assert sections_of('{"type":"GeometryCollection","geometries":[]}') is None


def test_reads_lines_out_of_a_geometry_collection():
    collection = json.dumps(
        {
            "type": "GeometryCollection",
            "geometries": [
                {"type": "LineString", "coordinates": LONG},
                {"type": "MultiLineString", "coordinates": [SHORT, LONG]},
                {"type": "Point", "coordinates": [4.83, 45.76]},
            ],
        }
    )
    assert len(line_pieces(collection)) == 3
    assert json.loads(sections_of(collection) or "")["coordinates"] == [LONG, LONG]


# --- parallel roads ----------------------------------------------------------------

from integrations.shared.zone_sections import sections_per_length  # noqa: E402

# A 78 m × 7.8 m rectangle: one road inside gives a ratio near 1, four give near 4.
RECT = json.dumps(
    {
        "type": "Polygon",
        "coordinates": [
            [[4.83, 45.76], [4.831, 45.76], [4.831, 45.76007], [4.83, 45.76007], [4.83, 45.76]]
        ],
    }
)


def test_one_road_in_the_polygon_is_about_one_length():
    assert 0.8 < sections_per_length(RECT, multi(LONG)) < 1.2


def test_several_parallel_roads_raise_the_ratio():
    assert sections_per_length(RECT, multi(LONG, LONG, LONG, LONG)) > 3.5


def test_slivers_do_not_count_in_the_ratio():
    assert sections_per_length(RECT, multi(SHORT, SHORT, SHORT)) == 0


def test_a_missing_polygon_gives_zero():
    assert sections_per_length(None, multi(LONG)) == 0
