"""`integrations/shared/perimeter.py` — rebuilding DiaLog's competence check upstream."""

import json

import polars as pl
import pytest
from shapely.geometry import box

from integrations.shared.perimeter import (
    SIMPLIFICATION_BY_CODE_TYPE,
    Perimeter,
    discard_outside_perimeter,
)

# A 1° × 1° square: longitude 4 → 5, latitude 45 → 46.
SQUARE = box(4.0, 45.0, 5.0, 46.0)


def line(*points) -> str:
    return json.dumps({"type": "LineString", "coordinates": [list(p) for p in points]})


INSIDE = line((4.2, 45.2), (4.4, 45.4))
CROSSING = line((4.9, 45.5), (5.3, 45.5))  # leaves the square eastwards
OUTSIDE = line((5.5, 45.5), (5.8, 45.5))
TOUCHING = line((5.0, 45.5), (5.5, 45.5))  # starts exactly on the border


@pytest.fixture
def perimeter():
    return Perimeter.build("insee", "test", [SQUARE])


def test_intersects_is_dialog_s_test_touching_counts_as_inside(perimeter):
    assert perimeter.intersects(INSIDE) is True
    assert perimeter.intersects(CROSSING) is True
    assert perimeter.intersects(TOUCHING) is True
    assert perimeter.intersects(OUTSIDE) is False


def test_intersects_says_none_when_it_cannot_read_the_geometry(perimeter):
    assert perimeter.intersects(None) is None
    assert perimeter.intersects("not json") is None
    assert perimeter.intersects(json.dumps({"type": "Nope"})) is None


def test_discard_keeps_what_touches_the_territory_and_drops_the_rest(perimeter):
    df = pl.DataFrame(
        {
            "codetroncon": ["in", "crossing", "outside", "touching"],
            "geometry": [INSIDE, CROSSING, OUTSIDE, TOUCHING],
        }
    )
    kept = discard_outside_perimeter(df, perimeter)
    assert kept.get_column("codetroncon").to_list() == ["in", "crossing", "touching"]


def test_discard_leaves_unreadable_geometries_to_other_rules(perimeter):
    df = pl.DataFrame({"codetroncon": ["null", "bad"], "geometry": [None, "not json"]})
    assert discard_outside_perimeter(df, perimeter).height == 2


def test_discard_reads_the_configured_geometry_column(perimeter):
    df = pl.DataFrame({"g": [INSIDE, OUTSIDE]})
    assert discard_outside_perimeter(df, perimeter, geometry_column="g").height == 1


def test_build_unions_the_communes_and_simplifies_with_dialog_s_tolerance():
    west = box(4.0, 45.0, 4.5, 46.0)
    east = box(4.5, 45.0, 5.0, 46.0)
    # A tiny notch on the border: 0.001° deep, erased at the EPCI tolerance (0.002°) and
    # kept at the commune tolerance (0).
    notch = box(4.999, 45.4, 5.0, 45.6)
    commune = Perimeter.build("insee", "c", [west, east.difference(notch)])
    epci = Perimeter.build("epci", "e", [west, east.difference(notch)])
    probe = line((4.9995, 45.5), (4.9995, 45.5001))  # inside the notch
    inside_the_notch = json.loads(probe)
    from shapely.geometry import shape

    assert not commune.geometry.intersects(shape(inside_the_notch))
    assert epci.geometry.intersects(shape(inside_the_notch))
    assert epci.exact is not None and not epci.exact.intersects(shape(inside_the_notch))


def test_an_emprise_in_the_simplification_s_slack_is_dropped():
    # DiaLog's simplified geometry covers the notch, the exact contour does not: the
    # emprise lies on the neighbour's territory, and the API refused two such segments
    # on 2026-09-18 (Craponne). Both geometries must be touched.
    west = box(4.0, 45.0, 4.5, 46.0)
    east = box(4.5, 45.0, 5.0, 46.0)
    notch = box(4.999, 45.4, 5.0, 45.6)
    epci = Perimeter.build("epci", "e", [west, east.difference(notch)])
    in_the_notch = line((4.9995, 45.5), (4.9995, 45.5001))
    well_inside = line((4.2, 45.5), (4.2, 45.5001))

    assert epci.intersects(in_the_notch) is False
    assert epci.intersects(well_inside) is True
    df = pl.DataFrame({"geometry": [in_the_notch, well_inside, None]})
    assert discard_outside_perimeter(df, epci).height == 2


def test_dialog_s_tolerances_are_the_ones_read_in_its_code():
    assert SIMPLIFICATION_BY_CODE_TYPE == {
        "insee": 0.0,
        "epci": 0.002,
        "departement": 0.001,
        "region": 0.003,
    }


def test_unknown_code_type_is_refused():
    with pytest.raises(ValueError):
        Perimeter.fetch("canton", "1")
