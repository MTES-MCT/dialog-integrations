"""Named-street locations (`roadType = lane`) built from Eudonet's structured description.

The cases mirror what the 2026-09-08 dump actually contains: recent entries with a house
number or a crossing road, the 2017 migration with its uppercase address labels, and the
shapes DiaLog cannot geocode (zones, axes, points and sections without bounds).
"""

from typing import cast

import polars as pl
import pytest

from api.dia_log_client.models import (
    DirectionEnum,
    RoadTypeEnum,
    SaveLocationDTO,
    SaveNamedStreetDTO,
)
from integrations.base_data_source_integration import RegulationMeasure
from integrations.base_integration import BaseIntegration
from integrations.co_paris.eudonet import locations
from integrations.co_paris.eudonet.locations import (
    Bound,
    city_code,
    compute_location_fields,
    parse_bound,
    parse_house_number,
    parse_house_number_range,
)

INPUT_COLUMNS = {
    "l_file_id": pl.Int64,
    "l_scope": pl.Utf8,
    "l_road_name": pl.Utf8,
    "l_district": pl.Utf8,
    "l_direction": pl.Utf8,
    "l_status": pl.Utf8,
    "l_from_house_number": pl.Utf8,
    "l_from_road_name": pl.Utf8,
    "l_from_address_label": pl.Utf8,
    "l_to_house_number": pl.Utf8,
    "l_to_road_name": pl.Utf8,
    "l_to_address_label": pl.Utf8,
    "l_point_house_number": pl.Utf8,
    "l_point_address_label": pl.Utf8,
}


def frame(*rows: dict) -> pl.DataFrame:
    """A raw-shaped frame with one extra column, to check that other columns survive."""
    filled = [
        {**{name: None for name in INPUT_COLUMNS}, "l_file_id": index, **row}
        for index, row in enumerate(rows, start=1)
    ]
    df = pl.DataFrame(filled, schema=INPUT_COLUMNS)
    return df.with_columns(pl.lit("m").alias("m_type"))


def point(number, **kw):
    return {
        "l_scope": "Un point",
        "l_road_name": "Rue Dulong",
        "l_district": "17ème arrondissement",
        "l_point_house_number": number,
        **kw,
    }


def section(**kw):
    return {
        "l_scope": "Une section",
        "l_road_name": "Rue Galande",
        "l_district": "5ème arrondissement",
        **kw,
    }


# -- parsing ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text, expected",
    [
        ("4", "4"),
        ("12 bis", "12"),
        ("12B", "12"),
        ("n°39 à 43", "39"),
        ("9t ", "9"),
        ("", None),
        (None, None),
        ("au droit du", None),
        ("4 avenue", "4"),
    ],
)
def test_parse_house_number(text, expected):
    assert parse_house_number(text) == expected


@pytest.mark.parametrize(
    "text, expected",
    [
        ("4", ("4", "4")),
        ("n°39 à 43", ("39", "43")),
        ("39-43", ("39", "43")),
        ("12 bis", ("12", "12")),
        ("", None),
    ],
)
def test_parse_house_number_range(text, expected):
    assert parse_house_number_range(text) == expected


def test_bound_prefers_the_explicit_house_number():
    assert parse_bound("117", None, "117 QUAI DE LA GARE") == Bound(
        "houseNumber", house_number="117"
    )


def test_bound_then_the_crossing_road():
    assert parse_bound(None, "Rue du Bac", None) == Bound("intersection", road_name="Rue du Bac")


def test_bound_from_a_2017_label_with_a_number_is_a_house_number():
    """ "117 QUAI DE LA GARE" names the road of the location itself (1 484 cases of 1 502)."""
    assert parse_bound(None, None, "117 QUAI DE LA GARE") == Bound(
        "houseNumber", house_number="117"
    )
    assert parse_bound(None, None, "1B PASSAGE TURQUETIL") == Bound("houseNumber", house_number="1")


def test_bound_from_a_2017_label_without_a_number_is_an_intersection():
    assert parse_bound(None, None, "RUE DANTE") == Bound("intersection", road_name="RUE DANTE")


def test_no_bound():
    assert parse_bound(None, None, None) is None
    assert parse_bound("", "", "") is None


@pytest.mark.parametrize(
    "district, expected",
    [
        ("1er arrondissement", "75101"),
        ("17ème arrondissement", "75117"),
        ("20ème arrondissement", "75120"),
        ("1er arrondissement ; 2ème arrondissement", "75101"),
        ("", None),
        (None, None),
        ("21ème arrondissement", None),
    ],
)
def test_city_code(district, expected):
    assert city_code(district) == expected


# -- shapes ----------------------------------------------------------------------------


def test_point_with_a_house_number():
    out = compute_location_fields(frame(point("12")))
    row = out.row(0, named=True)
    assert row["location_road_type"] == RoadTypeEnum.LANE.value
    assert row["location_city_code"] == "75117"
    assert row["location_city_label"] == "Paris"
    assert row["location_road_name"] == "Rue Dulong"
    assert (row["location_from_point_type"], row["location_from_house_number"]) == (
        "houseNumber",
        "12",
    )
    assert (row["location_to_point_type"], row["location_to_house_number"]) == ("houseNumber", "12")
    assert row["location_from_road_name"] is None and row["location_to_road_name"] is None
    assert row["location_direction"] == DirectionEnum.BOTH.value


def test_point_with_a_range_of_numbers():
    row = compute_location_fields(frame(point("n°39 à 43"))).row(0, named=True)
    assert (row["location_from_house_number"], row["location_to_house_number"]) == ("39", "43")


def test_point_numbered_only_in_its_address_label():
    """2017 migration: `2755` empty, the number sits in `2754`."""
    row = compute_location_fields(
        frame(point(None, l_point_address_label="1B PASSAGE TURQUETIL"))
    ).row(0, named=True)
    assert (row["location_from_house_number"], row["location_to_house_number"]) == ("1", "1")


def test_point_without_a_number_is_dropped():
    assert compute_location_fields(frame(point(None), point(""))).height == 0


def test_section_number_to_number():
    row = compute_location_fields(
        frame(section(l_from_house_number="126", l_to_house_number="128"))
    ).row(0, named=True)
    assert (row["location_from_point_type"], row["location_from_house_number"]) == (
        "houseNumber",
        "126",
    )
    assert (row["location_to_point_type"], row["location_to_house_number"]) == (
        "houseNumber",
        "128",
    )


def test_section_road_to_road():
    row = compute_location_fields(
        frame(section(l_from_road_name="Rue du Bac", l_to_road_name="Rue des Saints-Pères"))
    ).row(0, named=True)
    assert (row["location_from_point_type"], row["location_from_road_name"]) == (
        "intersection",
        "Rue du Bac",
    )
    assert (row["location_to_point_type"], row["location_to_road_name"]) == (
        "intersection",
        "Rue des Saints-Pères",
    )
    assert row["location_from_house_number"] is None


def test_section_mixed_number_and_road():
    row = compute_location_fields(
        frame(section(l_from_house_number="39", l_to_road_name="Rue des Dames"))
    ).row(0, named=True)
    assert row["location_from_point_type"] == "houseNumber"
    assert row["location_to_point_type"] == "intersection"


def test_section_from_the_2017_migration():
    """Only `Libellé adresse début/fin` are filled, uppercase, accents stripped."""
    row = compute_location_fields(
        frame(
            section(
                l_from_address_label="117 QUAI DE LA GARE",
                l_to_address_label="BOULEVARD VINCENT AURIOL",
            )
        )
    ).row(0, named=True)
    assert (row["location_from_point_type"], row["location_from_house_number"]) == (
        "houseNumber",
        "117",
    )
    assert (row["location_to_point_type"], row["location_to_road_name"]) == (
        "intersection",
        "BOULEVARD VINCENT AURIOL",
    )


def test_sections_without_both_bounds_are_dropped():
    out = compute_location_fields(
        frame(
            section(),  # the 2017 migration left 4 466 of these: bounds only in the PDF
            section(l_from_house_number="2"),  # a single bound
            section(l_to_road_name="Rue Malar"),
        )
    )
    assert out.height == 0


def test_whole_street():
    row = compute_location_fields(
        frame(
            {
                "l_scope": "La totalité de la voie",
                "l_road_name": "Rue Cler",
                "l_district": "7ème arrondissement",
            }
        )
    ).row(0, named=True)
    assert row["location_road_name"] == "Rue Cler"
    assert row["location_from_point_type"] is None and row["location_to_point_type"] is None


def test_zones_axes_and_unknown_scopes_are_dropped():
    out = compute_location_fields(
        frame(
            {"l_scope": "Une zone", "l_road_name": "Rue Nationale"},
            {"l_scope": "Un axe", "l_road_name": "Boulevard Périphérique"},
            {"l_scope": "Autre chose", "l_road_name": "Rue X"},
        )
    )
    assert out.height == 0


def test_withdrawn_missing_road_and_absent_locations_are_dropped():
    out = compute_location_fields(
        frame(
            point("4", l_status="Retiré"),
            point("4", l_status="Supprimé"),
            point("4", l_road_name=""),
            point("4", l_district=None),  # DiaLog refuses Paris as a whole (75056)
            {"l_file_id": None},  # a measure without any location
        )
    )
    assert out.height == 0


def test_kept_rows_keep_their_other_columns_and_lose_the_rejection_column():
    out = compute_location_fields(frame(point("4"), point(None)))
    assert out.height == 1
    assert out["m_type"].to_list() == ["m"]
    assert "location_rejection" not in out.columns


@pytest.mark.parametrize(
    "label, expected",
    [
        ("du début vers la fin du segment", "A_TO_B"),
        ("de la fin vers le début du segment", "B_TO_A"),
        ("dans les deux sens", "BOTH"),
        ("dans le sens de la circulation générale", "BOTH"),
        (None, "BOTH"),
    ],
)
def test_direction(label, expected):
    row = compute_location_fields(frame(point("4", l_direction=label))).row(0, named=True)
    assert row["location_direction"] == expected


# -- contract with the pivot and the DTO -------------------------------------------------


def test_every_location_column_is_a_pivot_key():
    produced = {name for name in locations.LOCATION_COLUMNS if name != "location_rejection"}
    assert produced <= set(RegulationMeasure.__annotations__)


def test_the_row_builds_a_named_street_dto():
    """`create_save_location_dto` hands every `location_*` key to `SaveNamedStreetDTO`."""
    row = compute_location_fields(
        frame(section(l_from_house_number="39", l_to_road_name="Rue des Dames"))
    ).row(0, named=True)
    measure = cast(
        RegulationMeasure,
        {key: value for key, value in row.items() if key.startswith("location_")},
    )

    dto = BaseIntegration.create_save_location_dto(
        BaseIntegration.__new__(BaseIntegration), measure
    )

    assert isinstance(dto, SaveLocationDTO)
    assert dto.road_type == RoadTypeEnum.LANE
    assert isinstance(dto.named_street, SaveNamedStreetDTO)
    payload = dto.to_dict()["namedStreet"]
    assert payload["roadName"] == "Rue Galande"
    assert payload["fromPointType"] == "houseNumber"
    assert payload["fromHouseNumber"] == "39"
    assert payload["toPointType"] == "intersection"
    assert payload["toRoadName"] == "Rue des Dames"
    assert payload["direction"] == "BOTH"
