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
    city_codes,
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
    "l_side": pl.Utf8,
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
    assert parse_bound(None, None, "117 QUAI DE LA GARE") == Bound(
        "houseNumber", house_number="117"
    )
    assert parse_bound(None, None, "1B PASSAGE TURQUETIL") == Bound("houseNumber", house_number="1")


def test_bound_from_a_2017_label_without_a_number_is_an_intersection():
    assert parse_bound(None, None, "RUE DANTE") == Bound("intersection", road_name="Rue Dante")


def test_a_2017_label_is_respelled_with_the_official_street_name():
    assert locations.official_street_name("RUE D AUTEUIL") == "Rue d'Auteuil"
    assert locations.official_street_name("PLACE PAUL PAINLEVE") == "Place Paul Painlevé"


def test_a_label_outside_the_referential_or_already_spelled_is_left_as_is():
    assert locations.official_street_name("PONT DES ARTS ZZZ") == "PONT DES ARTS ZZZ"
    assert locations.official_street_name("Rue des Dames") == "Rue des Dames"


def test_no_bound():
    assert parse_bound(None, None, None) is None
    assert parse_bound("", "", "") is None


@pytest.mark.parametrize(
    "district, expected",
    [
        ("1er arrondissement", ["75101"]),
        ("17ème arrondissement", ["75117"]),
        ("20ème arrondissement", ["75120"]),
        ("1er arrondissement ; 2ème arrondissement", ["75101", "75102"]),
        ("", []),
        (None, []),
        ("21ème arrondissement", []),
    ],
)
def test_city_codes(district, expected):
    assert city_codes(district) == expected


def city_codes_of(*rows: dict) -> list[str | None]:
    return compute_location_fields(frame(*rows))["location_city_code"].to_list()


def test_a_missing_arrondissement_is_the_only_one_of_its_street():
    """Rue Dulong has addresses in the 17th only: a gap filled, not a guess (R-06)."""
    assert city_codes_of(point("4", l_district=None)) == ["75117"]


def test_a_missing_arrondissement_is_read_on_the_house_number():
    assert city_codes_of(
        point("20", l_district=None, l_road_name="Boulevard Saint-Michel"),
        point("21", l_district=None, l_road_name="Boulevard Saint-Michel"),
    ) == ["75106", "75105"]


def test_bounds_falling_in_two_arrondissements_settle_nothing():
    row = section(
        l_road_name="Boulevard Saint-Michel",
        l_district=None,
        l_from_house_number="20",
        l_to_house_number="21",
    )
    assert compute_location_fields(frame(row)).height == 0


def test_a_street_on_two_arrondissements_takes_the_one_of_its_numbers():
    row = section(
        l_road_name="Rue de Vaugirard",
        l_district="6ème arrondissement ; 15ème arrondissement",
        l_from_house_number="200",
        l_to_house_number="250",
    )
    assert city_codes_of(row) == ["75115"]


def test_a_street_on_two_arrondissements_without_numbers_keeps_the_first_cited():
    row = section(
        l_road_name="Rue de Vaugirard",
        l_district="15ème arrondissement ; 6ème arrondissement",
        l_from_road_name="Rue de Rennes",
        l_to_road_name="Rue d'Assas",
    )
    assert city_codes_of(row) == ["75115"]


def test_numbers_pointing_outside_the_cited_arrondissements_keep_the_first_cited():
    row = section(
        l_road_name="Rue de Vaugirard",
        l_district="6ème arrondissement ; 7ème arrondissement",
        l_from_house_number="200",
        l_to_house_number="250",
    )
    assert city_codes_of(row) == ["75106"]


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
    row = compute_location_fields(
        frame(
            point(
                None,
                l_road_name="Passage Turquetil",
                l_district="11ème arrondissement",
                l_point_address_label="1B PASSAGE TURQUETIL",
            )
        )
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


def test_section_from_the_2017_migration():
    row = compute_location_fields(
        frame(
            section(
                l_road_name="Quai de la Gare",
                l_district="13ème arrondissement",
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
        "Boulevard Vincent Auriol",
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
            point("4", l_district=None, l_road_name="Rue Imaginaire"),
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
    "column, label",
    [
        ("l_direction", "dans les deux sens"),
        ("l_direction", "dans le sens de la circulation générale"),
        ("l_direction", None),
        ("l_side", "côté pair et impair"),
        ("l_side", "dans l'axe de la chaussée"),
    ],
)
def test_a_location_not_limited_to_one_direction_goes_both_ways(column, label):
    row = compute_location_fields(frame(point("4", **{column: label}))).row(0, named=True)
    assert row["location_direction"] == "BOTH"


@pytest.mark.parametrize(
    "column, label",
    [
        ("l_direction", "du début vers la fin du segment"),
        ("l_direction", "de la fin vers le début du segment"),
        ("l_side", "côté pair"),
        ("l_side", "côté impair"),
    ],
)
def test_a_location_limited_to_one_direction_or_one_side_is_dropped(column, label):
    """Never widened to both directions or both sides, never published on one."""
    assert compute_location_fields(frame(point("4", **{column: label}))).height == 0


def test_a_bound_numbered_on_the_crossing_street_drops_the_location():
    corner = {
        "l_road_name": "Rue Brunel",
        "l_district": "17ème arrondissement",
        "l_from_house_number": "28",
        "l_from_address_label": "28 Place Saint-Ferdinand",
        "l_to_house_number": "1",
        "l_to_address_label": "1 Avenue de la Grande Armée",
    }
    misspelt = {
        "l_road_name": "Villa Patrice Boudart",
        "l_district": "14ème arrondissement",
        "l_from_address_label": "4 VILLA PATRICE BOUDARD",
        "l_to_address_label": "12 VILLA PATRICE BOUDARD",
    }
    out = compute_location_fields(
        frame(
            section(**corner),
            section(**misspelt),
            point("8", l_point_address_label="8 Rue de la Paix"),
        )
    )
    assert out["location_road_name"].to_list() == ["Villa Patrice Boudart"]


def test_every_location_column_is_a_pivot_key():
    working = {"location_rejection", "location_district_source"}
    produced = {name for name in locations.LOCATION_COLUMNS if name not in working}
    assert produced <= set(RegulationMeasure.__annotations__)


def test_the_row_builds_a_named_street_dto():
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


def test_a_numbered_road_label_is_a_house_number_on_the_road_itself():
    bound = parse_bound(None, "22 RUE DES REGLISES", "22 RUE DES REGLISES")
    assert bound == Bound(locations.POINT_TYPE_HOUSE_NUMBER, house_number="22")
    bound = parse_bound(None, "RUE DU FAUCONNIER", None)
    assert bound == Bound(locations.POINT_TYPE_INTERSECTION, road_name="Rue du Fauconnier")
