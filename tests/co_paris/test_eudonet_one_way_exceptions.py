from typing import Any

import polars as pl
import pytest
from loguru import logger

from integrations.co_paris.eudonet.measures import (
    compute_measure_fields,
    discard_measures_out_of_model,
)
from integrations.co_paris.eudonet.one_way_exceptions import arbitrate_one_way_streets, lane_users
from integrations.co_paris.eudonet.regulations import compute_regulation_fields
from integrations.co_paris.eudonet.vehicles import compute_vehicle_fields
from tests.co_paris.test_eudonet_transform import make_frame, make_row

ONE_WAY = "sens interdit (ou sens unique)"
BUS_USERS = [["véhicule concerné (2)", "des véhicules de transports en commun"]]


@pytest.fixture
def captured_logs():
    collected: list[str] = []
    handle = logger.add(collected.append, level="INFO", format="{message}")
    yield collected
    logger.remove(handle)


def one_way(**overrides: Any) -> dict[str, Any]:
    """« Un sens unique … rue Baudricourt, depuis rue Nationale jusqu'à rue de Tolbiac »."""
    return make_row(
        **{
            "m_file_id": 10,
            "m_type": ONE_WAY,
            "m_params": [],
            "l_file_id": 100,
            "l_scope": "Une section",
            "l_road_name": "Rue Baudricourt",
            "l_district": "13ème arrondissement",
            "l_point_house_number": None,
            "l_from_number_complement": "depuis",
            "l_from_road_name": "Rue Nationale",
            "l_to_number_complement": "jusqu'à",
            "l_to_road_name": "Rue de Tolbiac",
            **overrides,
        }
    )


def lane(**overrides: Any) -> dict[str, Any]:
    """A bus lane of the same act, from Tolbiac to Nationale: the other way."""
    return one_way(
        **{
            "m_file_id": 20,
            "m_type": "voie réservée",
            "m_params": BUS_USERS,
            "l_file_id": 200,
            "l_from_road_name": "Rue de Tolbiac",
            "l_to_road_name": "Rue Nationale",
            **overrides,
        }
    )


def published(*rows: dict[str, Any]) -> pl.DataFrame:
    return (
        make_frame(*rows)
        .pipe(compute_regulation_fields)
        .pipe(compute_measure_fields)
        .pipe(arbitrate_one_way_streets)
        .pipe(discard_measures_out_of_model)
        .pipe(compute_vehicle_fields)
    )


def exemptions(df: pl.DataFrame) -> list:
    return df.select("vehicle_exempted_types", "vehicle_other_exempted_type_text").rows()


def test_a_bus_lane_the_other_way_exempts_the_buses_from_the_one_way_street():
    out = published(one_way(), lane())
    assert exemptions(out) == [(["other"], "véhicules de transports en commun")]


def test_a_lane_said_against_the_general_traffic_is_the_other_way_whatever_its_bounds():
    other_way = lane(
        l_direction="dans le sens inverse de la circulation générale",
        l_from_number_complement="dans sa partie comprise entre",
        l_to_number_complement="et le",
    )
    assert exemptions(published(one_way(), other_way)) == [
        (["other"], "véhicules de transports en commun")
    ]


def test_a_two_way_cycle_track_exempts_the_cycles():
    track = lane(
        m_type="voie cyclable",
        m_params=[["Direction", "bidirectionnelle"]],
        l_from_number_complement="dans sa partie comprise entre",
        l_to_number_complement="et le",
    )
    assert exemptions(published(one_way(), track)) == [(["bicycle"], None)]


def test_a_lane_going_the_same_way_changes_nothing():
    same_way = lane(l_from_road_name="Rue Nationale", l_to_road_name="Rue de Tolbiac")
    assert exemptions(published(one_way(), same_way)) == [(None, None)]


def test_a_lane_on_another_street_changes_nothing():
    elsewhere = lane(l_road_name="Rue Nationale")
    assert exemptions(published(one_way(), elsewhere)) == [(None, None)]


@pytest.mark.parametrize(
    "overrides",
    [
        # Another section: how much of the one-way street it opens is not known.
        {"l_from_road_name": "Rue Charcot"},
        # « dans sa partie comprise entre … et le … » does not say which way.
        {
            "l_from_number_complement": "dans sa partie comprise entre",
            "l_to_number_complement": "et le",
        },
        # Users outside the closed list.
        {"m_params": [["véhicule concerné (2)", "du tramway"]]},
        # No users at all.
        {"m_params": []},
        # No location: which street is not known.
        {
            "l_file_id": None,
            "l_scope": None,
            "l_road_name": None,
            "l_from_road_name": None,
            "l_to_road_name": None,
        },
    ],
)
def test_a_lane_whose_extent_way_or_users_cannot_be_read_drops_the_one_way_street(
    overrides, captured_logs
):
    assert published(one_way(), lane(**overrides)).height == 0
    assert any("may open the other way" in line for line in captured_logs)


def test_a_meeting_zone_on_the_whole_street_lets_the_cycles_ride_both_ways():
    zone = lane(m_type="zone de rencontre", m_params=[], l_scope="La totalité de la voie")
    assert exemptions(published(one_way(), zone)) == [(["bicycle"], None)]


def test_a_meeting_zone_where_the_act_keeps_cycles_from_the_contraflow_exempts_no_one():
    zone = lane(m_type="zone de rencontre", m_params=[], l_scope="La totalité de la voie")
    kept_out = lane(
        m_file_id=30,
        l_file_id=300,
        m_type="interdiction double sens pour les cyclistes",
        m_params=[],
        l_from_road_name="Rue Nationale",
        l_to_road_name="Rue de Tolbiac",
    )
    assert exemptions(published(one_way(), zone, kept_out)) == [(None, None)]


def test_two_way_traffic_restored_on_the_street_drops_the_one_way_street():
    restored = lane(m_type="rétablissement double sens", m_params=[])
    assert published(one_way(), restored).height == 0


def test_sections_of_one_measure_with_different_exemptions_become_two_dialog_measures():
    beside = one_way(
        l_file_id=101, l_road_name="Rue Jeanne d'Arc", l_from_road_name="Rue Nationale"
    )
    out = published(one_way(), beside, lane()).sort("l_file_id")
    keys = out["measure_group_key"].to_list()
    assert len(set(keys)) == 2
    assert exemptions(out) == [(["other"], "véhicules de transports en commun"), (None, None)]


def test_the_users_of_a_lane_are_its_named_vehicles_and_its_exemptions():
    params = [
        ["véhicule concerné (2)", "des  véhicules de transports en commun et cycles"],
        ["dérogation pour véhicule", "aux taxis"],
    ]
    assert lane_users("voie réservée", params) == [
        "véhicules de transports en commun",
        "aux cycles",
        "aux taxis",
    ]
