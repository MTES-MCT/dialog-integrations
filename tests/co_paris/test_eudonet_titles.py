from typing import Any

import polars as pl
import pytest

from integrations.co_paris.eudonet.free_text import Explained
from integrations.co_paris.eudonet.raw_rows import RAW_COLUMNS
from integrations.co_paris.eudonet.titles import (
    Act,
    bound_key,
    disagreement,
    judge_unread_titles,
    read_title,
)
from integrations.co_paris.eudonet.verdicts import UNREAD_TITLE
from tests.co_paris.steps import discard_unread_titles, drop_refused

RIVOLI = Explained(("Rue de Rivoli", "1er arrondissement"))


def parking_act(**overrides: Any) -> Act:
    act = Act(temporary=True)
    for name, value in overrides.items():
        setattr(act, name, value)
    return act


@pytest.mark.parametrize(
    "title",
    [
        "Modifiant, à titre provisoire, la règle du stationnement rue de Rivoli, à Paris 1er",
        "IR_modifiant à titre provisoire les règles de stationnement rue de Rivoli à Paris dans le "
        "1er arrondissement",
        "instaurant la règle du stationnement gênant la circulation générale rue de Rivoli",
    ],
)
def test_a_title_the_fields_explain_holds(title):
    reading = read_title(title, RIVOLI)
    assert reading.unread == ()
    assert reading.families == {"parking"}
    assert disagreement(reading, parking_act()) is None


def test_a_condition_written_in_the_title_drops_the_act():
    reading = read_title(
        "réglementant le stationnement rue de Rivoli durant les épisodes de pollution "
        "atmosphérique",
        RIVOLI,
    )
    assert disagreement(reading, parking_act()) == "title not understood"


def test_a_title_naming_parking_on_a_closure_holds():
    reading = read_title(
        "modifiant, à titre provisoire, la règle du stationnement rue de Rivoli", RIVOLI
    )
    assert reading.families == {"parking"}
    assert disagreement(reading, parking_act()) is None


def test_provisional_is_said_of_a_temporary_act_only():
    reading = read_title("interdisant, à titre provisoire, le stationnement rue de Rivoli", RIVOLI)
    assert disagreement(reading, parking_act(temporary=False)) == (
        "title says provisional, act is permanent"
    )


def test_a_speed_in_the_title_is_one_of_the_act():
    reading = read_title("limitant la vitesse des véhicules à 30 km/h rue de Rivoli", RIVOLI)
    act = Act(temporary=False, speeds={20})
    assert disagreement(reading, act) == "title speed not in the act"
    act.speeds = {30}
    assert disagreement(reading, act) is None


def test_bounds_in_the_title_are_the_act_s_bounds_however_written():
    reading = read_title(
        "limitant la vitesse à 30 km/h dans la rue Erlanger, entre la rue d'Auteuil et la rue "
        "Molitor",
        Explained(("Rue Erlanger",)),
    )
    act = Act(
        temporary=False,
        speeds={30},
        bound_keys={bound_key("RUE D AUTEUIL"), bound_key("RUE MOLITOR")},
    )
    assert disagreement(reading, act) is None
    act.bound_keys = {bound_key("RUE D AUTEUIL")}
    assert disagreement(reading, act) == "title bounds not in the act's locations"
    # Other bounds, on locations all narrower than the street: nothing published is wider.
    act.narrower_than_the_street = True
    assert disagreement(reading, act) is None


def test_a_title_amending_another_act_describes_that_act():
    reading = read_title(
        "complétant dans le 17ème arrondissement l'arrêté n° 96-10651 du 2 mai 1996 désignant à "
        "Paris les voies où l'arrêt et le stationnement sont considérés comme gênants",
        Explained(),
    )
    assert reading.unread == ()


def _row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = dict.fromkeys(RAW_COLUMNS)
    row.update(
        a_file_id=1,
        a_identifier="2026T10001",
        a_type="Temporaire",
        a_title_html="modifiant, à titre provisoire, la règle du stationnement rue de Rivoli",
        m_file_id=10,
        m_type="interdiction de stationnement",
        l_file_id=100,
        l_road_name="Rue de Rivoli",
        l_district="1er arrondissement",
    )
    row.update(overrides)
    return row


BOUNDED_TITLE = (
    "limitant la vitesse à 30 km/h rue de Rivoli, entre la rue du Louvre et la rue du Pont Neuf"
)


def test_title_bounds_on_a_whole_street_drop_the_act_even_through_a_dropped_location():
    whole_street = _row(a_title_html=BOUNDED_TITLE, m_type="zone 30", l_scope="Une section")
    bounded = _row(
        a_file_id=2,
        a_identifier="2026T10002",
        a_title_html=BOUNDED_TITLE,
        m_type="zone 30",
        m_file_id=20,
        l_file_id=200,
        l_scope="Une section",
        l_from_road_name="RUE DES HALLES",
        l_to_road_name="RUE DE L ARBRE SEC",
    )
    raw = pl.DataFrame([whole_street, bounded], schema=RAW_COLUMNS)
    assert discard_unread_titles(raw)["a_identifier"].unique().to_list() == ["2026T10002"]
    # A whole-street location refused earlier still counts: the title reads every row.
    other = _row(
        a_file_id=2,
        a_identifier="2026T10002",
        m_type="zone 30",
        m_file_id=21,
        l_file_id=201,
        l_scope="Une section",
        a_title_html=BOUNDED_TITLE,
    )
    rows = pl.concat([raw, pl.DataFrame([other], schema=RAW_COLUMNS)])
    alive = pl.Series([True, True, False])
    assert drop_refused(judge_unread_titles(rows, alive), UNREAD_TITLE).is_empty()


@pytest.mark.parametrize(
    "title, condition",
    [
        ("relatif à la vitesse durant les épisodes de pollution atmosphérique", "épisodes"),
        ("suspendant l'opération Paris Respire en cas d'épisode neigeux", "en cas d"),
        (
            "instituant les limitations d'accès aux berges de Seine lorsque la Seine monte",
            "lorsque",
        ),
        ("réglementant le stationnement rue du Pic de Barrette", None),
    ],
)
def test_a_title_that_sets_a_condition_is_caught_before_its_formulas(title, condition):
    from integrations.co_paris.eudonet.regulations import title_condition

    assert title_condition(title) == condition
