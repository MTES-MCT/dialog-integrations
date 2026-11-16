from typing import Any

import pytest
from loguru import logger

from integrations.co_paris.eudonet.free_text_filters import (
    discard_unread_location_details,
    discard_unread_measure_paragraphs,
    discard_unread_regulation_texts,
)
from integrations.co_paris.eudonet.neutral_text import (
    act_number_key,
    article_is_neutral,
    note_is_neutral,
    paragraph_is_neutral,
)
from integrations.co_paris.eudonet.raw_rows import build_raw_dataframe
from integrations.co_paris.eudonet.vehicles import compute_vehicle_fields
from tests.co_paris.test_eudonet_transform import make_frame, make_row, pipeline


@pytest.fixture
def captured_logs():
    collected: list[str] = []
    handle = logger.add(collected.append, level="WARNING", format="{message}")
    yield collected
    logger.remove(handle)


@pytest.mark.parametrize(
    "article",
    [
        "Les dispositions du présent arrêté abrogent et remplacent toutes les dispositions "
        "contraires antérieures.",
        "Les arrêtés préfectoraux énumérés ci-après sont abrogés : - arrêté n°98-12005 du 21 "
        "décembre 1998 - arrêté n° 00-10775 du 19 mai 2000",
        "- l'arrêté n°2007-174 du 28/12/2007 neutralisant la circulation les jours du marché, "
        "est abrogé.",
        "Les dispositions de l'arrêté n°2017P12620 susvisé sont suspendues pendant la durée des "
        "travaux.",
        "Les mesures édictées par le présent arrêté sont applicables jusqu'à la fin des travaux "
        "et la dépose de la signalisation.",
        "Conformément aux dispositions de l'article R. 417-10 du code de la route, les "
        "infractions au présent arrêté seront constatées par des procès-verbaux.",
        "Dans cette portion de voie, les cyclistes sont autorisés à circuler à contresens de la "
        "circulation générale.",
        "Deux barrières manœuvrables sont installées au n°107 et au n°117 rue des Amandiers.",
        "<p>La voie de l'article 1er complète l'arrêté préfectoral 95-11310 du 21 août 1995.</p>",
        "Les présentes dispositions sont applicables jusqu'au 31 décembre de l'année en cours "
        "et reconduites tacitement pendant toute la durée de la menace terroriste.",
        "L'annexe 1 de l'arrêté préfectoral du 23 novembre 2010 susvisé est modifiée comme suit.",
        "L'arrêté n° 2013-01231 du 9 décembre 2013 susvisé est ainsi modifié : « 14 places » est "
        "remplacée par « 11 places ».",
        "",
        None,
    ],
)
def test_a_complementary_article_that_changes_nothing_is_neutral(article):
    assert article_is_neutral(article, has_pedestrian_area=False)


@pytest.mark.parametrize(
    "article",
    [
        "Les mesures du présent arrêté ne s'appliquent pas à la rue Raymond Losserand.",
        "Par dérogation à l'article 1er du présent arrêté les véhicules d'approvisionnement du "
        "marché sont autorisés à stationner le vendredi de 10 heures 30 à 20 heures.",
        "Dans la portion de voie, la circulation des véhicules dont la longueur excède 10 "
        "mètres est interdite.",
        # Amending this act is not amending another one.
        "L'article 2 du présent arrêté est modifié : la rue Cadet est ajoutée.",
        # A neutral sentence does not cover the next one.
        "Les dispositions du présent arrêté abrogent et remplacent toutes les dispositions "
        "contraires antérieures. Toutefois ces dispositions ne sont pas applicables le dimanche.",
    ],
)
def test_a_complementary_article_that_narrows_the_act_is_not_neutral(article):
    assert not article_is_neutral(article, has_pedestrian_area=False)


def test_the_local_access_of_a_pedestrian_area_is_read_only_when_the_act_holds_one():
    access = (
        "La circulation des véhicules nécessaires à la desserte interne de cette aire piétonne "
        "est autorisée et limitée strictement aux catégories de véhicules suivants : - véhicules "
        "de secours ; - véhicules des riverains."
    )
    assert article_is_neutral(access, has_pedestrian_area=True)
    assert not article_is_neutral(access, has_pedestrian_area=False)
    assert not article_is_neutral(access + " - livraisons de 7h à 10h.", has_pedestrian_area=True)


@pytest.mark.parametrize(
    "note, neutral",
    [
        ("Arrêté transmis par mail le 06/03/2026 : BMO+M10+MOA+SCOP", True),
        ("ARRETE TRANSMIS EN MAIRIE LE 27/04/2026 POUR AFFICHAGE", True),
        ("Envoyé le 13/09/2024 au BMO/MOA/M10", True),
        (None, True),
        ("Attention cet arrêté est abrogé par le n° 2013P0806 du 22 août 2013", False),
        ('La rue Hermel est devenue une aire piétonne "rue aux écoles"', False),
        (
            "Cet arrêté a été injecté dans GACS le 23/01/19. Seules les mesures et les "
            "localisations ont été saisies.",
            False,
        ),
    ],
)
def test_only_the_office_bookkeeping_is_a_neutral_note(note, neutral):
    assert note_is_neutral(note) is neutral


@pytest.mark.parametrize(
    "paragraph, zone_30, neutral",
    [
        ('Quartier Vert " Censier "', True, True),
        ('Il est institué une zone 30 dénommée "Saint-paul" délimité comme suit :', True, True),
        ('Quartier Vert " Censier "', False, False),
        (
            "Tout stationnement d'un véhicule en infraction avec cette interdiction est considéré "
            "comme gênant.",
            False,
            True,
        ),
        ("Les rues de Belleville et des Pyrénées sont exclues de cette zone 30", True, False),
        ("cycles", False, False),
        ("les jours de marché de 2h00 à 17h00", False, False),
        ("Ces dispositions sont applicables les dimanches de 10h00 à 14h00.", False, False),
    ],
)
def test_a_measure_paragraph_is_neutral_only_when_it_names_or_qualifies(
    paragraph, zone_30, neutral
):
    assert paragraph_is_neutral(paragraph, is_zone_30=zone_30) is neutral


@pytest.mark.parametrize(
    "overrides",
    [
        {"a_next_version": "2019P15935"},
        {"a_repealing_act": "2021P10825"},
        {"a_repealed_by_clone": "2021P112222"},
        {"a_repealed_by_multiple": "2022P10001"},
        {"a_notes": "Partiellement abrogé par 2021P112184 concernant rue de Moscou"},
        {"a_articles": ["Les mesures du présent arrêté ne s'appliquent pas à la rue Cadet."]},
        {"a_key_1": "PARIS RESPIRE"},
        {"a_key_2": "MARCHES"},
        {"a_title_html": "Instituant une aire piétonne les dimanches, rue des Moines"},
        {"a_title_html": "Interdisant à certaines heures la circulation"},
        {"a_title_html": "Réglementant le stationnement aux abords du marché alimentaire Aligre"},
    ],
)
def test_a_replaced_repealed_or_qualified_act_is_dropped_whole(overrides, captured_logs):
    kept = make_row(a_file_id=1, a_identifier="2024P10001", l_file_id=11)
    dropped = make_row(a_file_id=2, a_identifier="2024P10002", m_file_id=20, **overrides)
    second_location = make_row(a_file_id=2, a_identifier="2024P10002", m_file_id=20, l_file_id=21)
    second_location.update(overrides)
    out = discard_unread_regulation_texts(make_frame(kept, dropped, second_location))
    assert out["a_identifier"].to_list() == ["2024P10001"]
    assert any("Dropping 1 regulations" in line for line in captured_logs)


@pytest.mark.parametrize(
    "title",
    [
        "Limitant la vitesse à 15 km/heure dans les voies citées en annexe",
        "Modifiant les règles de stationnement rue du Marché Saint-Honoré",
    ],
)
def test_a_title_naming_a_street_or_a_speed_unit_keeps_the_act(title):
    assert discard_unread_regulation_texts(make_frame(make_row(a_title_html=title))).height == 1


def test_an_act_with_neutral_texts_is_kept():
    row = make_row(
        a_notes="Envoyé le 13/09/2024 au BMO/MOA/M10",
        a_articles=["L'arrêté n° 80-16555 du 2 août 1980 est abrogé."],
    )
    assert discard_unread_regulation_texts(make_frame(row)).height == 1


def test_a_measure_with_a_paragraph_that_narrows_it_is_dropped():
    df = make_frame(
        make_row(m_file_id=10, l_file_id=100),
        make_row(m_file_id=11, l_file_id=110, m_paragraph="les jours de marché de 2h00 à 17h00"),
        make_row(
            m_file_id=12,
            l_file_id=120,
            m_paragraph="Tout stationnement d'un véhicule en "
            "infraction avec cette interdiction est considéré comme gênant",
        ),
    )
    assert discard_unread_measure_paragraphs(df)["m_file_id"].to_list() == [10, 12]


@pytest.mark.parametrize(
    "overrides",
    [
        {"l_notes": "SUR LA PISTE CYCLABLE"},
        {"l_reserved_space_type": "Livraisons"},
        {"l_reserved_places": "3"},
        {"l_reserved_length": "10"},
        {"l_displayed_length": "sur 10 ml"},
    ],
)
def test_a_location_narrowed_by_its_details_is_dropped(overrides: dict[str, Any]):
    section = {"l_scope": "Une section", "l_point_house_number": None}
    df = make_frame(make_row(l_file_id=100), make_row(l_file_id=101, **section, **overrides))
    assert discard_unread_location_details(df)["l_file_id"].to_list() == [100]


def test_reserved_places_and_a_length_on_a_point_keep_it_but_not_its_notes():
    df = make_frame(
        make_row(l_file_id=100, l_reserved_places="1", l_reserved_space_type="payant"),
        make_row(l_file_id=101, l_displayed_length="sur 10 ml"),
        make_row(l_file_id=102, l_notes="Exclue du périmètre"),
    )
    assert discard_unread_location_details(df)["l_file_id"].to_list() == [100, 101]


@pytest.mark.parametrize(
    "paragraph",
    [
        "Toutefois ces dispositions ne sont pas applicables aux véhicules de secours.",
        "sauf aux véhicules de secours",
        "sauf aux véhicules de secours et aux véhicules des sapeurs pompiers",
    ],
)
def test_an_emergency_exemption_in_the_paragraph_becomes_the_measure_exemption(paragraph):
    row = make_row(m_type="circulation interdite", l_scope="Une section", m_paragraph=paragraph)
    out = discard_unread_measure_paragraphs(pipeline(row))
    assert out.height == 1
    assert out.pipe(compute_vehicle_fields)["vehicle_exempted_types"].to_list() == [
        ["emergencyServices"]
    ]


@pytest.mark.parametrize(
    "paragraph",
    [
        "sauf aux véhicules de livraison",
        "sauf aux véhicules de secours de 8h à 18h",
        "aux véhicules de secours",
    ],
)
def test_any_other_exemption_paragraph_still_drops_the_measure(paragraph):
    row = make_row(m_type="circulation interdite", m_paragraph=paragraph)
    assert discard_unread_measure_paragraphs(pipeline(row)).height == 0


def test_the_complementary_articles_reach_their_regulation_but_not_the_measure_articles():
    tables = {
        "1100": [{"FileId": 1, "1101": "2024P10001", "1108": "Permanent", "1107": "En vigueur"}],
        "1200": [],
        "1400": [
            {"1101_fid": 1, "1402": "Mesure", "1405": "Le stationnement est interdit"},
            {"1101_fid": 1, "1402": "Article complémentaire", "1405": "Sont abrogés : …"},
            {"1101_fid": 1, "1402": "Article complémentaire", "1405": ""},
        ],
    }
    assert build_raw_dataframe(tables)["a_articles"].to_list() == [["Sont abrogés : …"]]


@pytest.mark.parametrize(
    "number, key",
    [
        ("89-10393", (1989, "", 10393)),
        ("1989-10393", (1989, "", 10393)),
        ("64.10389", (1964, "", 10389)),
        ("2013P0842", (2013, "P", 842)),
        ("2010-00002", (2010, "", 2)),
        ("2003-139", (2003, "", 139)),
        ("1996-10651-07", (1996, "", 10651)),
        ("rue", None),
    ],
)
def test_an_act_number_reads_the_same_in_eudonet_and_in_a_citation(number, key):
    assert act_number_key(number) == key


@pytest.mark.parametrize(
    "article",
    [
        "L'arrêté municipal n° 2010-00002 du 28 janvier 2010 est abrogé.",
        "Les dispositions de l'arrêté n° 10-00002 susvisé sont abrogées en ce qui concerne "
        "la rue Cadet.",
        "Les dispositions de l'arrêté n°2010-002 sont suspendues pendant la durée des travaux.",
    ],
)
def test_an_act_another_act_repeals_or_suspends_is_dropped(article, captured_logs):
    cited = make_row(a_file_id=1, a_identifier="2010-002", l_file_id=11)
    citing = make_row(
        a_file_id=2, a_identifier="2014P0160", m_file_id=20, l_file_id=21, a_articles=[article]
    )
    out = discard_unread_regulation_texts(make_frame(cited, citing))
    assert out["a_identifier"].to_list() == ["2014P0160"]
    assert any("repealed or amended by another act" in line for line in captured_logs)


def test_an_act_citing_another_without_repealing_it_keeps_both():
    cited = make_row(a_file_id=1, a_identifier="2010-002", l_file_id=11)
    citing = make_row(
        a_file_id=2,
        a_identifier="2014P0160",
        m_file_id=20,
        l_file_id=21,
        a_title_html="Vu l'arrêté n° 2010-002 du 28 janvier 2010",
    )
    assert discard_unread_regulation_texts(make_frame(cited, citing)).height == 2


def test_an_act_amending_itself_is_not_dropped():
    row = make_row(
        a_identifier="2014P0160",
        a_title_html="Arrêté n° 2014P0160 modifiant les règles de stationnement",
    )
    assert discard_unread_regulation_texts(make_frame(row)).height == 1
