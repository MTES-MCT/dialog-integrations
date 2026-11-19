from typing import Any

import pytest
from loguru import logger

from integrations.co_paris.eudonet import free_text
from integrations.co_paris.eudonet.citations import act_number_key
from integrations.co_paris.eudonet.free_text import Code, clauses, read_text
from integrations.co_paris.eudonet.free_text_filters import (
    discard_location_details,
    discard_superseded_regulations,
    discard_unread_texts,
)
from integrations.co_paris.eudonet.raw_rows import build_raw_dataframe
from integrations.co_paris.eudonet.vehicles import compute_vehicle_fields
from tests.co_paris.test_eudonet_transform import make_frame, make_row, pipeline

ONE_WAY = "sens interdit (ou sens unique)"
PEDESTRIAN_AREA = "aire piétonne"


@pytest.fixture
def captured_logs():
    collected: list[str] = []
    handle = logger.add(collected.append, level="INFO", format="{message}")
    yield collected
    logger.remove(handle)


def kept_measures(*rows: dict[str, Any]) -> list[int]:
    out = discard_unread_texts(make_frame(*rows))
    return out["m_file_id"].unique(maintain_order=True).to_list()


def exempted(df) -> list:
    return df.pipe(compute_vehicle_fields)["vehicle_exempted_types"].to_list()


# The reading itself.


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
        "Deux barrières manœuvrables sont installées au n°107 et au n°117 rue des Amandiers.",
        "<p>La voie de l'article 1er complète l'arrêté préfectoral 95-11310 du 21 août 1995.</p>",
        "Les présentes dispositions sont applicables jusqu'au 31 décembre de l'année en cours "
        "et reconduites tacitement pendant toute la durée de la menace terroriste.",
        "L'annexe 1 de l'arrêté préfectoral du 23 novembre 2010 susvisé est modifiée comme suit.",
        "Arrêté transmis par mail le 06/03/2026 : BMO+M10+MOA+SCOP",
        "Envoyé le 13/09/2024 au BMO/MOA/M10",
        "Tout stationnement d'un véhicule en infraction avec cette interdiction est considéré "
        "comme gênant.",
        "Sont abrogés :<br>L'arrêté n° 2001-102 du 2 mars 2001<br>L'arrêté n° 2003-12 du 4 mai",
        "",
        None,
    ],
)
def test_a_text_that_changes_nothing_is_understood_without_effect(article):
    reading = read_text(article)
    assert reading.understood and reading.effects == ()


@pytest.mark.parametrize(
    "text",
    [
        "Les mesures du présent arrêté ne s'appliquent pas à la rue Raymond Losserand.",
        "Par dérogation à l'article 1er du présent arrêté les véhicules d'approvisionnement du "
        "marché sont autorisés à stationner le vendredi de 10 heures 30 à 20 heures.",
        "Dans la portion de voie, la circulation des véhicules dont la longueur excède 10 "
        "mètres est interdite.",
        "L'article 2 du présent arrêté est modifié : la rue Cadet est ajoutée.",
        # A neutral sentence does not cover the next one, nor the next line.
        "Les dispositions du présent arrêté abrogent et remplacent toutes les dispositions "
        "contraires antérieures. Toutefois ces dispositions ne sont pas applicables le dimanche.",
        "cycles, cyclomoteurs et motocyclettes sur le trottoir<br />Complète l'arrêté "
        "préfectoral n° 96-10651 du 2 mai 1996",
        "Attention cet arrêté est abrogé par le n° 2013P0806 du 22 août 2013",
        "Cet arrêté a été injecté dans GACS le 23/01/19. Seules les mesures et les "
        "localisations ont été saisies.",
        "les jours de marché de 2h00 à 17h00",
        "Ces dispositions sont applicables les dimanches de 10h00 à 14h00.",
        "Les rues de Belleville et des Pyrénées sont exclues de cette zone 30",
    ],
)
def test_a_text_that_says_more_is_not_understood(text):
    assert not read_text(text).understood


def test_the_mail_merge_clauses_are_read_apart_even_without_a_full_stop():
    text = (
        "A titre provisoire, Un sens unique de circulation est institué Rue Le Goff "
        "Toutefois ces dispositions ne sont pas applicables aux véhicules de secours"
    )
    assert clauses(text) == [
        "A titre provisoire, Un sens unique de circulation est institué Rue Le Goff",
        "Toutefois ces dispositions ne sont pas applicables aux véhicules de secours",
    ]


@pytest.mark.parametrize(
    "text, code",
    [
        (
            "Dans cette portion de voie, les cyclistes sont autorisés à circuler à contresens.",
            Code.AGAINST_THE_FLOW,
        ),
        (
            "L'arrêté préfectoral n° 89-10393 du 5 mai 1989 susvisé est modifié en ce sens que les "
            "vélos sont autorisés à circuler à double sens",
            Code.AGAINST_THE_FLOW,
        ),
        (
            "Dans cette portion de voie, les autobus, les taxis et les cycles sont autorisés à "
            "circuler à contre sens de la circulation générale",
            Code.AGAINST_THE_FLOW,
        ),
        (
            "Les cycles empruntant ces voies en sens inverse de la circulation générale sont "
            "tenus d'emprunter la piste cyclable",
            Code.AGAINST_THE_FLOW,
        ),
        ("1 place.s ZL permanente", Code.RESERVED_PLACES),
        ("sur 2 places.,", Code.RESERVED_PLACES),
        ('Quartier Vert " Censier "', Code.ZONE_NAME),
        ("sauf aux véhicules de secours", Code.EXEMPTION),
        (
            "L'arrêté n° 2013-01231 du 9 décembre 2013 susvisé est ainsi modifié : « 14 places » "
            "est remplacée par « 11 places ».",
            Code.AMENDMENT_WORDING,
        ),
    ],
)
def test_a_text_understood_carries_its_code(text, code):
    reading = read_text(text)
    assert reading.understood
    assert reading.codes() == {code}


def test_cycles_that_may_not_ride_against_the_flow_carry_no_exemption():
    reading = read_text(
        "Les cycles ne sont pas autorisés à circuler en sens inverse de la circulation générale."
    )
    assert reading.understood and reading.effects == ()


def test_a_blocking_formula_is_not_understood_even_if_a_formula_matches(monkeypatch):
    monkeypatch.setitem(free_text.BLOCKING_FORMULAS, "contrary", r"abrogent et remplacent")
    reading = read_text(
        "Les dispositions du présent arrêté abrogent et remplacent toutes les dispositions "
        "contraires antérieures."
    )
    assert not reading.understood and reading.blocking == reading.unread


# The act and its texts.


@pytest.mark.parametrize(
    "overrides",
    [
        {"a_next_version": "30252"},
        {"a_repealing_act": "39584"},
        {"a_repealed_by_clone": "28617"},
        {"a_repealed_by_multiple": "29523"},
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
    out = discard_superseded_regulations(make_frame(kept, dropped, second_location))
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
    assert discard_superseded_regulations(make_frame(make_row(a_title_html=title))).height == 1


@pytest.mark.parametrize(
    "overrides",
    [
        {"a_notes": "Partiellement abrogé par 2021P112184 concernant rue de Moscou"},
        {"a_articles": ["Les mesures du présent arrêté ne s'appliquent pas à la rue Cadet."]},
        # Exempting every measure of the act at once says more than one parameter can.
        {
            "a_articles": [
                "Toutefois ces dispositions ne sont pas applicables aux véhicules de secours."
            ]
        },
    ],
)
def test_an_act_whose_own_text_is_not_understood_is_dropped_whole(overrides, captured_logs):
    kept = make_row(a_file_id=1, a_identifier="2024P10001", l_file_id=11)
    dropped = make_row(a_file_id=2, a_identifier="2024P10002", m_file_id=20, **overrides)
    second_measure = make_row(a_file_id=2, a_identifier="2024P10002", m_file_id=21, l_file_id=22)
    second_measure.update(overrides)
    out = discard_unread_texts(make_frame(kept, dropped, second_measure))
    assert out["a_identifier"].to_list() == ["2024P10001"]
    assert any("Dropping 1 regulations whose free text" in line for line in captured_logs)


def test_an_act_with_neutral_texts_is_kept():
    row = make_row(
        a_notes="Envoyé le 13/09/2024 au BMO/MOA/M10",
        a_articles=["L'arrêté n° 80-16555 du 2 août 1980 est abrogé."],
    )
    assert discard_unread_texts(make_frame(row)).height == 1


def test_the_new_wording_of_another_act_is_its_business_in_an_article_not_in_a_paragraph():
    wording = (
        "Le deuxième alinéa de l'article 2 de l'arrêté du 21 avril 2019 susvisé est ainsi "
        'modifié : "- le trottoir côté impair de la rue du Cloître Notre-Dame"'
    )
    assert discard_unread_texts(make_frame(make_row(a_articles=[wording]))).height == 1
    assert discard_unread_texts(make_frame(make_row(m_paragraph=wording))).height == 0


# Measures.


def test_a_measure_whose_paragraph_or_notes_narrow_it_is_dropped():
    rows = [
        make_row(m_file_id=10, l_file_id=100),
        make_row(m_file_id=11, l_file_id=110, m_paragraph="les jours de marché de 2h00 à 17h00"),
        make_row(
            m_file_id=12, l_file_id=120, m_notes="durée spécifique 6 mois à compter du 3/03/2025"
        ),
        make_row(
            m_file_id=13,
            l_file_id=130,
            m_paragraph="Tout stationnement d'un véhicule en "
            "infraction avec cette interdiction est considéré comme gênant",
        ),
    ]
    assert kept_measures(*rows) == [10, 13]


def test_a_measure_note_that_amends_another_act_is_neutral_and_the_other_act_falls():
    note = (
        "Les dispositions de l'arrêté n° 89-10393 du 5 mai 1989 susvisé sont modifiées en ce "
        "qui concerne la portion de voie mentionnée au présent article."
    )
    cited = make_row(a_file_id=1, a_identifier="89-10393", l_file_id=11)
    citing = make_row(
        a_file_id=2, a_identifier="2019P16496", m_file_id=20, l_file_id=21, m_notes=note
    )
    df = make_frame(cited, citing)
    assert discard_superseded_regulations(df)["a_identifier"].to_list() == ["2019P16496"]
    assert discard_unread_texts(df).height == 2


@pytest.mark.parametrize(
    "paragraph, measure, kept",
    [
        ('Quartier Vert " Censier "', "zone 30", True),
        (
            'Il est institué une zone 30 dénommée "Saint-paul" délimité comme suit :',
            "zone 30",
            True,
        ),
        ('Quartier Vert " Censier "', "limitation de vitesse", False),
        ("cycles", "interdiction de stationnement", False),
    ],
)
def test_a_zone_is_named_by_its_own_paragraph_only(paragraph, measure, kept):
    row = make_row(m_type=measure, m_paragraph=paragraph)
    assert (discard_unread_texts(make_frame(row)).height == 1) is kept


@pytest.mark.parametrize(
    "paragraph",
    [
        "Toutefois ces dispositions ne sont pas applicables aux véhicules de secours.",
        "sauf aux véhicules de secours",
        "sauf aux véhicules de secours et aux véhicules des sapeurs pompiers",
    ],
)
def test_an_emergency_exemption_in_a_measure_text_becomes_the_measure_exemption(paragraph):
    row = make_row(m_type="circulation interdite", l_scope="Une section", m_paragraph=paragraph)
    out = pipeline(row).pipe(discard_unread_texts)
    assert out.height == 1
    assert exempted(out) == [["emergencyServices"]]


@pytest.mark.parametrize(
    "paragraph",
    [
        "sauf aux véhicules de livraison",
        "sauf aux véhicules de secours de 8h à 18h",
        "aux véhicules de secours",
    ],
)
def test_any_other_exemption_in_a_measure_text_drops_the_measure(paragraph):
    row = make_row(m_type="circulation interdite", m_paragraph=paragraph)
    assert pipeline(row).pipe(discard_unread_texts).height == 0


# The measure's article, generated from its fields by the mail merge.

GENERATED_LOCATION = {
    "m_type": "interdiction de stationnement",
    "m_opening": "Le stationnement est interdit",
    "m_option_formulas": [
        "Tout stationnement d'un véhicule en infraction avec cette interdiction est considéré "
        "comme",
        "",
        "",
    ],
    "m_params": [["caractère aggravant", "gênant"]],
    "l_road_name": "Rue de Picpus",
    "l_district": "12ème arrondissement",
    "l_point_number_complement": "au droit du",
    "l_point_house_number": "32",
    "l_reserved_places": "1",
    "l_reserved_space_type": "ZL permanente",
    "l_article_details": "1 place.s ZL permanente",
}
GENERATED_ARTICLE = (
    "A titre provisoire, Le stationnement est interdit Rue de Picpus, 12ème arrondissement, au "
    "droit du N°32, 1 place.s ZL permanente Tout stationnement d'un véhicule en infraction "
    "avec cette interdiction est considéré comme gênant"
)


def test_an_article_the_fields_explain_keeps_its_measure():
    row = make_row(**GENERATED_LOCATION, m_article=GENERATED_ARTICLE)
    assert discard_unread_texts(make_frame(row)).height == 1


@pytest.mark.parametrize(
    "addition",
    [
        " Cette mesure s'applique du 7 au 14 octobre 2026 de 9h00 à 16h00",
        " - Rue Lacroix, 17ème arrondissement, au droit du N°14",
        ", sur 10 mètres linéaires",
    ],
)
def test_an_article_saying_what_no_field_says_drops_its_measure(addition):
    row = make_row(**GENERATED_LOCATION, m_article=GENERATED_ARTICLE + addition)
    assert discard_unread_texts(make_frame(row)).height == 0


def test_a_location_an_earlier_rule_dropped_still_explains_the_article():
    raw = make_frame(
        make_row(**GENERATED_LOCATION, m_article=GENERATED_ARTICLE + " - Rue de Test, N°12"),
        make_row(l_file_id=101, l_road_name="Rue de Test", l_direction="vers l'est"),
    )
    assert discard_unread_texts(raw.head(1), all_rows=raw).height == 1


# Bicycles against the flow, pedestrian areas.

CYCLES = "Dans cette voie, les cycles sont autorisés à circuler à double sens."


def test_cycles_against_the_flow_exempt_bicycles_on_the_one_way_streets_of_the_act():
    one_way = make_row(m_type=ONE_WAY, l_scope="Une section", a_articles=[CYCLES])
    other = make_row(m_file_id=11, l_file_id=110, l_scope="Une section", a_articles=[CYCLES])
    out = pipeline(one_way, other).pipe(discard_unread_texts)
    assert exempted(out) == [["bicycle"], None]


def test_cycles_against_the_flow_in_an_act_without_one_way_street_change_nothing():
    row = make_row(m_type="zone 30", l_scope="Une section", m_paragraph=CYCLES)
    out = pipeline(row).pipe(discard_unread_texts)
    assert out.height == 1 and exempted(out) == [None]


def test_cycles_exempted_from_a_one_way_street_by_its_paragraph_ride_against_the_flow():
    row = make_row(
        m_type=ONE_WAY,
        l_scope="Une section",
        m_paragraph="Toutefois ces dispositions ne sont pas applicables aux cycles",
    )
    assert exempted(pipeline(row).pipe(discard_unread_texts)) == [["bicycle"]]


def test_other_vehicles_against_the_flow_are_exempted_on_the_one_way_streets_of_the_act():
    buses = "Les autobus, les taxis et les cycles sont autorisés à circuler à contresens."
    row = make_row(m_type=ONE_WAY, l_scope="Une section", a_articles=[buses])
    out = pipeline(row).pipe(discard_unread_texts).pipe(compute_vehicle_fields)
    assert out["vehicle_exempted_types"].to_list() == [["bicycle", "taxi", "other"]]
    assert out["vehicle_other_exempted_type_text"].to_list() == ["autobus"]


def test_cycles_bound_to_a_track_ride_against_the_flow_with_the_condition_in_words():
    track = (
        "Les cycles empruntant ces voies en sens inverse de la circulation générale sont "
        "tenus d'emprunter la piste cyclable."
    )
    row = make_row(m_type=ONE_WAY, l_scope="Une section", m_paragraph=track)
    out = pipeline(row).pipe(discard_unread_texts).pipe(compute_vehicle_fields)
    assert out["vehicle_exempted_types"].to_list() == [["bicycle", "other"]]
    assert out["vehicle_other_exempted_type_text"].to_list() == [
        "cycles tenus d'emprunter la piste cyclable"
    ]


ACCESS = (
    "La circulation des véhicules nécessaires à la desserte interne de cette aire piétonne est "
    "autorisée et limitée strictement aux catégories de véhicules suivants : - véhicules de "
    "secours ; - véhicules des riverains ; - véhicules des services publics utilisés pour "
    "l'exercice de leurs missions."
)


def test_the_closed_list_of_a_pedestrian_area_takes_the_place_of_the_local_access():
    row = make_row(m_type=PEDESTRIAN_AREA, l_scope="Une section", a_articles=[ACCESS])
    out = pipeline(row).pipe(discard_unread_texts).pipe(compute_vehicle_fields)
    assert out["vehicle_exempted_types"].to_list() == [
        ["emergencyServices", "localResident", "other"]
    ]
    assert out["vehicle_other_exempted_type_text"].to_list() == [
        "véhicules des services publics utilisés pour l'exercice de leurs missions"
    ]


def test_a_pedestrian_access_list_without_a_pedestrian_area_drops_the_act():
    assert discard_unread_texts(make_frame(make_row(a_articles=[ACCESS]))).height == 0


def test_a_pedestrian_access_list_with_a_time_drops_the_act():
    row = make_row(m_type=PEDESTRIAN_AREA, a_articles=[ACCESS + " - livraisons de 7h à 10h."])
    assert discard_unread_texts(make_frame(row)).height == 0


# Locations.


def test_a_location_text_that_narrows_the_measure_drops_the_location(captured_logs):
    df = make_frame(
        make_row(l_file_id=10),
        make_row(l_file_id=11, l_from_location_complement="<p>les mardis de 14h30 à 16h30</p>"),
        make_row(
            a_identifier="2024P10002",
            a_file_id=2,
            m_file_id=20,
            l_file_id=20,
            l_to_location_complement="dans la contre-allée",
        ),
        make_row(l_file_id=12, l_notes="SUR LA PISTE CYCLABLE"),
        make_row(l_file_id=13, l_article_details="1 place.s payant du 28/09 au 5/10"),
    )
    kept = discard_unread_texts(df)
    assert kept["l_file_id"].to_list() == [10]
    assert any("Dropping 4 locations whose free text" in line for line in captured_logs)


@pytest.mark.parametrize(
    "complement, point_number",
    [
        (None, "12"),
        ("", "12"),
        ("Bis,", "12"),
        (", BIS", "12"),
        ("et jusqu'à", "12"),
        ("sur toute la longueur de la voie.", "12"),
        ("Sur toutes les places.", "12"),
        ("& n°13", "13"),
        ("56-56bis", "56"),
        ("n° 34 à 36 ter", "34 à 36"),
        ("<!--[if gte mso 9]><xml><w:Word>Normal</w:Word></xml><![endif]-->", "12"),
    ],
)
def test_a_location_complement_that_says_nothing_keeps_the_location(complement, point_number):
    df = make_frame(
        make_row(l_point_location_complement=complement, l_point_house_number=point_number)
    )
    assert discard_unread_texts(df).height == 1


@pytest.mark.parametrize(
    "complement",
    [
        "de 8h à 18h",
        "8h-18h",
        "sur 1 place, sauf aux GIG/GIC",
        "côté pair",
        "du 02/11/2026 au 31/08/2027",
        # Numbers the bounds do not carry: a second place, or a day without its month.
        "n° 21 et du n° 47",
        "le 25",
        "du 1 au 15",
    ],
)
def test_a_short_complement_that_narrows_the_measure_drops_the_location(complement):
    df = make_frame(make_row(l_point_location_complement=complement, l_point_house_number="21"))
    assert discard_unread_texts(df).height == 0


def test_a_section_complement_may_repeat_the_numbers_of_both_bounds():
    section = make_row(
        l_scope="Une section",
        l_point_house_number=None,
        l_from_house_number="12",
        l_to_house_number="20",
        l_from_location_complement="du n° 12 au n° 20",
    )
    assert discard_unread_texts(make_frame(section)).height == 1


@pytest.mark.parametrize(
    "column, text", [("l_point_location_complement", "sur 2 places.,"), ("l_article_details", "13")]
)
def test_places_written_in_a_text_keep_a_point_but_not_a_section(column, text):
    point = make_row(l_file_id=10, **{column: text})
    section = make_row(
        l_file_id=11,
        l_scope="Une section",
        l_point_house_number=None,
        l_from_house_number="12",
        l_to_house_number="20",
        **{column: text},
    )
    assert discard_unread_texts(make_frame(point, section))["l_file_id"].to_list() == [10]


@pytest.mark.parametrize(
    "overrides",
    [
        {"l_reserved_space_type": "Livraisons"},
        {"l_reserved_places": "3"},
        {"l_reserved_length": "10"},
        {"l_displayed_length": "sur 10 ml"},
    ],
)
def test_a_section_narrowed_by_its_details_is_dropped(overrides: dict[str, Any]):
    section = {"l_scope": "Une section", "l_point_house_number": None}
    df = make_frame(make_row(l_file_id=100), make_row(l_file_id=101, **section, **overrides))
    assert discard_location_details(df)["l_file_id"].to_list() == [100]


def test_reserved_places_and_a_length_on_a_point_keep_it():
    df = make_frame(
        make_row(l_file_id=100, l_reserved_places="1", l_reserved_space_type="payant"),
        make_row(l_file_id=101, l_displayed_length="sur 10 ml"),
    )
    assert discard_location_details(df)["l_file_id"].to_list() == [100, 101]


# Raw rows.


def test_each_article_reaches_its_own_regulation_or_measure():
    tables = {
        "1100": [{"FileId": 1, "1101": "2024P10001", "1108": "Permanent", "1107": "En vigueur"}],
        "1200": [{"FileId": 10, "1101_fid": 1, "1202": "stationnement réservé"}],
        "2200": [
            {"1101_fid": 1, "2205": "Sont abrogés : …"},
            {"1101_fid": 1, "2205": ""},
            {"1101_fid": 2, "2205": "Article d'un autre arrêté"},
        ],
        "3200": [{"1202_fid": 10, "3204": "Des emplacements sont réservés"}],
        # The nightly copy is no longer read.
        "1400": [{"1101_fid": 1, "1402": "Article complémentaire", "1405": "Copie périmée"}],
    }
    row = build_raw_dataframe(tables).row(0, named=True)
    assert row["a_articles"] == ["Sont abrogés : …"]
    assert row["m_article"] == "Des emplacements sont réservés"


# Acts cited by another act.


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
    out = discard_superseded_regulations(make_frame(cited, citing))
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
    assert discard_superseded_regulations(make_frame(cited, citing)).height == 2


def test_an_act_amending_itself_is_not_dropped():
    row = make_row(
        a_identifier="2014P0160",
        a_title_html="Arrêté n° 2014P0160 modifiant les règles de stationnement",
    )
    assert discard_superseded_regulations(make_frame(row)).height == 1


# Formulas repeated across records.


def acts(count: int, **overrides: Any) -> list[dict[str, Any]]:
    """`count` acts with one measure and one location each, all with the same `overrides`."""
    return [
        make_row(
            a_file_id=i,
            a_identifier=f"2024P{10000 + i}",
            m_file_id=100 + i,
            l_file_id=1000 + i,
            **overrides,
        )
        for i in range(1, count + 1)
    ]


DEAD_END = "Une mise en impasse est instaurée rue de Test au droit du n° 3."
CONTRARY_PROVISIONS = (
    "Les dispositions du présent arrêté abrogent et remplacent toutes les dispositions "
    "contraires antérieures."
)


def test_a_formula_not_understood_in_more_than_ten_acts_is_logged(captured_logs):
    alerts: list[str] = []
    out = discard_unread_texts(make_frame(*acts(11, a_articles=[DEAD_END])), alerts)
    assert out.height == 0
    assert any("Regulation texts: 11 records repeat a formula" in line for line in captured_logs)
    assert alerts == []


def test_ten_acts_are_not_enough_to_report_a_formula(captured_logs):
    discard_unread_texts(make_frame(*acts(10, a_articles=[DEAD_END])), [])
    assert not any("repeat a formula" in line for line in captured_logs)


def test_a_formula_compares_without_its_digits(captured_logs):
    rows = [
        {**row, "a_articles": [f"Une mise en impasse est instaurée au droit du n° {i}."]}
        for i, row in enumerate(acts(11), start=1)
    ]
    discard_unread_texts(make_frame(*rows), [])
    assert any("11 records repeat a formula" in line for line in captured_logs)


def test_beyond_a_hundred_acts_the_team_is_alerted():
    alerts: list[str] = []
    discard_unread_texts(make_frame(*acts(101, a_articles=[DEAD_END])), alerts)
    assert alerts == [
        "texte libre présent à de très nombreuses reprises (textes des arrêtés : "
        "1 formule au-delà de 100) ; à prendre en compte dans le code"
    ]


def test_a_neutral_formula_is_never_reported(captured_logs):
    alerts: list[str] = []
    rows = acts(101, a_articles=[CONTRARY_PROVISIONS])
    assert discard_unread_texts(make_frame(*rows), alerts).height == 101
    assert not any("repeat a formula" in line for line in captured_logs)
    assert alerts == []


def test_a_blocking_formula_drops_the_act_without_being_reported(monkeypatch, captured_logs):
    monkeypatch.setitem(
        free_text.BLOCKING_FORMULAS, "contrary provisions", r"abrogent et remplacent"
    )
    alerts: list[str] = []
    rows = acts(101, a_articles=[CONTRARY_PROVISIONS])
    assert discard_unread_texts(make_frame(*rows), alerts).height == 0
    assert not any("repeat a formula" in line for line in captured_logs)
    assert alerts == []


def test_a_recurring_measure_text_is_logged(captured_logs):
    rows = acts(11, m_paragraph="les caravanes et auto-caravanes au droit du n° 3")
    assert discard_unread_texts(make_frame(*rows), []).height == 0
    assert any("Measure texts: 11 records repeat a formula" in line for line in captured_logs)


@pytest.mark.parametrize(
    "article, cited_service, cited",
    [
        ("L'arrêté municipal n° 2011-104 est abrogé.", "Préfecture de Police", False),
        ("L'arrêté municipal n° 2011-104 est abrogé.", "Mairie de Paris", True),
        # The prefect's older acts came over to the city's services in 2017.
        ("L'arrêté préfectoral n° 2011-00104 est abrogé.", "Mairie de Paris - SR", True),
        ("L'arrêté municipal n° 2011-104 est abrogé.", "Conjoint", True),
        ("L'arrêté n° 2011-104 est abrogé.", "Préfecture de Police", True),
        (
            "Les arrêtés préfectoraux n° 98-12005 et municipal n° 2011-104 sont abrogés.",
            "Préfecture de Police",
            False,
        ),
    ],
)
def test_a_citation_naming_the_city_or_the_prefect_reaches_only_its_acts(
    article, cited_service, cited
):
    target = make_row(a_file_id=1, a_identifier="2011-00104", a_service=cited_service)
    citing = make_row(
        a_file_id=2, a_identifier="2021P112857", m_file_id=20, l_file_id=21, a_articles=[article]
    )
    out = discard_superseded_regulations(make_frame(target, citing))
    assert (out.height == 1) is cited
