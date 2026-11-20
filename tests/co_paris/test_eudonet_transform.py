import re
from datetime import date, datetime
from typing import Any

import polars as pl
import pytest

from api.dia_log_client.models import (
    MeasureTypeEnum,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodySubject,
)
from canonical.regulation_measure import RegulationMeasure
from integrations.co_paris.eudonet import location_filters
from integrations.co_paris.eudonet.data_source_integration import (
    DataSourceIntegration,
    add_split_order,
)
from integrations.co_paris.eudonet.location_filters import (
    judge_bound_qualifiers,
    judge_copied_locations,
    judge_points_beyond_parking,
    judge_refused_locations,
    load_refused_locations,
)
from integrations.co_paris.eudonet.measures import (
    PEDESTRIAN_OR_CYCLE_MEASURES,
    add_pedestrian_area_speed,
)
from integrations.co_paris.eudonet.published_elsewhere import (
    index_by_eudonet_number,
    load_frozen_elsewhere,
)
from integrations.co_paris.eudonet.raw_rows import (
    build_raw_dataframe,
)
from integrations.co_paris.eudonet.regulations import (
    IDENTIFIER_PREFIX,
    keep_authority,
    title_says_repealed,
)
from integrations.co_paris.eudonet.vehicles import (
    EXEMPTION_IN_TITLE_ONLY,
    PEDESTRIAN_OR_CYCLE_VEHICLES,
)
from integrations.co_paris.eudonet.verdicts import (
    BOUND_QUALIFIER,
    COPIED_LOCATION,
    POINT_BEYOND_PARKING,
    REFUSED_LOCATION,
)
from integrations.co_paris.eudonet.vocabulary import (
    MEASURE_TYPE_BY_LABEL,
    PERMANENT_SUBJECT_TEXT,
    UNKNOWN_REASON_TEXT,
)
from payloads.measures import build_vehicle_set
from tests.co_paris.acts import FIXTURE_EXTRACTED_ON, everything, make_frame, make_row, pipeline
from tests.co_paris.eudonet_fixture import load_fixture
from tests.co_paris.steps import (
    compute_location_fields,
    compute_measure_fields,
    compute_period_fields,
    compute_regulation_fields,
    compute_vehicle_fields,
    discard_bound_qualifiers,
    discard_measures_out_of_model,
    discard_published_elsewhere,
    drop_refused,
    everyone,
    orient_one_way_streets,
)
from toolkit.rules.apply import apply_rules
from toolkit.rules.vehicles import THRESHOLD_MISSING
from toolkit.transforms.titles import ELLIPSIS as TITLE_ELLIPSIS
from toolkit.transforms.titles import TITLE_MAX_LENGTH


@pytest.fixture
def raw():
    tables = load_fixture()
    return build_raw_dataframe(tables, everything(tables))


@pytest.fixture
def clean(raw):
    return (
        raw.pipe(compute_regulation_fields)
        .pipe(compute_measure_fields)
        .pipe(discard_measures_out_of_model)
        .pipe(compute_vehicle_fields)
        .pipe(compute_period_fields, FIXTURE_EXTRACTED_ON)
    )


def test_the_identifier_is_the_eudonet_number_behind_a_stable_prefix(clean):
    identifiers = set(clean.get_column("regulation_identifier"))
    assert "PARIS-EUDO-2021T113851" in identifiers
    assert "PARIS-EUDO-1979-16282" in identifiers
    assert all(value.startswith(IDENTIFIER_PREFIX) for value in identifiers)
    # 60 characters is the API limit on `identifier`.
    assert max(len(value) for value in identifiers) <= 60


def test_the_identifier_is_stable_across_two_runs(raw):
    first = compute_regulation_fields(raw).get_column("regulation_identifier").to_list()
    second = compute_regulation_fields(raw).get_column("regulation_identifier").to_list()
    assert first == second


@pytest.mark.parametrize(
    "state, kept_permanent, kept_temporary",
    [
        ("En vigueur", True, True),
        ("Publié", False, True),
        ("Signé", False, True),
        ("Non signé", False, False),
        ("Périmé", False, False),
        ("Abrogé", False, False),
        ("Abrogation en cours", False, False),
        (None, False, False),
    ],
)
def test_only_enforceable_states_survive(state, kept_permanent, kept_temporary):
    permanent = make_row(a_state=state)
    temporary = make_row(a_type="Temporaire", a_state=state, a_end_date=date(2026, 12, 1))
    assert compute_regulation_fields(make_frame(permanent)).height == kept_permanent
    assert compute_regulation_fields(make_frame(temporary)).height == kept_temporary


def test_the_repealed_and_the_expired_of_the_fixture_are_gone(clean):
    identifiers = set(clean.get_column("a_identifier"))
    assert "1982-10803" not in identifiers  # Abrogé
    assert "2022T18442" not in identifiers  # Périmé, ended 2026-03-01


def test_a_temporary_regulation_still_to_start_is_kept():
    row = make_row(
        a_type="Temporaire",
        a_reason="Travaux",
        a_start_date=date(2026, 10, 1),
        a_end_date=date(2026, 11, 30),
    )
    assert pipeline(row).height == 1


def test_a_temporary_regulation_that_ended_reaches_the_rules_with_its_end():
    row = make_row(
        a_type="Temporaire",
        a_reason="Travaux",
        a_start_date=date(2026, 1, 1),
        a_end_date=date(2026, 9, 7),
    )

    assert pipeline(row).get_column("period_end_date").to_list() == ["2026-09-07T23:59:59+02:00"]


@pytest.mark.parametrize(
    "dates",
    [{"a_end_date": None}, {"a_start_date": None, "a_end_date": date(2026, 11, 30)}],
    ids=["no end: nothing would close it", "no start: the write date would stand in"],
)
def test_a_temporary_regulation_needs_both_its_dates(dates):
    """A temporary period has a start and an end."""
    row = make_row(a_type="Temporaire", a_reason="Travaux", **dates)

    missing = pl.col("period_start_date").is_null() | pl.col("period_end_date").is_null()
    assert pipeline(row).select(missing).to_series().to_list() == [True]


def test_a_regulation_without_an_order_number_is_dropped_not_the_run():
    rows = make_frame(make_row(), make_row(a_file_id=2, a_identifier=None))
    kept = compute_regulation_fields(rows)
    assert kept.get_column("a_file_id").to_list() == [1]


def test_the_title_is_stripped_of_its_html(clean):
    title = clean.filter(pl.col("a_identifier") == "2021T113851").get_column("regulation_title")[0]
    assert title == (
        "Arrêté n° 2021T113851 — Modifiant, à titre provisoire, les règles de stationnement "
        "gênant la circulation générale rue Mathis à Paris 19ème."
    )
    assert "&nbsp;" not in title and "<" not in title


def test_the_title_survives_a_word_paste():
    row = make_row(
        a_title_html="<!-- comment --><p style='x'>Réglementant&nbsp;le&nbsp;stationnement</p>"
    )
    title = compute_regulation_fields(make_frame(row)).get_column("regulation_title")[0]
    assert title == "Arrêté n° 2024P10001 — Réglementant le stationnement"


@pytest.mark.parametrize(
    "raw_title, cleaned",
    [
        (
            'dans le 1</font><font face="Marianne">4</font><sup>e</sup> arrondissement',
            "dans le 14e arrondissement",
        ),
        ("C<strong>oncernant la fermeture</strong>", "Concernant la fermeture"),
        ("Paris 16<sup>ème </sup>arrondissement", "Paris 16ème arrondissement"),
        ("rue Bardinet<br />à Paris</p><p>suite", "rue Bardinet à Paris suite"),
    ],
)
def test_formatting_tags_do_not_cut_words(raw_title, cleaned):
    row = make_row(a_title_html=raw_title)
    title = compute_regulation_fields(make_frame(row)).get_column("regulation_title")[0]
    assert title == f"Arrêté n° 2024P10001 — {cleaned}"


@pytest.mark.parametrize(
    "title, left_out",
    [
        ("ABROGE_par_arrêt_conjoint_2026P0050_portant création d'une aire piétonne", True),
        (
            "Voiries Souterraines des Halles à PARIS Centre / Abrogé par l'arrêté n° 2025P14099",
            True,
        ),
        ("rue des Ternes, à Paris 17ème arrondissement article 1er abrogé", True),
        ("abrogeant l'arrêté n° 2014-01031 du 16 décembre 2014", False),
        ("régularisation de l'arrêté n° 2010-00434 abrogé par l'arrêté n° 2014P0193", False),
        ("portant abrogation de l'arrêté préfectoral n° 95-11067", False),
    ],
)
def test_a_regulation_whose_title_says_it_is_repealed_is_left_out(title, left_out):
    assert title_says_repealed(title) is left_out


@pytest.mark.parametrize("raw_title", [None, "", "   ", "<p></p>", "&nbsp;"])
def test_an_empty_title_is_built_rather_than_blocking(raw_title):
    """A title is never a reason to drop a regulation."""
    row = make_row(a_title_html=raw_title)
    title = compute_regulation_fields(make_frame(row)).get_column("regulation_title")[0]
    assert title == "Arrêté n° 2024P10001"


@pytest.mark.parametrize("length, truncated", [(200, False), (400, True)])
def test_the_title_is_truncated_to_the_api_limit(length, truncated):
    row = make_row(a_title_html="a" * length)
    title = compute_regulation_fields(make_frame(row)).get_column("regulation_title")[0]
    assert len(title) <= TITLE_MAX_LENGTH
    if truncated:
        assert len(title) == TITLE_MAX_LENGTH
        assert title.endswith(TITLE_ELLIPSIS)


@pytest.mark.parametrize(
    "reason, subject, text",
    [
        ("Travaux", PostApiRegulationsAddBodySubject.ROADMAINTENANCE.value, "Travaux"),
        ("Evènement", PostApiRegulationsAddBodySubject.EVENT.value, "Evènement"),
        ("Manifestation", PostApiRegulationsAddBodySubject.EVENT.value, "Manifestation"),
        ("Cinéma", PostApiRegulationsAddBodySubject.EVENT.value, "Cinéma"),
        ("Pollution", PostApiRegulationsAddBodySubject.OTHER.value, "Pollution"),
        ("Déménagement", PostApiRegulationsAddBodySubject.OTHER.value, "Déménagement"),
        ("Autre", PostApiRegulationsAddBodySubject.OTHER.value, "Autre"),
        ("couloir de bus", PostApiRegulationsAddBodySubject.OTHER.value, "couloir de bus"),
        (None, PostApiRegulationsAddBodySubject.OTHER.value, UNKNOWN_REASON_TEXT),
        ("", PostApiRegulationsAddBodySubject.OTHER.value, UNKNOWN_REASON_TEXT),
    ],
)
def test_the_subject_comes_from_the_reason(reason, subject, text):
    row = make_row(
        a_type="Temporaire",
        a_reason=reason,
        a_start_date=FIXTURE_EXTRACTED_ON,
        a_end_date=date(2026, 12, 1),
    )
    df = compute_regulation_fields(make_frame(row))
    assert df.get_column("regulation_subject")[0] == subject
    assert df.get_column("regulation_other_category_text")[0] == text


def test_a_permanent_regulation_says_so_in_its_subject():
    df = compute_regulation_fields(make_frame(make_row()))
    assert df.get_column("regulation_subject")[0] == PostApiRegulationsAddBodySubject.OTHER.value
    assert df.get_column("regulation_other_category_text")[0] == PERMANENT_SUBJECT_TEXT
    assert (
        df.get_column("regulation_category")[0]
        == PostApiRegulationsAddBodyCategory.PERMANENTREGULATION.value
    )


def test_the_other_category_text_stays_within_a_hundred_characters():
    row = make_row(a_type="Temporaire", a_reason="R" * 200, a_end_date=date(2026, 12, 1))
    df = compute_regulation_fields(make_frame(row))
    assert len(df.get_column("regulation_other_category_text")[0]) == 100


def test_no_document_url_is_invented(clean):
    assert clean.get_column("regulation_document_url").null_count() == clean.height


def test_an_unknown_regulation_type_is_dropped_not_the_run(captured_logs):
    rows = make_frame(
        make_row(), make_row(a_file_id=2, a_identifier="2024P10002", a_type="Provisoire")
    )
    kept = compute_regulation_fields(rows)
    assert kept.get_column("a_identifier").to_list() == ["2024P10001"]
    assert any("unknown to catalog 1108" in line and "Provisoire" in line for line in captured_logs)


@pytest.mark.parametrize("label, expected", sorted(MEASURE_TYPE_BY_LABEL.items()))
def test_every_label_of_catalog_1202_maps_as_decided(label, expected):
    params = [["véhicule concerné (1)", "à tous les véhicules"]]
    if label == "limitation de vitesse":
        params = [["valeur de la vitesse", "à 30 km/h"]]
    row = make_row(
        m_type=label,
        m_params=params,
        l_direction="dans les deux sens" if label == "sens interdit (ou sens unique)" else None,
    )
    df = discard_measures_out_of_model(
        compute_measure_fields(compute_regulation_fields(make_frame(row)))
    )
    if expected is None:
        assert df.height == 0
    else:
        assert df.get_column("measure_type_").to_list() == [expected]


def test_the_catalog_table_is_complete():
    """32 values in catalog `1202` on 2026-09-08; a 33rd has to be qualified by hand."""
    assert len(MEASURE_TYPE_BY_LABEL) == 32


def test_an_unknown_measure_label_not_understood_drops_its_act_with_a_warning(captured_logs):
    """Paris can enrich the catalog: a new value is read as a free text, never guessed, nor
    stops the run; not understood, what it does to the act's other measures is not known."""
    unknown = make_row(m_file_id=11, m_type="interdiction de klaxonner", l_file_id=None)
    beside = make_row()
    other_act = make_row(a_file_id=2, a_identifier="2024P10002", m_file_id=20, l_file_id=200)

    df = compute_measure_fields(compute_regulation_fields(make_frame(unknown, beside, other_act)))

    assert df.get_column("m_file_id").to_list() == [20]
    dropped = [line for line in captured_logs if "unknown to catalog 1202" in line]
    assert len(dropped) == 1
    assert "'interdiction de klaxonner': 1" in dropped[0]


def test_an_unknown_measure_label_that_says_nothing_drops_only_its_measure():
    """Read as a free text and understood as saying nothing to publish ("pose de barrières"),
    the label leaves the act's other measures as they are: its own is not published."""
    unknown = make_row(m_file_id=11, m_type="pose de barrières", l_file_id=110)
    beside = make_row()
    df = pipeline(unknown, beside)
    assert df.get_column("m_file_id").to_list() == [10]


def test_measures_outside_the_model_are_read_then_dropped_and_counted(raw, captured_logs):
    kept = compute_measure_fields(compute_regulation_fields(raw))
    assert "Mesure libre" in kept.get_column("m_type").to_list()
    discard_measures_out_of_model(kept)
    dropped = [line for line in captured_logs if "without a DiaLog equivalent" in line]
    assert len(dropped) == 1
    assert "'Mesure libre': 1" in dropped[0]


def test_rows_without_any_measure_are_dropped_and_counted(captured_logs):
    df = compute_measure_fields(
        compute_regulation_fields(make_frame(make_row(m_type=None, m_params=None)))
    )
    assert df.height == 0
    assert any("rows without any measure" in line for line in captured_logs)


@pytest.mark.parametrize(
    "value, expected", [("à 30 km/h", 30), ("30 km/h", 30), ("à 50 km/h", 50), ("50 km/h", 50)]
)
def test_the_speed_is_read_from_its_parameter(value, expected):
    row = make_row(m_type="limitation de vitesse", m_params=[["valeur de la vitesse", value]])
    df = compute_measure_fields(compute_regulation_fields(make_frame(row)))
    assert df.get_column("measure_max_speed").to_list() == [expected]


@pytest.mark.parametrize("params", [None, [["valeur de la vitesse", ""]]])
def test_a_speed_limitation_without_a_speed_is_dropped(params, captured_logs):
    """30 or 50 is a decision, not a default."""
    row = make_row(m_type="limitation de vitesse", m_params=params)
    df = compute_measure_fields(compute_regulation_fields(make_frame(row)))
    assert df.height == 0
    assert any("without a readable 'valeur de la vitesse'" in line for line in captured_logs)


def test_zone_30_carries_its_speed_in_its_name():
    row = make_row(m_type="zone 30", m_params=None)
    df = compute_measure_fields(compute_regulation_fields(make_frame(row)))
    assert df.get_column("measure_type_").to_list() == [MeasureTypeEnum.SPEEDLIMITATION.value]
    assert df.get_column("measure_max_speed").to_list() == [30]


def test_no_speed_leaks_onto_another_measure_type():
    row = make_row(
        m_type="interdiction de stationnement", m_params=[["valeur de la vitesse", "à 30 km/h"]]
    )
    df = compute_measure_fields(compute_regulation_fields(make_frame(row)))
    assert df.get_column("measure_max_speed").to_list() == [None]


def test_a_dead_end_is_out_of_the_model(captured_logs):
    """A dead end closes one end of the street, not the street."""
    assert pipeline(make_row(m_type="mise en impasse")).height == 0
    assert any("without a DiaLog equivalent" in line for line in captured_logs)


def one_way_section(**overrides: Any) -> dict[str, Any]:
    defaults = {
        "m_type": "sens interdit (ou sens unique)",
        "l_scope": "Une section",
        "l_road_name": "Rue Blanche",
        "l_district": "9ème arrondissement",
        "l_point_house_number": None,
        "l_from_road_name": "Rue Jean-Baptiste Pigalle",
        "l_to_road_name": "Rue Saint-Lazare",
        "l_from_number_complement": "depuis",
        "l_to_number_complement": "jusqu'à",
    }
    return make_row(**{**defaults, **overrides})


def located(row: dict[str, Any]) -> pl.DataFrame:
    return compute_location_fields(discard_bound_qualifiers(pipeline(row)))


@pytest.mark.parametrize("to_complement", ["jusqu'à", "vers"])
def test_a_one_way_street_from_a_to_b_forbids_entry_from_b_to_a(to_complement):
    """« Un sens unique est institué rue Blanche, depuis rue Pigalle jusqu'à rue Saint-Lazare »."""
    out = orient_one_way_streets(located(one_way_section(l_to_number_complement=to_complement)))
    assert out["measure_type_"].to_list() == [MeasureTypeEnum.NOENTRY.value]
    assert out["location_direction"].to_list() == ["B_TO_A"]
    assert out["location_from_road_name"].to_list() == ["Rue Jean-Baptiste Pigalle"]


@pytest.mark.parametrize(
    "overrides",
    [
        # The form's default: the bounds say where, not which way.
        {"l_from_number_complement": "au droit du", "l_to_number_complement": "et le"},
        {
            "l_from_number_complement": "dans sa partie comprise entre",
            "l_to_number_complement": "et le",
        },
        {"l_scope": "La totalité de la voie"},
    ],
)
def test_a_one_way_street_that_does_not_say_which_way_is_dropped(overrides, captured_logs):
    assert orient_one_way_streets(located(one_way_section(**overrides))).height == 0
    assert any("do not say which way" in line for line in captured_logs)


def test_orienting_leaves_the_other_measures_both_ways():
    out = orient_one_way_streets(located(make_row()))
    assert out["location_direction"].to_list() == ["BOTH"]


def test_the_common_one_way_freeze_is_lifted_for_paris_only():
    assert "ONE_DIRECTION" in DataSourceIntegration.disabled_rules


def test_a_filled_time_slot_drops_the_measure(captured_logs):
    """A slot like "les dimanches et jours fériés" is not expressible, so the measure goes."""
    row = make_row(
        m_params=[["Jours et Horaires", "de 20h00 à 7h00 ainsi que les dimanches et jours fériés"]]
    )
    assert pipeline(row).height == 0
    assert any("'Jours et Horaires'" in line for line in captured_logs)


def test_an_empty_time_slot_is_no_time_slot():
    assert pipeline(make_row(m_params=[["Jours et Horaires", ""]])).height == 1


def test_a_filled_lane_drops_the_measure(captured_logs):
    """A closure of the bus lane alone must not close the whole street."""
    row = make_row(
        m_type="circulation interdite",
        m_params=[
            ["véhicule concerné (3)", "à tous les véhicules"],
            [
                "voie concernée",
                "sur la voie unidirectionnelle réservée aux véhicules de transports en commun",
            ],
        ],
    )
    assert pipeline(row).height == 0
    assert any("'voie concernée'" in line for line in captured_logs)


def test_an_empty_lane_is_no_lane():
    assert pipeline(make_row(m_params=[["voie concernée", ""]])).height == 1


def test_every_location_of_a_measure_shares_its_group_key():
    """One Eudonet measure is one DiaLog measure, whatever its number of locations."""
    df = pipeline(make_row(l_file_id=1), make_row(l_file_id=2), make_row(m_file_id=11))
    assert df.get_column("measure_group_key").to_list() == ["10", "10", "11"]
    assert DataSourceIntegration.group_locations_by_measure


def test_slices_follow_the_eudonet_order_whatever_the_order_read():
    """Cut above the ceiling, a regulation keeps the same rows in each slice every night."""
    pairs = ((11, 3), (10, 2), (11, 1), (10, 4))
    rows = [make_row(m_file_id=measure, l_file_id=location) for measure, location in pairs]
    df = add_split_order(pipeline(*rows)).sort("regulation_split_order")
    assert df.select("m_file_id", "l_file_id").rows() == [(10, 2), (10, 4), (11, 1), (11, 3)]


def test_a_pedestrian_area_is_a_ban_with_local_access_and_a_walking_pace_twin():
    """`noEntry` + `desserteLocale` + Paris's own exemptions, and 5 km/h for all."""
    row = make_row(
        m_type="aire piétonne",
        m_params=[
            [
                "Dérogations véhicules",
                "cycles ; véhicules d'intérêt général prioritaires"
                " ou bénéficiant de facilités de passage",
            ]
        ],
    )
    df = add_pedestrian_area_speed(pipeline(row)).sort("measure_type_")
    assert df.get_column("measure_type_").to_list() == [
        MeasureTypeEnum.NOENTRY.value,
        MeasureTypeEnum.SPEEDLIMITATION.value,
    ]
    assert df.get_column("vehicle_exempted_types").to_list() == [
        ["desserteLocale", "bicycle", "emergencyServices"],
        None,
    ]
    assert df.get_column("measure_max_speed").to_list() == [None, 5]
    assert df.get_column("vehicle_all_vehicles").to_list() == [True, True]
    assert df.get_column("measure_group_key").to_list() == ["10", "10-V5"]
    assert df.get_column("regulation_identifier").n_unique() == 1


@pytest.mark.parametrize(
    "formulas, kept",
    [
        (["", "", ""], True),
        (["Toutefois ces dispositions ne sont pas applicables", "", ""], True),
        (["", "Ces dispositions sont applicables", ""], True),
        (["", "Ces dispositions sont suspendues", ""], False),
        # Known in first place only: the third formula is never anything but the exemption one.
        (["", "", "Ces dispositions sont applicables"], False),
    ],
)
def test_an_unknown_formula_before_an_optional_parameter_drops_the_measure(formulas, kept):
    out = make_frame(make_row(m_option_formulas=formulas)).pipe(compute_measure_fields)
    assert (out.height == 1) is kept


def test_a_pedestrian_area_keyed_at_15_kmh_gets_a_15_kmh_twin():
    row = make_row(
        m_type="aire piétonne", a_key_1="STATIONNEMENT GENANT- LIMITATION VITESSE 15km/h"
    )
    twin = add_pedestrian_area_speed(pipeline(row)).filter(
        pl.col("measure_type_") == MeasureTypeEnum.SPEEDLIMITATION.value
    )
    assert twin.get_column("measure_max_speed").to_list() == [15]
    assert twin.get_column("measure_group_key").to_list() == ["10-V15"]


@pytest.mark.parametrize(
    "authority, kept",
    [
        (None, {"2024P10001", "2024P10002", "2024P10003"}),
        ("city", {"2024P10001", "2024P10003"}),
        ("prefecture", {"2024P10002"}),
    ],
)
def test_each_authority_keeps_its_acts(authority, kept):
    """A regulation with no service is not the prefecture's."""
    df = make_frame(
        make_row(),
        make_row(a_identifier="2024P10002", a_service="Préfecture de Police"),
        make_row(a_identifier="2024P10003", a_service=None),
    )
    assert set(keep_authority(df, authority)["a_identifier"]) == kept


def test_the_number_behind_another_channel_identifier_is_read_without_normalising():
    """`Paris_` is cut, a bare number is taken whole, ours are skipped."""
    numbers = index_by_eudonet_number(
        [
            ("Paris_2024P12345", "Paris (préfecture)"),
            ("paris_2025P12924", "Paris (préfecture)"),
            ("2024T11570", "Paris"),
            (f"{IDENTIFIER_PREFIX}2024T10001", "Paris"),
        ]
    )
    assert numbers == {
        "2024P12345": "Paris (préfecture): Paris_2024P12345",
        "2025P12924": "Paris (préfecture): paris_2025P12924",
        "2024T11570": "Paris: 2024T11570",
    }


def test_the_frozen_identifiers_of_the_prefecture_channel_are_readable():
    frozen = load_frozen_elsewhere()
    numbers = index_by_eudonet_number(frozen)
    assert len(numbers) == len(frozen) == 100
    assert all(re.fullmatch(r"\d{4}P\d+", number) for number in numbers)


def test_a_number_another_channel_published_is_not_created_again():
    """One act, one entry in DiaLog: the regulation already in place stays."""
    df = make_frame(
        make_row(),
        make_row(a_file_id=2, a_identifier="2024T11570"),
        make_row(a_file_id=3, a_identifier="2024P12345"),
    )
    numbers = index_by_eudonet_number(
        [("Paris_2024P12345", "Paris (préfecture)"), ("2024T11570", "Paris")]
    )
    kept = discard_published_elsewhere(df, numbers)
    assert set(kept["a_identifier"]) == set(df["a_identifier"]) - {"2024T11570", "2024P12345"}
    assert discard_published_elsewhere(df, None).height == df.height


def test_a_single_point_carries_only_a_parking_ban():
    """A ban, a speed or a gauge stands on a stretch of road, not on a house number."""
    df = make_frame(
        make_row(a_file_id=1, a_identifier="1"),
        make_row(a_file_id=2, a_identifier="2"),
        make_row(a_file_id=3, a_identifier="3", l_scope="Une section"),
    ).with_columns(measure_type_=pl.Series(["parkingProhibited", "noEntry", "noEntry"]))
    judged = judge_points_beyond_parking(df)
    assert judged[POINT_BEYOND_PARKING.column].to_list()[1] == {
        "status": "refused",
        "detail": "noEntry",
    }
    assert drop_refused(judged, POINT_BEYOND_PARKING)["a_identifier"].to_list() == ["1", "3"]


def copied_row(**overrides: Any) -> dict[str, Any]:
    defaults = {
        "a_type": "Temporaire",
        "a_title_html": "modifiant, à titre provisoire, la règle du stationnement rue Pillet-Will",
        "l_status": "Maintenue",
        "l_road_name": "Rue René Binet",
    }
    return make_row(**{**defaults, **overrides})


def test_a_copied_location_on_a_street_the_title_does_not_name_drops_its_whole_measure():
    df = make_frame(
        copied_row(l_file_id=100),
        copied_row(l_file_id=101, l_status="Ajoutée", l_road_name="Rue Pillet Will"),
        copied_row(m_file_id=11, l_file_id=110, l_road_name="Rue Pillet Will"),
    )
    judged = judge_copied_locations(df, everyone(df))
    assert [v["detail"] for v in judged[COPIED_LOCATION.column].to_list()] == [
        "status Maintenue",
        None,
        None,
    ]
    assert drop_refused(judged, COPIED_LOCATION)["l_file_id"].to_list() == [110]


@pytest.mark.parametrize(
    "overrides",
    [
        {"l_status": "Ajoutée"},
        {"l_status": "Retiré"},
        {"a_type": "Permanent"},
        {"l_road_name": "Rues Pillet Will"},
        {"l_road_name": "Rue de Staël", "a_title_html": "… rue de Stael"},
        {"l_road_name": "Rue Saint-Charles"},
    ],
    ids=["entered for the act", "withdrawn", "permanent", "plural", "accent", "common words only"],
)
def test_a_location_entered_for_the_act_or_named_by_its_title_is_kept(overrides):
    frame = make_frame(copied_row(**overrides))
    judged = judge_copied_locations(frame, everyone(frame))
    assert drop_refused(judged, COPIED_LOCATION).height == 1


def test_a_refused_location_is_dropped_and_its_regulation_goes_with_the_others(monkeypatch):
    df = make_frame(
        make_row(l_file_id=10),
        make_row(l_file_id=11),
        make_row(a_identifier="2024P10002", l_file_id=20),
    )
    refused = {11: "Cette adresse n'est pas reconnue", 20: "Les deux rues ne se croisent pas"}
    monkeypatch.setattr(location_filters, "load_refused_locations", lambda: refused)
    judged = judge_refused_locations(df)
    assert [v["detail"] for v in judged[REFUSED_LOCATION.column].to_list()] == [
        None,
        "Cette adresse n'est pas reconnue",
        "Les deux rues ne se croisent pas",
    ]
    kept = drop_refused(judged, REFUSED_LOCATION)
    assert kept["l_file_id"].to_list() == [10]
    assert set(kept["a_identifier"]) == {"2024P10001"}
    monkeypatch.setattr(location_filters, "load_refused_locations", lambda: {})
    assert drop_refused(judge_refused_locations(df), REFUSED_LOCATION).height == df.height


def section_row(**overrides: Any) -> dict[str, Any]:
    defaults = {
        "l_scope": "Une section",
        "l_point_house_number": None,
        "l_from_house_number": "12",
        "l_to_house_number": "20",
        "l_from_number_complement": "au droit du",
        "l_to_number_complement": "et le",
    }
    return make_row(**{**defaults, **overrides})


@pytest.mark.parametrize(
    "overrides",
    [
        {},
        {"l_from_number_complement": "dans sa partie comprise entre"},
        {"l_from_number_complement": "depuis", "l_to_number_complement": "jusqu'à"},
        {"l_from_side": "côté pair et impair", "l_to_direction": "dans les deux sens"},
        # On a parking ban "vers" is only the end of the range.
        {"l_from_number_complement": "depuis", "l_to_number_complement": "vers"},
        {
            "m_type": "aire piétonne",
            "l_from_number_complement": "depuis",
            "l_to_number_complement": "vers",
        },
        # A closure takes the whole carriageway: facing number 12 is where it starts.
        {"m_type": "circulation interdite", "l_from_number_complement": "en vis à vis du"},
    ],
)
def test_a_bound_that_only_says_where_it_is_keeps_the_location(overrides):
    assert discard_bound_qualifiers(pipeline(section_row(**overrides))).height == 1


@pytest.mark.parametrize(
    "overrides",
    [
        {"l_from_number_complement": "en vis à vis du"},
        {"l_to_number_complement": "en vis à vis du"},
        {"l_from_number_complement": "à l'intersection avec"},
        {"l_from_side": "côté pair"},
        {"l_to_side": "côté impair"},
        {"l_from_direction": "dans le sens de la circulation générale"},
        {
            "m_type": "circulation interdite",
            "l_from_number_complement": "depuis",
            "l_to_number_complement": "vers",
        },
    ],
)
def test_a_bound_that_qualifies_the_measure_drops_the_location(overrides, captured_logs):
    judged = judge_bound_qualifiers(pipeline(section_row(**overrides)))
    assert judged[BOUND_QUALIFIER.column].to_list()[0]["status"] == "refused"
    assert drop_refused(judged, BOUND_QUALIFIER).height == 0
    assert any("bound qualifies the measure" in line for line in captured_logs)


@pytest.mark.parametrize(
    "overrides, detail",
    [
        ({"l_from_number_complement": "en vis à vis du"}, "number complement 'en vis à vis du'"),
        ({"l_to_side": "côté impair"}, "one side of the street at a bound"),
        (
            {"l_from_direction": "dans le sens de la circulation générale"},
            "one direction at a bound",
        ),
    ],
)
def test_a_qualified_bound_says_why_in_its_verdict(overrides, detail):
    judged = judge_bound_qualifiers(pipeline(section_row(**overrides)))
    assert judged[BOUND_QUALIFIER.column].to_list() == [{"status": "refused", "detail": detail}]


def test_a_parking_ban_facing_the_number_is_dropped():
    """ "en vis à vis du" puts it on the other side of the street, which DiaLog cannot carry."""
    point = make_row(l_point_number_complement="en vis à vis du")
    assert discard_bound_qualifiers(pipeline(point)).height == 0
    assert (
        discard_bound_qualifiers(pipeline(make_row(l_point_number_complement="au droit du"))).height
        == 1
    )


def test_the_versioned_blacklist_is_readable():
    assert isinstance(load_refused_locations(), dict)


@pytest.mark.parametrize("weight_label, weight", [("3,5 T", 3.5), ("7,5 T", 7.5), ("19 T", 19.0)])
def test_a_tonnage_limit_is_a_no_entry_for_heavy_goods_vehicles(weight_label, weight):
    """A gauge limit is `noEntry` restricted to the vehicles it names, not a closure."""
    row = make_row(
        m_type="limitation catégorielle",
        m_params=[["véhicule concerné (1)", f"aux véhicules de plus de {weight_label}"]],
    )
    df = pipeline(row)
    assert df.get_column("measure_type_").to_list() == [MeasureTypeEnum.NOENTRY.value]
    assert df.get_column("vehicle_restricted_types").to_list() == [["heavyGoodsVehicle"]]
    assert df.get_column("vehicle_heavyweight_max_weight").to_list() == [weight]
    assert df.get_column("vehicle_all_vehicles").to_list() == [False]


@pytest.mark.parametrize(
    "second",
    ["aux véhicules de plus de 7,5 T", "aux véhicules transportant des matières dangereuses"],
)
def test_two_tonnages_on_one_measure_keep_the_stricter(second):
    row = make_row(
        m_type="limitation catégorielle",
        m_params=[
            ["véhicule concerné (1)", "aux véhicules de plus de 7,5 T"],
            ["véhicule concerné (2)", "aux véhicules de plus de 19 T"],
            ["véhicule concerné (3)", second],
        ],
    )
    assert pipeline(row).get_column("vehicle_heavyweight_max_weight").to_list() == [7.5]


def test_all_vehicles_alone_produces_only_that_field():
    source = DataSourceIntegration(None, None)  # type: ignore[arg-type]
    measure = pipeline(make_row()).pipe(source.select_regulation_measure_fields).row(0, named=True)
    dto = build_vehicle_set(measure)  # type: ignore[arg-type]
    assert dto.to_dict() == {"allVehicles": True}


def test_hazardous_materials_have_their_own_type():
    row = make_row(
        m_type="limitation catégorielle",
        m_params=[["véhicule concerné (1)", "aux véhicules transportant des matières dangereuses"]],
    )
    df = pipeline(row)
    assert df.get_column("vehicle_restricted_types").to_list() == [["hazardousMaterials"]]


def test_a_length_in_metres_is_not_turned_into_a_dimension():
    """The label never says which dimension, and none of the 26 325 measures of 2026-09-08
    carries a length, height or width beside it: without a paragraph naming it, "other"."""
    row = make_row(
        m_type="limitation dimensionnelle",
        m_params=[
            ["valeur de la limite", "aux véhicules de plus de 10 mètres"],
            ["véhicule concerné (1)", "à tous les véhicules"],
        ],
    )
    df = pipeline(row)
    assert df.get_column("vehicle_restricted_types").to_list() == [["other"]]
    assert df.get_column("vehicle_other_restricted_type_text").to_list() == [
        "aux véhicules de plus de 10 mètres"
    ]
    assert df.get_column("vehicle_max_length").to_list() == [None]
    assert df.get_column("vehicle_max_height").to_list() == [None]
    assert df.get_column("vehicle_max_width").to_list() == [None]


def test_a_category_without_an_equivalent_becomes_other_plus_its_label():
    row = make_row(
        m_type="limitation catégorielle",
        m_params=[["véhicule concerné (1)", "aux autocars"]],
    )
    df = pipeline(row)
    assert df.get_column("vehicle_restricted_types").to_list() == [["other"]]
    assert df.get_column("vehicle_other_restricted_type_text").to_list() == ["aux autocars"]
    assert df.get_column("vehicle_all_vehicles").to_list() == [False]


def test_a_restriction_outside_the_closed_list_drops_its_measure(captured_logs):
    """A new value waits for a human."""
    label = "aux véhicules de plus de 12 T"
    row = make_row(m_type="limitation catégorielle", m_params=[["véhicule concerné (1)", label]])
    assert pipeline(row).height == 0
    assert any("outside the closed list" in line and label in line for line in captured_logs)


def test_a_measure_restricting_cycles_is_left_out_not_one_exempting_them():
    restricting = make_row(
        m_file_id=1, m_params=[["véhicule concerné (2)", "des taxis ; des cycles"]]
    )
    shared_lane = make_row(
        m_file_id=2,
        m_params=[["véhicule concerné (2)", "des  véhicules de transports en commun et cycles"]],
    )
    exempting = make_row(m_file_id=3, m_params=[["dérogation pour véhicule", "aux cycles"]])

    result = apply_rules(
        make_frame(restricting, shared_lane, exempting), [PEDESTRIAN_OR_CYCLE_VEHICLES]
    )

    assert result.frame["m_file_id"].to_list() == [3]


@pytest.mark.parametrize(
    "label, left_out",
    [
        ("interdiction double sens pour les cyclistes", True),
        ("voie cyclable", True),
        ("Feux piéton", True),
        ("circulation interdite", False),
        (None, False),
    ],
)
def test_measures_aimed_at_pedestrians_or_cycles_are_left_out_by_label(label, left_out):
    result = apply_rules(make_frame(make_row(m_type=label)), [PEDESTRIAN_OR_CYCLE_MEASURES])
    assert result.frame.is_empty() is left_out


@pytest.mark.parametrize("label", ["limitation dimensionnelle", "limitation catégorielle"])
def test_an_unreadable_gauge_reaches_the_rules_naming_no_vehicle(label):
    """Neither a closure for everyone, nor "other" with no threshold: the common rule
    leaves it out."""
    row = make_row(m_type=label, m_params=[["véhicule concerné (1)", "à tous les véhicules"]])

    result = apply_rules(pipeline(row), [THRESHOLD_MISSING], canonical=True)

    assert result.frame.is_empty()


@pytest.mark.parametrize(
    "title, params, left_out",
    [
        ("portant interdiction d'arrêt sauf aux véhicules de police", [], True),
        (
            "portant interdiction d'arrêt sauf aux véhicules de police",
            [["dérogation pour véhicule", "aux véhicules des services de police"]],
            False,
        ),
        ("portant interdiction d'arrêt rue de Test", [], False),
    ],
)
def test_an_exemption_written_in_the_title_only_leaves_the_measure_out(title, params, left_out):
    row = make_row(
        a_title_html=title,
        m_params=[["véhicule concerné (3)", "à tous les véhicules"], *params],
    )
    result = apply_rules(pipeline(row), [EXEMPTION_IN_TITLE_ONLY])
    assert result.frame.is_empty() is left_out


@pytest.mark.parametrize(
    "parameter, value, expected",
    [
        ("dérogation pour usager", "aux véhicules des riverains", "localResident"),
        ("dérogation pour véhicule", "aux véhicules de secours", "emergencyServices"),
        ("dérogation pour véhicule", "aux véhicules des sapeurs pompiers", "emergencyServices"),
        (
            "dérogation pour véhicule",
            "aux véhicules d'intérêt général prioritaire",
            "emergencyServices",
        ),
        (
            "dérogation pour véhicule",
            "aux véhicules de nettoiement",
            "roadMaintenanceOrConstruction",
        ),
        ("dérogation pour véhicule", "des véhicules de chantiers", "roadMaintenanceOrConstruction"),
        ("dérogation pour véhicule", "aux cycles", "bicycle"),
        ("dérogation pour véhicule", "aux véhicules de livraison", "commercial"),
        (
            "dérogation pour véhicule",
            "aux véhicules d'approvisionnement de marchés",
            "commercial",
        ),
        ("dérogation pour véhicule", "des taxis", "taxi"),
        ("Dérogations véhicules", "cycles", "bicycle"),
    ],
)
def test_every_exemption_of_the_catalog_maps_to_its_dialog_type(parameter, value, expected):
    row = make_row(m_type="circulation interdite", m_params=[[parameter, value]])
    df = pipeline(row)
    assert df.get_column("vehicle_exempted_types").to_list() == [[expected]]
    assert df.get_column("vehicle_other_exempted_type_text").to_list() == [None]


def test_a_multi_valued_exemption_is_split_before_being_mapped():
    row = make_row(
        m_type="circulation interdite",
        m_params=[
            [
                "dérogation pour véhicule",
                "aux véhicules de secours ; aux véhicules des sapeurs pompiers ; aux cycles",
            ]
        ],
    )
    df = pipeline(row)
    assert df.get_column("vehicle_exempted_types").to_list() == [["emergencyServices", "bicycle"]]


def test_an_unknown_exemption_keeps_its_label_as_free_text():
    row = make_row(
        m_type="circulation interdite",
        m_params=[
            [
                "Dérogations véhicules",
                "véhicules des services publics dans le cadre de leurs missions",
            ]
        ],
    )
    df = pipeline(row)
    assert df.get_column("vehicle_exempted_types").to_list() == [["other"]]
    assert df.get_column("vehicle_other_exempted_type_text").to_list() == [
        "véhicules des services publics dans le cadre de leurs missions"
    ]


@pytest.mark.parametrize("length, kept", [(300, True), (301, False)])
def test_exemptions_longer_than_the_api_takes_drop_the_measure_rather_than_widen_it(length, kept):
    text = "v" * length
    row = make_row(m_type="circulation interdite", m_params=[["Dérogations véhicules", text]])
    df = pipeline(row)
    assert df.get_column("vehicle_other_exempted_type_text").to_list() == ([text] if kept else [])


def test_the_penal_qualification_is_not_a_vehicle_restriction():
    row = make_row(
        m_type="interdiction de stationnement",
        m_params=[
            ["véhicule concerné (3)", "à tous les véhicules"],
            ["caractère aggravant", "gênant ; très gênant"],
        ],
    )
    df = pipeline(row)
    assert df.get_column("vehicle_all_vehicles").to_list() == [True]
    assert df.get_column("vehicle_restricted_types").to_list() == [None]


def test_a_filled_parameter_of_an_unknown_name_drops_its_measure(captured_logs):
    """It may narrow the measure like a time slot does: never published wider."""
    known = make_row(
        m_params=[["véhicule concerné (3)", "à tous les véhicules"], ["caractère aggravant", "x"]]
    )
    unknown = make_row(
        m_file_id=11,
        m_params=[["véhicule concerné (3)", "à tous les véhicules"], ["Alinéa", "sauf dimanche"]],
    )
    assert pipeline(known, unknown).get_column("m_file_id").to_list() == [10]
    assert any("unknown name" in line and "Alinéa" in line for line in captured_logs)
    assert not any("caractère aggravant" in line for line in captured_logs)


def test_a_temporary_period_spans_whole_local_days():
    row = make_row(
        a_type="Temporaire",
        a_reason="Travaux",
        a_start_date=date(2026, 9, 10),
        a_end_date=date(2026, 9, 20),
    )
    df = pipeline(row)
    assert df.get_column("period_start_date").to_list() == ["2026-09-10T00:00:00+02:00"]
    assert df.get_column("period_end_date").to_list() == ["2026-09-20T23:59:59+02:00"]
    assert df.get_column("period_is_permanent").to_list() == [False]
    assert df.get_column("period_recurrence_type").to_list() == ["everyDay"]


def test_a_winter_period_carries_the_winter_offset():
    row = make_row(
        a_type="Temporaire",
        a_reason="Travaux",
        a_start_date=date(2026, 1, 15),
        a_end_date=date(2026, 12, 31),
    )
    df = pipeline(row)
    assert df.get_column("period_start_date").to_list() == ["2026-01-15T00:00:00+01:00"]


def test_a_permanent_period_has_no_end():
    df = pipeline(make_row())
    assert df.get_column("period_is_permanent").to_list() == [True]
    assert df.get_column("period_end_date").to_list() == [None]
    assert df.get_column("period_recurrence_type").to_list() == ["everyDay"]


def test_a_permanent_without_a_start_is_dated_on_first_write(captured_logs):
    """Never the signature, never the moving "Modifié le"; `sync/dating.py` dates it
    when DiaLog is written."""
    row = make_row(
        a_start_date=None,
        a_signed_at=date(2011, 6, 30),
        a_modified_at=datetime(2017, 2, 3, 0, 30, 7),
    )
    df = pipeline(row)
    assert df.get_column("period_start_date").to_list() == [None]
    assert df.get_column("period_is_permanent").to_list() == [True]
    assert any("dated on first write" in line for line in captured_logs)


def test_a_start_after_its_end_rejects_the_whole_regulation(captured_logs):
    """Date controls: every row of the order goes, not only the one that shows the mistake."""
    valid = make_row(
        a_identifier="2026T10001",
        a_type="Temporaire",
        a_reason="Travaux",
        a_start_date=date(2026, 12, 1),
        a_end_date=date(2026, 12, 31),
        m_file_id=1,
        l_file_id=1,
    )
    impossible = {**valid, "a_start_date": date(2027, 1, 15), "m_file_id": 2, "l_file_id": 2}
    assert pipeline(valid).height == 1
    assert pipeline(valid, impossible).height == 0
    assert any("failing the date controls" in line for line in captured_logs)


def test_a_typo_on_the_year_rejects_the_regulation_the_state_still_calls_in_force(clean):
    assert "2019T15796" not in set(clean.get_column("a_identifier"))


def test_an_end_year_beyond_five_years_is_rejected():
    row = make_row(
        a_type="Temporaire",
        a_reason="Travaux",
        a_start_date=date(2026, 6, 1),
        a_end_date=date(2032, 6, 1),
    )
    assert pipeline(row).height == 0


def test_an_end_year_within_five_years_is_kept():
    row = make_row(
        a_type="Temporaire",
        a_reason="Travaux",
        a_start_date=date(2026, 6, 1),
        a_end_date=date(2031, 6, 1),
    )
    assert pipeline(row).height == 1


def test_a_start_year_beyond_next_year_is_rejected():
    row = make_row(
        a_type="Temporaire",
        a_reason="Travaux",
        a_start_date=date(2028, 1, 1),
        a_end_date=date(2028, 6, 1),
    )
    assert pipeline(row).height == 0


def test_a_temporary_regulation_longer_than_a_year_is_kept_but_counted():
    row = make_row(
        a_type="Temporaire",
        a_reason="Travaux",
        a_start_date=date(2026, 1, 1),
        a_end_date=date(2028, 1, 1),
    )
    assert pipeline(row).height == 1


def test_every_produced_column_survives_the_canonical_selection(clean):
    contract = set(RegulationMeasure.__annotations__)
    produced = {
        column
        for column in clean.columns
        if column.startswith(("regulation_", "measure_", "period_", "vehicle_"))
    }
    assert produced <= contract, sorted(produced - contract)


def test_the_canonical_keys_this_step_owns_are_all_produced(clean):
    expected = {
        "regulation_identifier",
        "regulation_title",
        "regulation_category",
        "regulation_subject",
        "regulation_other_category_text",
        "regulation_document_url",
        "measure_type_",
        "measure_max_speed",
        "period_start_date",
        "period_end_date",
        "period_recurrence_type",
        "period_is_permanent",
        "vehicle_all_vehicles",
        "vehicle_restricted_types",
        "vehicle_exempted_types",
        "vehicle_heavyweight_max_weight",
        "vehicle_max_length",
        "vehicle_max_height",
        "vehicle_max_width",
        "vehicle_other_restricted_type_text",
        "vehicle_other_exempted_type_text",
    }
    assert expected <= set(clean.columns)


def test_the_funnel_of_the_fixture_is_the_one_we_expect(raw, clean):
    # The unpublished measures are read too, but the 6 reserved parkings and the traffic light
    # (180 locations) get no row at all.
    assert raw.height == 75
    after_state = compute_regulation_fields(raw)
    assert after_state.height == 72  # 1 Abrogé + 1 Non signé + 1 Périmé
    after_type = discard_measures_out_of_model(compute_measure_fields(after_state))
    assert after_type.height == 64  # the 3 one-way streets are oriented further on
    assert clean.height == 61  # 2025T15489 and 2026T17998, by the date controls
    assert dict(
        zip(*clean.get_column("measure_type_").value_counts(sort=True).to_dict().values())
    ) == {
        MeasureTypeEnum.PARKINGPROHIBITED.value: 47,
        MeasureTypeEnum.NOENTRY.value: 13,
        MeasureTypeEnum.SPEEDLIMITATION.value: 1,
    }


@pytest.mark.parametrize(
    "opening, kept",
    [
        (None, True),
        ("", True),
        ("Le stationnement est interdit", True),
        ("Le stationnement est interdit tous les jours, sauf dérogation", False),
        ("La circulation est interdite", False),
    ],
)
def test_a_measure_opening_on_another_formula_than_its_name_is_dropped(opening, kept):
    out = make_frame(make_row(m_opening=opening)).pipe(compute_measure_fields)
    assert (out.height == 1) is kept
