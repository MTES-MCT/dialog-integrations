"""Offline tests of the Paris / Eudonet transformations (plan §3, §4).

Two sources of data: the frozen fixture, replayed on `2026-09-08` so that the perimeter
filters are deterministic, and small hand-built frames for the cases the fixture does not
carry (an unknown catalog value, a start after an end, a permanent without a start date).
"""

from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl
import pytest
from loguru import logger

from api.dia_log_client.models import (
    MeasureTypeEnum,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodySubject,
)
from integrations.base_data_source_integration import RegulationMeasure
from integrations.base_integration import BaseIntegration
from integrations.co_paris.eudonet.data_source_integration import (
    IDENTIFIER_PREFIX,
    RAW_COLUMNS,
    TITLE_ELLIPSIS,
    TITLE_MAX_LENGTH,
    DataSourceIntegration,
    EudonetVocabularyError,
    add_pedestrian_area_speed,
    build_raw_dataframe,
    compute_measure_fields,
    compute_period_fields,
    compute_regulation_fields,
    compute_vehicle_fields,
    discard_police_prefecture,
    load_fixture,
)
from integrations.co_paris.eudonet.vocabulary import (
    MEASURE_TYPE_BY_LABEL,
    PERMANENT_SUBJECT_TEXT,
    UNKNOWN_REASON_TEXT,
)

FIXTURE = Path("tests/co_paris/eudonet.json")

# The day the fixture was extracted. Every date control is relative to it.
TODAY = date(2026, 9, 8)


@pytest.fixture
def raw():
    return build_raw_dataframe(load_fixture(FIXTURE))


@pytest.fixture
def clean(raw):
    """Everything this step owns, stopping before the locations of the other module."""
    return (
        raw.pipe(compute_regulation_fields, TODAY)
        .pipe(compute_measure_fields)
        .pipe(compute_vehicle_fields)
        .pipe(compute_period_fields, TODAY)
    )


@pytest.fixture
def captured_logs():
    """Loguru does not go through `logging`: collect its records ourselves."""
    collected: list[str] = []
    handle = logger.add(collected.append, level="INFO", format="{message}")
    yield collected
    logger.remove(handle)


def make_row(**overrides: Any) -> dict[str, Any]:
    """One raw row, defaulted to a plain permanent parking prohibition."""
    row: dict[str, Any] = dict.fromkeys(RAW_COLUMNS)
    row.update(
        a_file_id=1,
        a_identifier="2024P10001",
        a_title_html="Réglementant le stationnement rue de Test",
        a_type="Permanent",
        a_state="En vigueur",
        a_start_date=date(2020, 1, 1),
        a_signed_at=date(2019, 12, 20),
        a_modified_at=datetime(2021, 3, 4, 0, 30, 0),
        a_service="Mairie de Paris",
        m_file_id=10,
        m_type="interdiction de stationnement",
        m_params=[["véhicule concerné (3)", "à tous les véhicules"]],
        l_file_id=100,
        l_scope="Un point",
        l_road_name="Rue de Test",
        l_district="11ème arrondissement",
        l_point_house_number="12",
    )
    row.update(overrides)
    return row


def make_frame(*rows: dict[str, Any]) -> pl.DataFrame:
    return pl.DataFrame(list(rows), schema=RAW_COLUMNS)


def pipeline(*rows: dict[str, Any], today: date = TODAY) -> pl.DataFrame:
    return (
        make_frame(*rows)
        .pipe(compute_regulation_fields, today)
        .pipe(compute_measure_fields)
        .pipe(compute_vehicle_fields)
        .pipe(compute_period_fields, today)
    )


# ---------------------------------------------------------------------------
# Regulation
# ---------------------------------------------------------------------------


def test_the_identifier_is_the_eudonet_number_behind_a_stable_prefix(clean):
    """`1101` goes through untouched: it is the key the whole synchronisation rests on."""
    identifiers = set(clean.get_column("regulation_identifier"))
    assert "PARIS-EUDO-2021T113851" in identifiers
    # Pre-2017 numbers have another shape and are not normalised either.
    assert "PARIS-EUDO-1979-16282" in identifiers
    assert all(value.startswith(IDENTIFIER_PREFIX) for value in identifiers)
    # 60 characters is the API limit on `identifier`.
    assert max(len(value) for value in identifiers) <= 60


def test_the_identifier_is_stable_across_two_runs(raw):
    first = compute_regulation_fields(raw, TODAY).get_column("regulation_identifier").to_list()
    second = compute_regulation_fields(raw, TODAY).get_column("regulation_identifier").to_list()
    assert first == second


@pytest.mark.parametrize(
    "state, kept",
    [
        ("En vigueur", True),
        ("Publié", True),
        ("Signé", True),
        ("Non signé", False),
        ("Périmé", False),
        ("Abrogé", False),
        ("Abrogation en cours", False),
    ],
)
def test_only_enforceable_states_survive(state, kept):
    """An act that is not signed is not enforceable; a repealed one has left the perimeter."""
    df = compute_regulation_fields(make_frame(make_row(a_state=state)), TODAY)
    assert df.height == (1 if kept else 0)


def test_the_repealed_and_the_expired_of_the_fixture_are_gone(clean):
    identifiers = set(clean.get_column("a_identifier"))
    assert "1982-10803" not in identifiers  # Abrogé
    assert "2022T18442" not in identifiers  # Périmé, ended 2026-03-01


def test_a_temporary_regulation_still_to_start_is_kept():
    """Offline, the end date is the filter — a works order announced for next month stays."""
    row = make_row(
        a_type="Temporaire",
        a_reason="Travaux",
        a_start_date=date(2026, 10, 1),
        a_end_date=date(2026, 11, 30),
    )
    assert pipeline(row).height == 1


def test_a_temporary_regulation_that_ended_is_dropped():
    row = make_row(
        a_type="Temporaire",
        a_reason="Travaux",
        a_start_date=date(2026, 1, 1),
        a_end_date=date(2026, 9, 7),
    )
    assert compute_regulation_fields(make_frame(row), TODAY).height == 0


def test_a_temporary_regulation_without_an_end_date_is_dropped():
    """Nothing would ever close it (R-37); online the Eudonet filter never returns one."""
    row = make_row(a_type="Temporaire", a_reason="Travaux", a_end_date=None)
    assert compute_regulation_fields(make_frame(row), TODAY).height == 0


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
    title = compute_regulation_fields(make_frame(row), TODAY).get_column("regulation_title")[0]
    assert title == "Arrêté n° 2024P10001 — Réglementant le stationnement"


@pytest.mark.parametrize("raw_title", [None, "", "   ", "<p></p>", "&nbsp;"])
def test_an_empty_title_is_built_rather_than_blocking(raw_title):
    """R-24: a title is never a reason to drop a regulation."""
    row = make_row(a_title_html=raw_title)
    title = compute_regulation_fields(make_frame(row), TODAY).get_column("regulation_title")[0]
    assert title == "Arrêté n° 2024P10001"


@pytest.mark.parametrize("length, truncated", [(20, False), (200, False), (400, True)])
def test_the_title_is_truncated_to_the_api_limit(length, truncated):
    row = make_row(a_title_html="a" * length)
    title = compute_regulation_fields(make_frame(row), TODAY).get_column("regulation_title")[0]
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
        a_type="Temporaire", a_reason=reason, a_start_date=TODAY, a_end_date=date(2026, 12, 1)
    )
    df = compute_regulation_fields(make_frame(row), TODAY)
    assert df.get_column("regulation_subject")[0] == subject
    assert df.get_column("regulation_other_category_text")[0] == text


def test_a_permanent_regulation_says_so_in_its_subject():
    """Permanent orders carry no reason at all in Eudonet (plan §3)."""
    df = compute_regulation_fields(make_frame(make_row()), TODAY)
    assert df.get_column("regulation_subject")[0] == PostApiRegulationsAddBodySubject.OTHER.value
    assert df.get_column("regulation_other_category_text")[0] == PERMANENT_SUBJECT_TEXT
    assert (
        df.get_column("regulation_category")[0]
        == PostApiRegulationsAddBodyCategory.PERMANENTREGULATION.value
    )


def test_the_other_category_text_stays_within_a_hundred_characters():
    row = make_row(a_type="Temporaire", a_reason="R" * 200, a_end_date=date(2026, 12, 1))
    df = compute_regulation_fields(make_frame(row), TODAY)
    assert len(df.get_column("regulation_other_category_text")[0]) == 100


def test_no_document_url_is_invented(clean):
    """The Eudonet attachment link is a one-shot token, not a public URL (plan §6)."""
    assert clean.get_column("regulation_document_url").null_count() == clean.height


def test_an_unknown_regulation_type_raises():
    with pytest.raises(EudonetVocabularyError, match="1108"):
        compute_regulation_fields(make_frame(make_row(a_type="Provisoire")), TODAY)


# ---------------------------------------------------------------------------
# Measure
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("label, expected", sorted(MEASURE_TYPE_BY_LABEL.items()))
def test_every_label_of_catalog_1202_maps_as_decided(label, expected):
    """The 32 values of the catalog, one test each: nothing falls through by accident."""
    params = [["véhicule concerné (1)", "à tous les véhicules"]]
    if label == "limitation de vitesse":
        params = [["valeur de la vitesse", "à 30 km/h"]]
    row = make_row(
        m_type=label,
        m_params=params,
        l_direction="dans les deux sens" if label == "sens interdit (ou sens unique)" else None,
    )
    df = compute_measure_fields(compute_regulation_fields(make_frame(row), TODAY))
    if expected is None:
        assert df.height == 0
    else:
        assert df.get_column("measure_type_").to_list() == [expected]


def test_the_catalog_table_is_complete():
    """32 values in catalog `1202` on 2026-09-08; a 33rd has to be qualified by hand."""
    assert len(MEASURE_TYPE_BY_LABEL) == 32


def test_an_unknown_measure_label_raises():
    """Paris can enrich the catalog: guessing what a new value means is forbidden (Q-05)."""
    row = make_row(m_type="interdiction de klaxonner")
    with pytest.raises(EudonetVocabularyError, match="interdiction de klaxonner"):
        compute_measure_fields(compute_regulation_fields(make_frame(row), TODAY))


def test_measures_outside_the_model_are_dropped_and_counted(raw, captured_logs):
    compute_measure_fields(compute_regulation_fields(raw, TODAY))
    dropped = [line for line in captured_logs if "without a DiaLog equivalent" in line]
    assert len(dropped) == 1
    # One row per measure: the locations of out-of-model measures are never read.
    assert "'stationnement réservé': 6" in dropped[0]


def test_rows_without_any_measure_are_dropped_and_counted(captured_logs):
    df = compute_measure_fields(
        compute_regulation_fields(make_frame(make_row(m_type=None, m_params=None)), TODAY)
    )
    assert df.height == 0
    assert any("rows without any measure" in line for line in captured_logs)


@pytest.mark.parametrize(
    "value, expected", [("à 30 km/h", 30), ("30 km/h", 30), ("à 50 km/h", 50), ("50 km/h", 50)]
)
def test_the_speed_is_read_from_its_parameter(value, expected):
    row = make_row(m_type="limitation de vitesse", m_params=[["valeur de la vitesse", value]])
    df = compute_measure_fields(compute_regulation_fields(make_frame(row), TODAY))
    assert df.get_column("measure_max_speed").to_list() == [expected]


@pytest.mark.parametrize("params", [None, [], [["valeur de la vitesse", ""]]])
def test_a_speed_limitation_without_a_speed_is_dropped(params, captured_logs):
    """R-02: 30 or 50 is a decision, not a default."""
    row = make_row(m_type="limitation de vitesse", m_params=params)
    df = compute_measure_fields(compute_regulation_fields(make_frame(row), TODAY))
    assert df.height == 0
    assert any("without a readable 'valeur de la vitesse'" in line for line in captured_logs)


def test_zone_30_carries_its_speed_in_its_name():
    row = make_row(m_type="zone 30", m_params=None)
    df = compute_measure_fields(compute_regulation_fields(make_frame(row), TODAY))
    assert df.get_column("measure_type_").to_list() == [MeasureTypeEnum.SPEEDLIMITATION.value]
    assert df.get_column("measure_max_speed").to_list() == [30]


def test_no_speed_leaks_onto_another_measure_type():
    row = make_row(
        m_type="interdiction de stationnement", m_params=[["valeur de la vitesse", "à 30 km/h"]]
    )
    df = compute_measure_fields(compute_regulation_fields(make_frame(row), TODAY))
    assert df.get_column("measure_max_speed").to_list() == [None]


@pytest.mark.parametrize("label", ["sens interdit (ou sens unique)", "mise en impasse"])
def test_one_ways_and_dead_ends_are_out_of_the_model(label, captured_logs):
    """R-32 freeze for the one-way; a dead end is not a closure of the street (R-02)."""
    row = make_row(m_type=label, l_direction="du début vers la fin du segment")
    assert pipeline(row).height == 0
    assert any("without a DiaLog equivalent" in line and label in line for line in captured_logs)


def test_the_one_way_of_the_fixture_is_dropped(clean):
    assert "05-00133" not in set(clean.get_column("a_identifier"))


def test_a_filled_time_slot_drops_the_measure(captured_logs):
    """R-78: "les dimanches et jours fériés" is not expressible, so the measure goes."""
    row = make_row(
        m_params=[["Jours et Horaires", "de 20h00 à 7h00 ainsi que les dimanches et jours fériés"]]
    )
    assert pipeline(row).height == 0
    assert any("'Jours et Horaires'" in line for line in captured_logs)


def test_an_empty_time_slot_is_no_time_slot():
    assert pipeline(make_row(m_params=[["Jours et Horaires", ""]])).height == 1


def test_every_location_of_a_measure_shares_its_group_key():
    """R-76: one Eudonet measure is one DiaLog measure, whatever its number of locations."""
    df = pipeline(make_row(l_file_id=1), make_row(l_file_id=2), make_row(m_file_id=11))
    assert df.get_column("measure_group_key").to_list() == ["10", "10", "11"]
    assert DataSourceIntegration.group_locations_by_measure


def test_a_pedestrian_area_is_a_ban_with_local_access_and_a_walking_pace_twin():
    """R-71: `noEntry` + `desserteLocale` + Paris's own exemptions, and 5 km/h for all."""
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


@pytest.mark.parametrize("publish, kept", [(True, 2), (False, 1)])
def test_the_police_prefecture_is_published_only_behind_its_constant(publish, kept):
    """R-73 by analogy: on the staging only, until Paris agrees."""
    df = make_frame(
        make_row(), make_row(a_identifier="2024P10002", a_service="Préfecture de Police")
    )
    assert discard_police_prefecture(df, publish).height == kept


# ---------------------------------------------------------------------------
# Vehicles
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("weight_label, weight", [("3,5 T", 3.5), ("7,5 T", 7.5), ("19 T", 19.0)])
def test_a_tonnage_limit_is_a_no_entry_for_heavy_goods_vehicles(weight_label, weight):
    """R-33: a gauge limit is `noEntry` restricted to the vehicles it names, not a closure."""
    row = make_row(
        m_type="limitation catégorielle",
        m_params=[["véhicule concerné (1)", f"aux véhicules de plus de {weight_label}"]],
    )
    df = pipeline(row)
    assert df.get_column("measure_type_").to_list() == [MeasureTypeEnum.NOENTRY.value]
    assert df.get_column("vehicle_restricted_types").to_list() == [["heavyGoodsVehicle"]]
    assert df.get_column("vehicle_heavyweight_max_weight").to_list() == [weight]
    # The road stays open to everything else.
    assert df.get_column("vehicle_all_vehicles").to_list() == [False]


def test_all_vehicles_alone_produces_only_that_field():
    """`create_save_vehicle_dto` must not send a set of empty lists next to `allVehicles`."""
    source = DataSourceIntegration.__new__(DataSourceIntegration)
    integration = BaseIntegration.__new__(BaseIntegration)
    measure = pipeline(make_row()).pipe(source.select_regulation_measure_fields).row(0, named=True)
    dto = integration.create_save_vehicle_dto(measure)  # type: ignore[arg-type]
    assert dto.to_dict() == {"allVehicles": True}


def test_hazardous_materials_have_their_own_type():
    row = make_row(
        m_type="limitation catégorielle",
        m_params=[["véhicule concerné (1)", "aux véhicules transportant des matières dangereuses"]],
    )
    df = pipeline(row)
    assert df.get_column("vehicle_restricted_types").to_list() == [["hazardousMaterials"]]


def test_a_length_in_metres_is_not_turned_into_a_dimension():
    """The label never says which dimension, and no measure of the dump precises it (R-35).

    Checked on the 26 325 measures of the 2026-09-08 dump: not one carries a
    `longueur` / `hauteur` / `largeur` value beside "aux véhicules de plus de N mètres".
    """
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


def test_cycles_are_restricted_as_other_because_bicycle_is_exemption_only():
    row = make_row(m_type="limitation catégorielle", m_params=[["véhicule concerné (1)", "cycles"]])
    df = pipeline(row)
    assert df.get_column("vehicle_restricted_types").to_list() == [["other"]]
    assert df.get_column("vehicle_other_restricted_type_text").to_list() == ["cycles"]


@pytest.mark.parametrize("label", ["limitation dimensionnelle", "limitation catégorielle"])
def test_an_unreadable_gauge_is_dropped_and_counted(label, captured_logs):
    """R-35: neither a closure for everyone, nor "other" with no threshold."""
    row = make_row(m_type=label, m_params=[["véhicule concerné (1)", "à tous les véhicules"]])
    assert pipeline(row).height == 0
    assert any("no readable vehicle value" in line for line in captured_logs)


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
    """6 cells of the perimeter join several catalog labels with "; "."""
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


def test_the_penal_qualification_is_not_a_vehicle_restriction():
    """`caractère aggravant` says the parking is `gênant`, not which vehicles are hit."""
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


def test_a_dropped_time_slot_is_counted(captured_logs):
    """The pivot has no `timeSlots`: the measure is broadcast for the whole day (phase 2)."""
    row = make_row(
        m_params=[
            ["véhicule concerné (3)", "à tous les véhicules"],
            ["Jours et Horaires", "de 20h00 à 7h00 ainsi que les dimanches et jours fériés"],
        ]
    )
    pipeline(row)
    assert any("Jours et Horaires" in line for line in captured_logs)


# ---------------------------------------------------------------------------
# Periods
# ---------------------------------------------------------------------------


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


def test_a_permanent_without_a_start_falls_back_on_the_signature_date(captured_logs):
    """R-39: a fallback, never a constant — and the fallback is counted."""
    row = make_row(a_start_date=None, a_signed_at=date(2011, 6, 30))
    df = pipeline(row)
    assert df.get_column("period_start_date").to_list() == ["2011-06-30T00:00:00+02:00"]
    assert any("fall back on" in line for line in captured_logs)


def test_a_permanent_without_a_start_nor_a_signature_is_dated_on_first_write(captured_logs):
    """R-39: never the moving "Modifié le"; `sync/dating.py` dates it when DiaLog is written."""
    row = make_row(
        a_start_date=None, a_signed_at=None, a_modified_at=datetime(2017, 2, 3, 0, 30, 7)
    )
    df = pipeline(row)
    assert df.get_column("period_start_date").to_list() == [None]
    assert df.get_column("period_is_permanent").to_list() == [True]
    assert any("dated on first write" in line for line in captured_logs)


def test_a_start_after_its_end_rejects_the_whole_regulation(captured_logs):
    """R-38. Every row of the order goes, not only the one that shows the mistake."""
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
    # On its own the first row would go through.
    assert pipeline(valid).height == 1
    assert pipeline(valid, impossible).height == 0
    assert any("R-38 date controls" in line for line in captured_logs)


def test_a_typo_on_the_year_rejects_the_regulation_the_state_still_calls_in_force(clean):
    """`2019T15796` runs to 2109 and Eudonet still says "En vigueur" (plan §4.3)."""
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


def test_a_temporary_regulation_longer_than_a_year_is_kept_but_counted(captured_logs):
    """Long works sites (tramway, RATP), not mistakes — 280 of them on the perimeter."""
    row = make_row(
        a_type="Temporaire",
        a_reason="Travaux",
        a_start_date=date(2026, 1, 1),
        a_end_date=date(2028, 1, 1),
    )
    assert pipeline(row).height == 1
    assert any("last more than a year" in line for line in captured_logs)


# ---------------------------------------------------------------------------
# Contract
# ---------------------------------------------------------------------------


def test_every_produced_column_survives_the_pivot(clean):
    """`select_regulation_measure_fields` drops any column the pivot does not declare."""
    contract = set(RegulationMeasure.__annotations__)
    produced = {
        column
        for column in clean.columns
        if column.startswith(("regulation_", "measure_", "period_", "vehicle_"))
    }
    assert produced <= contract, sorted(produced - contract)


def test_the_pivot_keys_this_step_owns_are_all_produced(clean):
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
    """Row by row, what the fixture loses and why. Update the numbers, never the rule."""
    # 251 locations, but out-of-model measures take one row each, locations unread
    assert raw.height == 79
    after_state = compute_regulation_fields(raw, TODAY)
    assert after_state.height == 76  # 1 Abrogé + 1 Non signé + 1 Périmé
    after_type = compute_measure_fields(after_state)
    # 13 measures out of model (3 one-ways and 1 dead end among them), 2 speedless; the
    # pedestrian area is in the model since 2026-09-23 (R-71)
    assert after_type.height == 61
    assert clean.height == 58  # 2025T15489 and 2026T17998, R-38 date controls
    assert dict(
        zip(*clean.get_column("measure_type_").value_counts(sort=True).to_dict().values())
    ) == {
        MeasureTypeEnum.PARKINGPROHIBITED.value: 47,
        MeasureTypeEnum.NOENTRY.value: 10,
        MeasureTypeEnum.SPEEDLIMITATION.value: 1,
    }
