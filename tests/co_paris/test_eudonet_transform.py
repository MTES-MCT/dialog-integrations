import re
from datetime import date, datetime
from typing import Any

import polars as pl
import pytest
from loguru import logger

from api.dia_log_client.models import (
    MeasureTypeEnum,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodySubject,
)
from canonical.regulation_measure import RegulationMeasure
from integrations.co_paris.eudonet import location_filters
from integrations.co_paris.eudonet.data_source_integration import (
    DataSourceIntegration,
)
from integrations.co_paris.eudonet.location_filters import (
    discard_location_complements,
    discard_points_beyond_parking,
    discard_refused_locations,
    load_refused_locations,
)
from integrations.co_paris.eudonet.measures import (
    add_pedestrian_area_speed,
    compute_measure_fields,
)
from integrations.co_paris.eudonet.published_elsewhere import (
    discard_published_elsewhere,
    index_by_eudonet_number,
    load_frozen_elsewhere,
)
from integrations.co_paris.eudonet.raw_rows import (
    RAW_COLUMNS,
    build_raw_dataframe,
)
from integrations.co_paris.eudonet.regulations import (
    IDENTIFIER_PREFIX,
    TITLE_MAX_LENGTH,
    compute_period_fields,
    compute_regulation_fields,
    filter_police_prefecture,
)
from integrations.co_paris.eudonet.text import (
    ELLIPSIS as TITLE_ELLIPSIS,
)
from integrations.co_paris.eudonet.vehicles import (
    compute_vehicle_fields,
)
from integrations.co_paris.eudonet.vocabulary import (
    MEASURE_TYPE_BY_LABEL,
    PERMANENT_SUBJECT_TEXT,
    UNKNOWN_REASON_TEXT,
)
from run.organization import BaseIntegration
from tests.co_paris.eudonet_fixture import load_fixture

FIXTURE_EXTRACTED_ON = date(2026, 9, 8)


@pytest.fixture
def raw():
    return build_raw_dataframe(load_fixture())


@pytest.fixture
def clean(raw):
    return (
        raw.pipe(compute_regulation_fields, FIXTURE_EXTRACTED_ON)
        .pipe(compute_measure_fields)
        .pipe(compute_vehicle_fields)
        .pipe(compute_period_fields, FIXTURE_EXTRACTED_ON)
    )


@pytest.fixture
def captured_logs():
    """Loguru does not go through `logging`: collect its records ourselves."""
    collected: list[str] = []
    handle = logger.add(collected.append, level="INFO", format="{message}")
    yield collected
    logger.remove(handle)


def make_row(**overrides: Any) -> dict[str, Any]:
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


def pipeline(*rows: dict[str, Any], today: date = FIXTURE_EXTRACTED_ON) -> pl.DataFrame:
    return (
        make_frame(*rows)
        .pipe(compute_regulation_fields, today)
        .pipe(compute_measure_fields)
        .pipe(compute_vehicle_fields)
        .pipe(compute_period_fields, today)
    )


def test_the_identifier_is_the_eudonet_number_behind_a_stable_prefix(clean):
    identifiers = set(clean.get_column("regulation_identifier"))
    assert "PARIS-EUDO-2021T113851" in identifiers
    assert "PARIS-EUDO-1979-16282" in identifiers
    assert all(value.startswith(IDENTIFIER_PREFIX) for value in identifiers)
    # 60 characters is the API limit on `identifier`.
    assert max(len(value) for value in identifiers) <= 60


def test_the_identifier_is_stable_across_two_runs(raw):
    first = (
        compute_regulation_fields(raw, FIXTURE_EXTRACTED_ON)
        .get_column("regulation_identifier")
        .to_list()
    )
    second = (
        compute_regulation_fields(raw, FIXTURE_EXTRACTED_ON)
        .get_column("regulation_identifier")
        .to_list()
    )
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
    assert (
        compute_regulation_fields(make_frame(permanent), FIXTURE_EXTRACTED_ON).height
        == kept_permanent
    )
    assert (
        compute_regulation_fields(make_frame(temporary), FIXTURE_EXTRACTED_ON).height
        == kept_temporary
    )


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


def test_a_temporary_regulation_that_ended_is_dropped():
    row = make_row(
        a_type="Temporaire",
        a_reason="Travaux",
        a_start_date=date(2026, 1, 1),
        a_end_date=date(2026, 9, 7),
    )
    assert compute_regulation_fields(make_frame(row), FIXTURE_EXTRACTED_ON).height == 0


@pytest.mark.parametrize(
    "dates",
    [{"a_end_date": None}, {"a_start_date": None, "a_end_date": date(2026, 11, 30)}],
    ids=["no end: nothing would close it", "no start: the write date would stand in"],
)
def test_a_temporary_regulation_needs_both_its_dates(dates):
    """A temporary period has a start and an end."""
    row = make_row(a_type="Temporaire", a_reason="Travaux", **dates)
    assert compute_regulation_fields(make_frame(row), FIXTURE_EXTRACTED_ON).height == 0


def test_a_regulation_without_an_order_number_is_dropped_not_the_run():
    rows = make_frame(make_row(), make_row(a_file_id=2, a_identifier=None))
    kept = compute_regulation_fields(rows, FIXTURE_EXTRACTED_ON)
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
    title = compute_regulation_fields(make_frame(row), FIXTURE_EXTRACTED_ON).get_column(
        "regulation_title"
    )[0]
    assert title == "Arrêté n° 2024P10001 — Réglementant le stationnement"


@pytest.mark.parametrize("raw_title", [None, "", "   ", "<p></p>", "&nbsp;"])
def test_an_empty_title_is_built_rather_than_blocking(raw_title):
    """A title is never a reason to drop a regulation."""
    row = make_row(a_title_html=raw_title)
    title = compute_regulation_fields(make_frame(row), FIXTURE_EXTRACTED_ON).get_column(
        "regulation_title"
    )[0]
    assert title == "Arrêté n° 2024P10001"


@pytest.mark.parametrize("length, truncated", [(200, False), (400, True)])
def test_the_title_is_truncated_to_the_api_limit(length, truncated):
    row = make_row(a_title_html="a" * length)
    title = compute_regulation_fields(make_frame(row), FIXTURE_EXTRACTED_ON).get_column(
        "regulation_title"
    )[0]
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
    df = compute_regulation_fields(make_frame(row), FIXTURE_EXTRACTED_ON)
    assert df.get_column("regulation_subject")[0] == subject
    assert df.get_column("regulation_other_category_text")[0] == text


def test_a_permanent_regulation_says_so_in_its_subject():
    df = compute_regulation_fields(make_frame(make_row()), FIXTURE_EXTRACTED_ON)
    assert df.get_column("regulation_subject")[0] == PostApiRegulationsAddBodySubject.OTHER.value
    assert df.get_column("regulation_other_category_text")[0] == PERMANENT_SUBJECT_TEXT
    assert (
        df.get_column("regulation_category")[0]
        == PostApiRegulationsAddBodyCategory.PERMANENTREGULATION.value
    )


def test_the_other_category_text_stays_within_a_hundred_characters():
    row = make_row(a_type="Temporaire", a_reason="R" * 200, a_end_date=date(2026, 12, 1))
    df = compute_regulation_fields(make_frame(row), FIXTURE_EXTRACTED_ON)
    assert len(df.get_column("regulation_other_category_text")[0]) == 100


def test_no_document_url_is_invented(clean):
    assert clean.get_column("regulation_document_url").null_count() == clean.height


def test_an_unknown_regulation_type_is_dropped_not_the_run(captured_logs):
    rows = make_frame(make_row(), make_row(a_identifier="2024P10002", a_type="Provisoire"))
    kept = compute_regulation_fields(rows, FIXTURE_EXTRACTED_ON)
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
    df = compute_measure_fields(compute_regulation_fields(make_frame(row), FIXTURE_EXTRACTED_ON))
    if expected is None:
        assert df.height == 0
    else:
        assert df.get_column("measure_type_").to_list() == [expected]


def test_the_catalog_table_is_complete():
    """32 values in catalog `1202` on 2026-09-08; a 33rd has to be qualified by hand."""
    assert len(MEASURE_TYPE_BY_LABEL) == 32


def test_an_unknown_measure_label_is_dropped_with_a_warning(captured_logs):
    """Paris can enrich the catalog: a new value is never guessed, nor stops the run."""
    unknown = make_row(m_file_id=11, m_type="interdiction de klaxonner", l_file_id=None)
    known = make_row()

    df = compute_measure_fields(
        compute_regulation_fields(make_frame(unknown, known), FIXTURE_EXTRACTED_ON)
    )

    assert df.get_column("m_file_id").to_list() == [10]
    dropped = [line for line in captured_logs if "unknown to catalog 1202" in line]
    assert len(dropped) == 1
    assert "'interdiction de klaxonner': 1" in dropped[0]


def test_measures_outside_the_model_are_dropped_and_counted(raw, captured_logs):
    compute_measure_fields(compute_regulation_fields(raw, FIXTURE_EXTRACTED_ON))
    dropped = [line for line in captured_logs if "without a DiaLog equivalent" in line]
    assert len(dropped) == 1
    assert "'stationnement réservé': 6" in dropped[0]


def test_rows_without_any_measure_are_dropped_and_counted(captured_logs):
    df = compute_measure_fields(
        compute_regulation_fields(
            make_frame(make_row(m_type=None, m_params=None)), FIXTURE_EXTRACTED_ON
        )
    )
    assert df.height == 0
    assert any("rows without any measure" in line for line in captured_logs)


@pytest.mark.parametrize(
    "value, expected", [("à 30 km/h", 30), ("30 km/h", 30), ("à 50 km/h", 50), ("50 km/h", 50)]
)
def test_the_speed_is_read_from_its_parameter(value, expected):
    row = make_row(m_type="limitation de vitesse", m_params=[["valeur de la vitesse", value]])
    df = compute_measure_fields(compute_regulation_fields(make_frame(row), FIXTURE_EXTRACTED_ON))
    assert df.get_column("measure_max_speed").to_list() == [expected]


@pytest.mark.parametrize("params", [None, [["valeur de la vitesse", ""]]])
def test_a_speed_limitation_without_a_speed_is_dropped(params, captured_logs):
    """30 or 50 is a decision, not a default."""
    row = make_row(m_type="limitation de vitesse", m_params=params)
    df = compute_measure_fields(compute_regulation_fields(make_frame(row), FIXTURE_EXTRACTED_ON))
    assert df.height == 0
    assert any("without a readable 'valeur de la vitesse'" in line for line in captured_logs)


def test_zone_30_carries_its_speed_in_its_name():
    row = make_row(m_type="zone 30", m_params=None)
    df = compute_measure_fields(compute_regulation_fields(make_frame(row), FIXTURE_EXTRACTED_ON))
    assert df.get_column("measure_type_").to_list() == [MeasureTypeEnum.SPEEDLIMITATION.value]
    assert df.get_column("measure_max_speed").to_list() == [30]


def test_no_speed_leaks_onto_another_measure_type():
    row = make_row(
        m_type="interdiction de stationnement", m_params=[["valeur de la vitesse", "à 30 km/h"]]
    )
    df = compute_measure_fields(compute_regulation_fields(make_frame(row), FIXTURE_EXTRACTED_ON))
    assert df.get_column("measure_max_speed").to_list() == [None]


@pytest.mark.parametrize("label", ["sens interdit (ou sens unique)", "mise en impasse"])
def test_one_ways_and_dead_ends_are_out_of_the_model(label, captured_logs):
    """No one-way until DiaLog carries the side; a dead end is not a closure of the street."""
    row = make_row(m_type=label, l_direction="du début vers la fin du segment")
    assert pipeline(row).height == 0
    assert any("without a DiaLog equivalent" in line and label in line for line in captured_logs)


def test_the_one_way_of_the_fixture_is_dropped(clean):
    assert "05-00133" not in set(clean.get_column("a_identifier"))


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
    "publish, kept",
    [(True, {"2024P10001", "2024P10002", "2024P10003"}), (False, {"2024P10001", "2024P10003"})],
)
def test_the_police_prefecture_is_published_only_behind_its_constant(publish, kept):
    """A regulation with no service is not the prefecture's."""
    df = make_frame(
        make_row(),
        make_row(a_identifier="2024P10002", a_service="Préfecture de Police"),
        make_row(a_identifier="2024P10003", a_service=None),
    )
    assert set(filter_police_prefecture(df, publish)["a_identifier"]) == kept


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
        make_row(a_identifier="2024T11570"),
        make_row(a_identifier="2024P12345"),
    )
    numbers = index_by_eudonet_number(
        [("Paris_2024P12345", "Paris (préfecture)"), ("2024T11570", "Paris")]
    )
    kept = discard_published_elsewhere(df, numbers)
    assert set(kept["a_identifier"]) == set(df["a_identifier"]) - {"2024T11570", "2024P12345"}
    assert discard_published_elsewhere(df, None).height == df.height


def test_a_single_point_carries_only_a_parking_ban():
    """A ban, a speed or a gauge stands on a stretch of road, not on a house number."""
    df = pl.DataFrame(
        {
            "a_identifier": ["1", "2", "3"],
            "l_scope": ["Un point", "Un point", "Une section"],
            "measure_type_": ["parkingProhibited", "noEntry", "noEntry"],
        }
    )
    assert discard_points_beyond_parking(df)["a_identifier"].to_list() == ["1", "3"]


def test_a_refused_location_is_dropped_and_its_regulation_goes_with_the_others(monkeypatch):
    df = make_frame(
        make_row(l_file_id=10),
        make_row(l_file_id=11),
        make_row(a_identifier="2024P10002", l_file_id=20),
    )
    refused = {11: "Cette adresse n'est pas reconnue", 20: "Les deux rues ne se croisent pas"}
    monkeypatch.setattr(location_filters, "load_refused_locations", lambda: refused)
    kept = discard_refused_locations(df)
    assert kept["l_file_id"].to_list() == [10]
    assert set(kept["a_identifier"]) == {"2024P10001"}
    monkeypatch.setattr(location_filters, "load_refused_locations", lambda: {})
    assert discard_refused_locations(df).height == df.height


def test_a_location_complement_that_narrows_the_measure_drops_the_location(captured_logs):
    """A complement like "sur 2 places", or a day and an hour, is not read, so the location goes."""
    df = make_frame(
        make_row(l_file_id=10),
        make_row(l_file_id=11, l_point_location_complement="sur 2 places.,"),
        make_row(l_file_id=12, l_from_location_complement="<p>les mardis de 14h30 à 16h30</p>"),
        make_row(
            a_identifier="2024P10002", l_file_id=20, l_to_location_complement="dans la contre-allée"
        ),
    )
    kept = discard_location_complements(df)
    assert kept["l_file_id"].to_list() == [10]
    assert set(kept["a_identifier"]) == {"2024P10001"}
    assert any(
        "complement narrows" in line and "1 left with no location" in line for line in captured_logs
    )


@pytest.mark.parametrize(
    "complement",
    [
        None,
        "",
        "Bis,",
        ", BIS",
        "et jusqu'à",
        "sur toute la longueur de la voie.",
        "Sur toutes les places.",
        "& n°13",
        "56-56bis",
        "n° 34 à 36 ter",
        "<!--[if gte mso 9]><xml><w:Word>Normal</w:Word></xml><![endif]-->",
    ],
)
def test_a_location_complement_that_says_nothing_keeps_the_location(complement):
    df = make_frame(make_row(l_point_location_complement=complement))
    assert discard_location_complements(df).height == 1


@pytest.mark.parametrize(
    "complement", ["de 8h à 18h", "8h-18h", "côté pair", "1 place", "du 02/11/2026 au 31/08/2027"]
)
def test_a_short_complement_that_narrows_the_measure_drops_the_location(complement):
    df = make_frame(make_row(l_point_location_complement=complement))
    assert discard_location_complements(df).height == 0


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
    """The label never says which dimension, and none of the 26 325 measures of 2026-09-08
    carries a length, height or width beside it."""
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


@pytest.mark.parametrize("label", ["cycles", "aux véhicules de plus de 12 T"])
def test_a_restriction_outside_the_closed_list_drops_its_measure(label, captured_logs):
    """Cycles are never restricted; a new value waits for a human."""
    row = make_row(m_type="limitation catégorielle", m_params=[["véhicule concerné (1)", label]])
    assert pipeline(row).height == 0
    assert any("outside the closed list" in line and label in line for line in captured_logs)


@pytest.mark.parametrize("label", ["limitation dimensionnelle", "limitation catégorielle"])
def test_an_unreadable_gauge_is_dropped_and_counted(label, captured_logs):
    """Neither a closure for everyone, nor "other" with no threshold."""
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


def test_a_temporary_regulation_longer_than_a_year_is_kept_but_counted(captured_logs):
    row = make_row(
        a_type="Temporaire",
        a_reason="Travaux",
        a_start_date=date(2026, 1, 1),
        a_end_date=date(2028, 1, 1),
    )
    assert pipeline(row).height == 1
    assert any("last more than a year" in line for line in captured_logs)


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
    assert raw.height == 79
    after_state = compute_regulation_fields(raw, FIXTURE_EXTRACTED_ON)
    assert after_state.height == 76  # 1 Abrogé + 1 Non signé + 1 Périmé
    after_type = compute_measure_fields(after_state)
    assert after_type.height == 61
    assert clean.height == 58  # 2025T15489 and 2026T17998, by the date controls
    assert dict(
        zip(*clean.get_column("measure_type_").value_counts(sort=True).to_dict().values())
    ) == {
        MeasureTypeEnum.PARKINGPROHIBITED.value: 47,
        MeasureTypeEnum.NOENTRY.value: 10,
        MeasureTypeEnum.SPEEDLIMITATION.value: 1,
    }
