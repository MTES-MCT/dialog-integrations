"""Tests for the Métropole de Lyon roadway integration.

The fixture is twenty-six hand-written segments, one per case the source really contains:
a numbered order spanning several segments, one carrying two measures on all of them, one
whose measures disagree from segment to segment, a pedestrian area labelled or not, a
dimension limit sitting on a 50 km/h road or on an 80 km/h one, an order number annotated
as a mere project, a zone à trafic limité, and a segment without geometry.
"""

import json

import polars as pl
import pytest

from integrations.base_integration import BaseIntegration
from integrations.co_lyon.chaussees_trottoirs.data_source_integration import (
    DataSourceIntegration,
    chain_segments,
    compute_measure_fields,
    discard_impossible_tonnages,
    discard_pedestrian_areas_off_the_road_network,
    explode_into_measures,
    read_order_number,
)
from integrations.co_lyon.chaussees_trottoirs.regulation_key import (
    foreign_order_keys,
    is_project,
    order_key,
    order_number,
)
from integrations.co_lyon.integration import Integration

FIXTURE = "tests/co_lyon/chaussees_trottoirs.csv"


def identifier_of(regulation) -> str:
    """Narrow the generated client's `str | Unset` down to the string we always send."""
    assert isinstance(regulation.identifier, str)
    return regulation.identifier


def measures_of(regulation) -> list:
    """Same, for the measures list."""
    assert isinstance(regulation.measures, list)
    return regulation.measures


@pytest.fixture
def clean_data() -> pl.DataFrame:
    source = DataSourceIntegration.__new__(DataSourceIntegration)
    raw = pl.read_csv(FIXTURE)
    validated = source.validate_raw_data(raw)
    return source.select_regulation_measure_fields(source.compute_clean_data(validated))


@pytest.fixture
def regulations(clean_data: pl.DataFrame):
    integration = Integration.__new__(Integration)
    return integration.create_regulations(clean_data, DataSourceIntegration)


# --- Reading the order number out of the free text ----------------------------------


@pytest.mark.parametrize(
    "text,expected",
    [
        ("ZCA : 2022 - Arrêté N°2024RP44520 du 29/09/2022", "2024RP44520"),
        ("Arrêté n°2024CIR177610A1 du 10/10/24", "2024CIR177610A1"),
        ("Arrêté N°UC 22-272 du 12/05/22", "UC 22-272"),
        ("Arrêté n°VN-2021-AP-001 le 19/10/21", "VN-2021-AP-001"),
        ("ZCA : 2017", None),
        (None, None),
        ("", None),
    ],
)
def test_order_number_absorbs_the_observed_spellings(text, expected):
    assert order_number(text) == expected


@pytest.mark.parametrize(
    "text,expected",
    [
        # The field stacks the regulatory layers of a segment, newest first. The first
        # citation is the order in force, whatever separates it from the next one: a full
        # stop, a dash or a line break.
        (
            "ZCA : 2022 - Arrêté N°2024RP44520. ZCA : 2019 - Arrêté N°2019RP36023 le 25/07/19",
            "2024RP44520",
        ),
        (
            "ZCA : 2022 - Arrêté N°2024RP44520 - ZCA : 2022 - Arrêté N°2022RP41281 du 17/05/2022",
            "2024RP44520",
        ),
        (
            "ZCA : Arrêté n° 2025RP48763 - 2022 - Arrêté N°2024RP44520. ZCA : 2018",
            "2025RP48763",
        ),
        (
            "ZCA : Arrêté n° 2025RP47402\r\n2022 - Arrêté N°2024RP44520. ZCA : 2019",
            "2025RP47402",
        ),
        # …but a dash inside the number is not a separator. A bare dash delimiter would
        # truncate these into another regulation.
        ("Arrêté n°VOI-2023 - 120 du 28/06/23", "VOI-2023 - 120"),
        ("ZCA : 2023 - Arrêté n°PV 2023 - 478 du 02/11/23", "PV 2023 - 478"),
        # And when the second citation is tied to a measure rather than to a vintage, it
        # is the one that governs: these rows carry the height limit, not the Ville 30.
        (
            "ZCA : 2022 - Arrêté N°2024RP44520 / Lim hauteur - Arrêté N°2022RP40610 du 04/03/2022",
            "2022RP40610",
        ),
    ],
)
def test_a_field_citing_several_orders_keeps_the_one_in_force(text, expected):
    assert order_number(text) == expected


def test_order_key_collapses_spelling_variants():
    """The same act typed two ways must not become two regulations."""
    assert order_key("2021 355RGC") == order_key("2021_355RGC") == "2021355RGC"
    assert order_key("0_AR_2018.0018") == "0AR20180018"
    assert order_key(None) is None


def test_a_project_annotation_is_recognised():
    assert is_project("Proposition zone apaisable = Zone 30 - Arrêté n°2021-055 le 02/06/21")
    assert not is_project("ZCA : 2022 - Arrêté N°2024RP44520 du 29/09/2022")


def test_foreign_order_keys_read_the_number_behind_any_municipality_prefix():
    """The other channel's identifiers are `{MUNICIPALITY}_{number}`, 52 spellings (P-04)."""
    keys = foreign_order_keys(
        [
            "LYON_2022RP40610",
            "VAULX_EN_VELIN_22P010",
            "SAINT-PRIEST_A_2022_0838",
            "CALUIRE_979",
            "ALBIGNY-SUR-SAONE_2022-080",
            "2023-ZFE-006",
            "MGL-CT-V30",
            "MGL-CHP-415731",
        ],
        "MGL-",
    )

    assert {"2022RP40610", "22P010", "A20220838", "979", "2022080", "2023ZFE006"} <= keys
    assert not any(key.startswith("MGL") for key in keys)


def test_foreign_order_keys_cut_at_underscores_only():
    """`2019` is a year typed in front of `TASSIN_2019_145`, not an order of its own.

    The trailing-substring match tried on 2026-08-12 produced three such false
    positives; cutting at underscores keeps the number whole.
    """
    keys = foreign_order_keys(["TASSIN_2019_145", "VENISSIEUX_2024_0213RGC"], "MGL-")

    assert "2019145" in keys and "145" in keys
    assert "2019" not in keys and "2024" not in keys


# --- Grouping rows into measures and regulations (R-28) -----------------------------


def test_rows_sharing_an_order_number_land_in_one_regulation(clean_data):
    """Two segments of the same order are two emprises, not two regulations."""
    order = clean_data.filter(pl.col("regulation_identifier") == "MGL-CT-2024RP44520")
    assert order.height == 2
    assert set(order["measure_group_key"]) == {"V30"}


def test_a_numbered_order_carries_every_measure_present_on_all_its_segments(clean_data):
    """Rue Germain's 2022-080: 20 km/h and 3.5 t on both segments — the order says both.

    Judged one by one, so the tonnage does not compete with the speed: an act can decide
    several things, as long as every segment citing it carries each of them (R-28).
    """
    order = clean_data.filter(pl.col("regulation_identifier") == "MGL-CT-2022080")
    assert set(order["measure_group_key"]) == {"V20", "GABARIT_T3_5"}
    assert set(order["regulation_title"]) == {"Arrêté n°2022-080 – Métropole de Lyon"}


def test_a_measure_missing_from_one_segment_leaves_the_order(clean_data):
    """2025-100: 30 km/h on both segments, 3.5 t on one only.

    The act can be said to decide the 30; nothing says it decides the tonnage. The
    tonnage joins the metropolitan regulation of its own measure, under that name.
    """
    order = clean_data.filter(pl.col("regulation_identifier") == "MGL-CT-2025100")
    assert set(order["measure_group_key"]) == {"V30"}
    assert order.height == 2

    tonnage = clean_data.filter(pl.col("regulation_identifier") == "MGL-CT-GABARIT_T3_5")
    assert any("Rue de la Gare" in label for label in tonnage["location_label"])
    assert set(tonnage["regulation_title"]) == {"Restriction de gabarit 3.5 t – Métropole de Lyon"}


def test_an_order_whose_segments_disagree_carries_nothing(clean_data):
    """2025-200: 30 km/h on one segment, 20 on the other — no majority, no tie-break.

    Both measures are published, each under the metropolitan regulation of its speed;
    the act's name is used for neither.
    """
    assert clean_data.filter(pl.col("regulation_identifier") == "MGL-CT-2025200").height == 0
    thirty = clean_data.filter(pl.col("regulation_identifier") == "MGL-CT-V30")["location_label"]
    twenty = clean_data.filter(pl.col("regulation_identifier") == "MGL-CT-V20")["location_label"]
    assert any("Rue du Marche" in label for label in thirty)
    assert any("Eglise" in label for label in twenty)


def test_a_number_already_published_by_another_channel_is_not_created_again():
    """`ALBIGNY-SUR-SAONE_2022-080` is already in the organisation: one act, one name.

    Our reading of 2022-080 steps aside for the metropolitan regulations of its measures,
    which keep their generic titles (P-04).
    """
    source = DataSourceIntegration.__new__(DataSourceIntegration)
    source.foreign_order_keys = foreign_order_keys(
        ["ALBIGNY-SUR-SAONE_2022-080", "TASSIN_2019_145"], "MGL-"
    )
    validated = source.validate_raw_data(pl.read_csv(FIXTURE))
    clean = source.select_regulation_measure_fields(source.compute_clean_data(validated))

    assert clean.filter(pl.col("regulation_identifier") == "MGL-CT-2022080").height == 0
    germain = clean.filter(pl.col("location_label").str.contains("Rue Germain"))
    assert set(germain["regulation_identifier"]) == {"MGL-CT-V20", "MGL-CT-GABARIT_T3_5"}
    assert not any(title.startswith("Arrêté") for title in germain["regulation_title"])
    # The other numbered orders are untouched.
    assert clean.filter(pl.col("regulation_identifier") == "MGL-CT-2025100").height == 2


def test_rows_without_an_order_number_group_by_measure_across_the_metropolis(clean_data):
    """Four segments, four municipalities, one 30 km/h regulation."""
    fallback = clean_data.filter(pl.col("regulation_identifier") == "MGL-CT-V30")
    assert fallback.height == 4
    assert fallback["measure_group_key"].n_unique() == 1
    assert fallback["regulation_title"][0] == "Limitation de vitesse à 30 km/h – Métropole de Lyon"


def test_a_project_annotation_detaches_the_order_but_keeps_the_measure(clean_data):
    """The cited order is a project: its 30 km/h joins the metropolitan fallback."""
    assert clean_data.filter(pl.col("regulation_identifier") == "MGL-CT-2021055").height == 0
    labels = clean_data.filter(pl.col("regulation_identifier") == "MGL-CT-V30")["location_label"]
    assert any("Ambroise Croizat" in label for label in labels)


def test_the_default_urban_speed_is_published_as_one_metropolitan_regulation(clean_data):
    """A satnav needs the default speed as much as a decided one (R-70).

    It groups under `MGL-CT-V50` like any unnumbered speed; the tonnage on the same
    segment keeps its own regulation.
    """
    fifty = clean_data.filter(pl.col("measure_group_key") == "V50")
    assert set(fifty["regulation_identifier"]) == {"MGL-CT-V50"}
    assert set(fifty["measure_type_"]) == {"speedLimitation"}
    assert set(fifty["measure_max_speed"]) == {50}
    assert any("Route de Vienne" in label for label in fifty["location_label"])
    tonnage = clean_data.filter(pl.col("regulation_identifier") == "MGL-CT-GABARIT_T3_5")
    assert tonnage.height == 3
    assert set(tonnage["measure_type_"]) == {"noEntry"}
    assert tonnage["vehicle_heavyweight_max_weight"][0] == 3.5


def test_the_default_rural_speed_is_published_too(clean_data):
    """80 km/h outside a built-up area goes out like the 50 (R-70) — the tonnage stays."""
    eighty = clean_data.filter(pl.col("measure_group_key") == "V80")
    assert set(eighty["regulation_identifier"]) == {"MGL-CT-V80"}
    assert set(eighty["measure_max_speed"]) == {80}
    tonnage = clean_data.filter(pl.col("regulation_identifier") == "MGL-CT-GABARIT_T7_5")
    assert tonnage.height == 1
    assert any("Strasbourg" in label for label in tonnage["location_label"])


def test_a_segment_can_feed_two_measures_of_one_order(clean_data):
    """One segment, a speed limit and a dimension limit, one order citing it.

    Both measures sit on every segment of the order — there is only one — so the order
    carries both, as two distinct measures of one regulation. The word "Tonnage" in the
    text plays no part: what the act decides is read off its segments, not its wording.
    """
    numbered = clean_data.filter(pl.col("regulation_identifier") == "MGL-CT-0AR20180018")
    assert set(numbered["measure_group_key"]) == {"GABARIT_T19_0_H4_5", "V70"}
    assert all("Rue du Pont" in label for label in numbered["location_label"])
    assert clean_data.filter(pl.col("regulation_identifier") == "MGL-CT-V70").height == 0


def test_a_dimension_limit_without_an_order_joins_the_metropolitan_regulation(clean_data):
    detached = clean_data.filter(pl.col("regulation_identifier") == "MGL-CT-GABARIT_H3_9")
    assert detached.height == 1
    assert detached["vehicle_max_height"][0] == 3.9


def test_a_pedestrian_area_is_a_ban_and_a_walking_pace_limit_in_one_regulation(clean_data):
    """R. 110-2: only the vehicles serving the area enter, and they drive at walking pace.

    Two measures, one regulation — never a `MGL-CT-V5` of its own (decision of 2026-09-22).
    """
    ban = clean_data.filter(pl.col("measure_group_key") == "AIRE_PIETONNE")
    assert ban.height == 2  # Victor Hugo, Saint Jean — labelled, and on a street
    assert set(ban["measure_type_"]) == {"noEntry"}
    assert ban["measure_max_speed"].null_count() == ban.height

    speed = clean_data.filter(pl.col("measure_group_key") == "AIRE_PIETONNE_V5")
    assert speed.height == 2
    assert set(speed["measure_type_"]) == {"speedLimitation"}
    assert set(speed["measure_max_speed"]) == {5}
    assert set(speed["location_label"]) == set(ban["location_label"])
    assert set(speed["regulation_identifier"]) == set(ban["regulation_identifier"])
    assert set(speed["regulation_identifier"]) == {"MGL-CT-AIRE_PIETONNE"}
    assert set(speed["regulation_title"]) == {"Aire piétonne – Métropole de Lyon"}


def test_a_pedestrian_area_carries_the_local_access_exemption_like_a_ztl(clean_data):
    """One enters a pedestrian area to reach an address, not to drive through (R-71)."""
    area = clean_data.filter(pl.col("measure_group_key") == "AIRE_PIETONNE")
    assert area["vehicle_exempted_types"].to_list() == [["desserteLocale"]] * area.height
    assert set(area["vehicle_all_vehicles"]) == {True}


def test_the_walking_pace_limit_applies_to_every_vehicle_let_in(clean_data):
    """The exemption belongs to the ban: whoever enters still drives at 5 km/h."""
    speed = clean_data.filter(pl.col("measure_group_key") == "AIRE_PIETONNE_V5")
    assert speed["vehicle_exempted_types"].null_count() == speed.height
    assert set(speed["vehicle_all_vehicles"]) == {True}


def test_a_segment_without_geometry_is_dropped(clean_data):
    assert not any("Sans Geometrie" in label for label in clean_data["location_label"])
    assert clean_data["location_geometry"].null_count() == 0


def test_every_period_is_permanent_and_undated(clean_data):
    """R-39: we never presume a commencement date the source does not carry.

    The start is left null, not set to the day of the run: the date is part of what the
    synchronization compares, and a run day made every order look modified each
    morning. `integrations/sync/dating.py` dates it when DiaLog is written.
    """
    assert clean_data["period_start_date"].null_count() == clean_data.height
    assert set(clean_data["period_is_permanent"]) == {True}
    assert clean_data["period_end_date"].null_count() == clean_data.height


# --- Building the API payloads -------------------------------------------------------


def test_one_measure_carries_every_segment_it_applies_to(regulations):
    """The whole point of the grouping: N locations under one measure, not N measures."""
    order = next(r for r in regulations if r.identifier == "MGL-CT-2024RP44520")
    speed = next(m for m in order.measures if m.type_ == "speedLimitation")
    assert len(speed.locations) == 2
    assert speed.max_speed == 30
    assert len(speed.periods) == 1


def test_the_geometry_survives_intact(regulations):
    order = next(r for r in regulations if r.identifier == "MGL-CT-2024RP44520")
    speed = next(m for m in order.measures if m.type_ == "speedLimitation")
    geometry = json.loads(speed.locations[0].raw_geo_json.geometry)
    assert geometry["type"] == "LineString"
    longitude, latitude = geometry["coordinates"][0]
    assert 4.4 < longitude < 5.3, "longitude first — swapped axes would land in Somalia"
    assert 45.4 < latitude < 46.0


def test_a_pedestrian_area_reaches_the_api_as_two_measures_of_one_regulation(regulations):
    area = next(r for r in regulations if r.identifier == "MGL-CT-AIRE_PIETONNE")
    by_type = {m.type_: m for m in measures_of(area)}

    assert set(by_type) == {"noEntry", "speedLimitation"}
    assert [str(t) for t in by_type["noEntry"].vehicle_set.exempted_types] == ["desserteLocale"]
    assert by_type["speedLimitation"].max_speed == 5
    assert by_type["speedLimitation"].vehicle_set.to_dict() == {"allVehicles": True}
    assert len(by_type["noEntry"].locations) == len(by_type["speedLimitation"].locations) == 2


def test_a_dimension_measure_restricts_vehicles_rather_than_all_of_them(regulations):
    tonnage = next(r for r in regulations if r.identifier == "MGL-CT-GABARIT_T3_5")
    measure = tonnage.measures[0]
    assert measure.type_ == "noEntry"
    assert measure.vehicle_set.all_vehicles is False
    assert measure.vehicle_set.restricted_types == ["heavyGoodsVehicle"]
    assert measure.vehicle_set.heavyweight_max_weight == 3.5


def test_identifiers_are_prefixed_and_unique(regulations):
    identifiers = [r.identifier for r in regulations]
    assert len(identifiers) == len(set(identifiers))
    assert all(identifier.startswith("MGL-CT-") for identifier in identifiers)


# --- The API ceiling -----------------------------------------------------------------


def test_a_regulation_above_the_ceiling_is_split_into_ordered_slices(clean_data, monkeypatch):
    """Staging refuses a POST past ~1 700 locations, whatever the measure split."""
    monkeypatch.setattr(DataSourceIntegration, "max_locations_per_regulation", 2)
    integration = Integration.__new__(Integration)
    regulations = integration.create_regulations(clean_data, DataSourceIntegration)

    slices = sorted(
        identifier_of(r) for r in regulations if identifier_of(r).startswith("MGL-CT-V30-")
    )
    assert slices == ["MGL-CT-V30-01", "MGL-CT-V30-02"]
    for regulation in regulations:
        assert sum(len(m.locations) for m in measures_of(regulation)) <= 2


def test_slices_follow_the_geographic_order(clean_data):
    """Slicing in source order would scatter each slice over the whole metropolis."""
    fallback = clean_data.filter(pl.col("regulation_identifier") == "MGL-CT-V30")
    ordered = fallback.sort("regulation_split_order")["location_label"].to_list()
    # Villeurbanne sits north-east of Vénissieux: its two segments stay together.
    assert "Vénissieux" not in ordered[0] or "Vénissieux" not in ordered[1]
    assert fallback["regulation_split_order"].n_unique() == fallback.height


def test_without_the_ceiling_nothing_is_split(clean_data):
    integration = Integration.__new__(Integration)

    class Unbounded(DataSourceIntegration):
        max_locations_per_regulation = None

    regulations = integration.create_regulations(clean_data, Unbounded)
    assert all("-01" not in identifier_of(r) for r in regulations)


def test_the_legacy_behaviour_is_untouched(clean_data):
    """A source that does not opt in still gets one measure per row, one location each."""
    integration = Integration.__new__(Integration)

    class Ungrouped(DataSourceIntegration):
        group_locations_by_measure = False
        max_locations_per_regulation = None

    regulations = integration.create_regulations(clean_data, Ungrouped)
    order = next(r for r in regulations if r.identifier == "MGL-CT-2024RP44520")
    assert len(measures_of(order)) == 2
    assert all(len(m.locations) == 1 for m in measures_of(order))


def test_base_integration_without_a_data_source_keeps_the_old_shape(clean_data):
    """`create_regulations(df)` — the signature every other integration relies on."""
    integration = BaseIntegration.__new__(Integration)
    regulations = integration.create_regulations(clean_data)
    order = next(r for r in regulations if r.identifier == "MGL-CT-2024RP44520")
    assert all(len(m.locations) == 1 for m in measures_of(order))


# --- Chaining contiguous segments ---------------------------------------------------


def test_two_segments_meeting_end_to_end_become_one():
    """AB + BC = AC. The layer cuts a street into stretches of pavement; we put it back."""
    ab = [[0.0, 0.0], [1.0, 0.0]]
    bc = [[1.0, 0.0], [2.0, 0.0]]

    assert chain_segments([ab, bc]) == [[[0.0, 0.0], [1.0, 0.0], [2.0, 0.0]]]


def test_a_segment_pointing_the_other_way_still_joins():
    """The producer does not orient its geometries; only the shared point matters."""
    ab = [[0.0, 0.0], [1.0, 0.0]]
    cb = [[2.0, 0.0], [1.0, 0.0]]

    chained = chain_segments([ab, cb])

    assert len(chained) == 1
    assert chained[0][0] == [0.0, 0.0] and chained[0][-1] == [2.0, 0.0]


def test_a_chain_of_three_collapses_to_one():
    segments = [
        [[0.0, 0.0], [1.0, 0.0]],
        [[1.0, 0.0], [2.0, 0.0]],
        [[2.0, 0.0], [3.0, 0.0]],
    ]

    assert chain_segments(segments) == [[[0.0, 0.0], [1.0, 0.0], [2.0, 0.0], [3.0, 0.0]]]


def test_segments_that_do_not_touch_stay_apart():
    """Two disjoint stretches of the same street are not one emprise."""
    far = [[[0.0, 0.0], [1.0, 0.0]], [[5.0, 0.0], [6.0, 0.0]]]

    assert len(chain_segments(far)) == 2


def test_a_junction_stops_the_chain():
    """Three segments meeting: which one continues which would be arbitrary.

    The merge is topological, so it stops where the topology stops deciding.
    """
    segments = [
        [[0.0, 0.0], [1.0, 0.0]],
        [[1.0, 0.0], [2.0, 0.0]],
        [[1.0, 0.0], [1.0, 1.0]],
    ]

    assert len(chain_segments(segments)) == 3


def test_a_closed_loop_is_left_alone():
    """A segment whose two ends are the same point has nothing to be joined to."""
    loop = [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]]]

    assert chain_segments(loop) == loop


def test_chaining_never_loses_a_coordinate():
    """Whatever the merge does, the covered geometry is the same as before."""
    segments = [
        [[0.0, 0.0], [1.0, 0.0]],
        [[2.0, 0.0], [1.0, 0.0]],
        [[9.0, 9.0], [8.0, 8.0]],
    ]

    chained = chain_segments(segments)

    before = {tuple(point) for segment in segments for point in segment}
    after = {tuple(point) for segment in chained for point in segment}
    assert before == after


# --- Zone à trafic limité -----------------------------------------------------------


def test_a_ztl_is_a_ban_with_a_local_access_exemption(clean_data):
    """A ZTL is not a speed: it is "no entry, except to get there"."""
    ztl = clean_data.filter(pl.col("measure_group_key") == "ZTL")

    assert ztl.height == 2
    assert set(ztl["measure_type_"]) == {"noEntry"}
    assert ztl["vehicle_exempted_types"].to_list() == [["desserteLocale"], ["desserteLocale"]]
    assert set(ztl["regulation_identifier"]) == {"MGL-CT-2025ZTL001"}


def test_the_ztl_replaces_the_speed_the_source_files_for_it(clean_data):
    """The source files the Presqu'île at 5, 20 or 30 km/h — how one drives inside it.

    That is not what the sign at the entrance says, and publishing both would put two
    measures on one segment — a redundant `noEntry` for the ones filed at 5 km/h.
    """
    labels = clean_data.filter(pl.col("measure_group_key") == "V30")["location_label"]
    assert not any("Mercière" in label for label in labels)

    areas = clean_data.filter(pl.col("measure_group_key") == "AIRE_PIETONNE")
    assert not any("Tupin" in label for label in areas["location_label"])


def test_a_ztl_measure_reaches_the_api_with_its_exemption(regulations):
    """`desserteLocale` must survive `create_save_vehicle_dto`, not be simplified away."""
    ztl = next(r for r in regulations if r.identifier == "MGL-CT-2025ZTL001")
    measure = measures_of(ztl)[0]

    assert measure.type_ == "noEntry"
    assert measure.vehicle_set.all_vehicles is True
    assert [str(t) for t in measure.vehicle_set.exempted_types] == ["desserteLocale"]


def test_a_measure_without_exemptions_stays_simple(regulations):
    """The simplification path every other source relies on must not change."""
    speed = next(r for r in regulations if r.identifier == "MGL-CT-V30")
    vehicle_set = measures_of(speed)[0].vehicle_set

    assert vehicle_set.all_vehicles is True
    assert vehicle_set.to_dict() == {"allVehicles": True}


# --- Pedestrian areas: what the source calls one -------------------------------------


def test_a_labelled_pedestrian_area_is_kept(clean_data):
    """Rue Saint Jean is the Vieux Lyon: a real ban a driver must not ignore."""
    labels = clean_data.filter(pl.col("measure_group_key") == "AIRE_PIETONNE")["location_label"]

    assert any("Saint Jean" in label for label in labels)


def test_a_labelled_area_off_the_road_network_is_dropped_all_the_same(clean_data):
    """Esplanade Fernand Rude is labelled a pedestrian area, and is not a street.

    The label filter and the name-based filter are chained: a ban in a park or on a
    private lane is noise for a satnav, whatever the producer files.
    """
    labels = clean_data.filter(pl.col("measure_group_key") == "AIRE_PIETONNE")["location_label"]

    assert not any("Fernand Rude" in label for label in labels)


def test_five_kilometres_per_hour_alone_is_not_a_pedestrian_area(clean_data):
    """Segments filed at 5 km/h without the label are never published (R-71).

    Allée des Tilleuls, Voie sans dénomination and Chemin Rural 20 all read 5 km/h and
    carry no `reglementationzca`. Publishing a `noEntry` there would state a driving ban
    the source never states.
    """
    labels = list(
        clean_data.filter(pl.col("measure_group_key") == "AIRE_PIETONNE")["location_label"]
    )

    assert not any("Tilleuls" in label for label in labels)
    assert not any("sans dénomination" in label for label in labels)
    assert not any("Chemin Rural" in label for label in labels)


def test_an_unlabelled_five_kilometre_segment_is_not_published_as_a_speed_limit(clean_data):
    """Dropping the ban must not turn it into a "5 km/h" advisory instead.

    The walking-pace limit is the twin of a published ban, never of a dropped one.
    """
    assert "V5" not in set(clean_data["measure_group_key"])
    assert not any("Tilleuls" in label for label in clean_data["location_label"])
    fives = clean_data.filter(pl.col("measure_max_speed") == 5)
    assert set(fives["measure_group_key"]) == {"AIRE_PIETONNE_V5"}
    assert not any("Fernand Rude" in label for label in fives["location_label"])


def test_the_filter_only_touches_pedestrian_areas(clean_data):
    """A private or nameless road keeps its speed limit and its tonnage limit.

    The filter runs after the measures are qualified, so it can only ever remove an
    `AIRE_PIETONNE`: being private says nothing about whether a 30 km/h limit is real.
    """
    others = clean_data.filter(pl.col("measure_group_key") != "AIRE_PIETONNE")

    assert others.height > 0
    assert "GABARIT_T3_5" in set(others["measure_group_key"])


# --- The name-based rule, on its own ------------------------------------------------


def test_the_name_based_rule_drops_private_nameless_and_non_road_areas():
    """`discard_pedestrian_areas_off_the_road_network`, alone on the qualified measures.

    It drops what its name says — and, as the fixture shows, one labelled area too.
    """
    source = DataSourceIntegration.__new__(DataSourceIntegration)
    qualified = source.validate_raw_data(pl.read_csv(FIXTURE)).pipe(_qualified_measures)

    kept = discard_pedestrian_areas_off_the_road_network(qualified)
    labels = list(kept.filter(pl.col("measure_group_key") == "AIRE_PIETONNE")["nomvoie1"])

    assert "Rue Saint Jean" in labels
    assert "Allée des Tilleuls" not in labels
    assert "Chemin Rural 20" not in labels
    assert "Esplanade Fernand Rude" not in labels  # the label loses to the name


def _qualified_measures(df: pl.DataFrame) -> pl.DataFrame:
    """Raw rows carried up to the point where the two pedestrian rules apply."""
    return (
        df.pipe(read_order_number)
        .pipe(discard_impossible_tonnages)
        .pipe(explode_into_measures)
        .pipe(compute_measure_fields)
    )


# --- perimeter (competence check rebuilt upstream) ---------------------------------


def test_the_organisation_perimeter_is_the_metropole_epci():
    from integrations.co_lyon.chaussees_trottoirs.data_source_integration import (
        ORGANIZATION_PERIMETER,
    )

    assert ORGANIZATION_PERIMETER == ("epci", "200046977")


def test_compute_clean_data_uses_the_injected_perimeter(monkeypatch):
    """A perimeter set on the instance is used as is: no network call."""
    from shapely.geometry import box

    from integrations.co_lyon.chaussees_trottoirs.data_source_integration import (
        DataSourceIntegration,
    )
    from integrations.shared import perimeter as perimeter_module
    from integrations.shared.perimeter import Perimeter

    monkeypatch.setattr(
        perimeter_module.Perimeter, "fetch", lambda *a, **k: pytest.fail("fetch must not be called")
    )
    source = DataSourceIntegration.__new__(DataSourceIntegration)
    source.perimeter = Perimeter.build("epci", "test", [box(4.0, 45.0, 6.0, 47.0)])
    df = pl.DataFrame(
        {
            "codetroncon": ["in", "out"],
            "geometry": [
                json.dumps({"type": "LineString", "coordinates": [[4.8, 45.7], [4.9, 45.8]]}),
                json.dumps({"type": "LineString", "coordinates": [[7.0, 45.7], [7.1, 45.8]]}),
            ],
        }
    )
    assert source.discard_outside_perimeter(df).get_column("codetroncon").to_list() == ["in"]
