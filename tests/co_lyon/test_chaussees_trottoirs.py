"""Tests for the Métropole de Lyon roadway integration.

The fixture is twelve hand-written segments, one per case the source really contains:
a numbered order spanning several segments, a pedestrian area, a dimension limit sitting
on a 50 km/h road, an order number annotated as a mere project, and a segment without
geometry.
"""

import json

import polars as pl
import pytest

from integrations.base_integration import BaseIntegration
from integrations.co_lyon.chaussees_trottoirs.data_source_integration import (
    DataSourceIntegration,
    chain_segments,
)
from integrations.co_lyon.chaussees_trottoirs.regulation_key import (
    is_project,
    mentions_dimensions,
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


def test_order_key_collapses_spelling_variants():
    """The same act typed two ways must not become two regulations."""
    assert order_key("2021 355RGC") == order_key("2021_355RGC") == "2021355RGC"
    assert order_key("0_AR_2018.0018") == "0AR20180018"
    assert order_key(None) is None


def test_a_project_annotation_is_recognised():
    assert is_project("Proposition zone apaisable = Zone 30 - Arrêté n°2021-055 le 02/06/21")
    assert not is_project("ZCA : 2022 - Arrêté N°2024RP44520 du 29/09/2022")


def test_dimension_mention_is_recognised():
    assert mentions_dimensions("Arrêté N°0_AR_2018.0018 du 11/09/2019 - Tonnage")
    assert not mentions_dimensions("ZCA : 2022 - Arrêté N°2024RP44520 du 29/09/2022")


# --- Grouping rows into measures and regulations (R-28) -----------------------------


def test_rows_sharing_an_order_number_land_in_one_regulation(clean_data):
    """Two segments of the same order are two emprises, not two regulations."""
    order = clean_data.filter(pl.col("regulation_identifier") == "MGL-CT-2024RP44520")
    assert order.height == 2
    assert set(order["measure_group_key"]) == {"V30"}


def test_a_numbered_order_carries_only_the_measure_it_decides(clean_data):
    """An order regulates one thing; its number is merely typed on every segment.

    The fixture's 2024RP44520 covers two 30 km/h segments and one pedestrian area. The
    pedestrian area is a different act that happens to sit inside the calmed-traffic
    zone, so it leaves for the metropolitan regulation of its own measure rather than
    making the order say something it never said.
    """
    order = clean_data.filter(pl.col("regulation_identifier") == "MGL-CT-2024RP44520")
    assert set(order["measure_group_key"]) == {"V30"}

    area = clean_data.filter(pl.col("measure_group_key") == "AIRE_PIETONNE")
    assert set(area["regulation_identifier"]) == {"MGL-CT-AIRE_PIETONNE"}
    assert any("Victor Hugo" in label for label in area["location_label"])


def test_a_displaced_measure_drops_the_order_name_from_its_title(clean_data):
    """A metropolitan regulation must not read "Arrêté n°…" — it is not that act."""
    area = clean_data.filter(pl.col("measure_group_key") == "AIRE_PIETONNE")
    assert set(area["regulation_title"]) == {"Aire piétonne – Métropole de Lyon"}


def test_rows_without_an_order_number_group_by_measure_across_the_metropolis(clean_data):
    """Three segments, three municipalities, one 30 km/h regulation."""
    fallback = clean_data.filter(pl.col("regulation_identifier") == "MGL-CT-V30")
    assert fallback.height == 3
    assert fallback["measure_group_key"].n_unique() == 1
    assert fallback["regulation_title"][0] == "Limitation de vitesse à 30 km/h – Métropole de Lyon"


def test_a_project_annotation_detaches_the_order_but_keeps_the_measure(clean_data):
    """The cited order is a project: its 30 km/h joins the metropolitan fallback."""
    assert clean_data.filter(pl.col("regulation_identifier") == "MGL-CT-2021055").height == 0
    labels = clean_data.filter(pl.col("regulation_identifier") == "MGL-CT-V30")["location_label"]
    assert any("Ambroise Croizat" in label for label in labels)


def test_the_default_urban_speed_is_discarded_but_not_its_segment(clean_data):
    """50 km/h is nobody's decision — yet the tonnage on the same segment is."""
    assert not any("Route de Vienne" in label for label in clean_data["location_label"])
    tonnage = clean_data.filter(pl.col("regulation_identifier") == "MGL-CT-GABARIT_T3_5")
    assert tonnage.height == 2
    assert set(tonnage["measure_type_"]) == {"noEntry"}
    assert tonnage["vehicle_heavyweight_max_weight"][0] == 3.5


def test_a_segment_can_feed_two_measures_that_land_in_different_regulations(clean_data):
    """One segment, a speed limit and a dimension limit — but one act decides only one.

    The order's text says "Tonnage", so the dimension measure is the one it carries; the
    70 km/h on the same segment joins the metropolitan regulation of its speed. The same
    segment therefore appears in two regulations, which is correct: two distinct
    restrictions at the same place.
    """
    numbered = clean_data.filter(pl.col("regulation_identifier") == "MGL-CT-0AR20180018")
    assert set(numbered["measure_group_key"]) == {"GABARIT_T19_0_H4_5"}

    speed = clean_data.filter(pl.col("measure_group_key") == "V70")
    assert set(speed["regulation_identifier"]) == {"MGL-CT-V70"}
    assert any("Rue du Pont" in label for label in speed["location_label"])
    assert any("Rue du Pont" in label for label in numbered["location_label"])


def test_a_dimension_limit_is_only_attached_to_an_order_that_mentions_it(clean_data):
    """Otherwise it would make a calmed-traffic-zone order say something it never said."""
    detached = clean_data.filter(pl.col("regulation_identifier") == "MGL-CT-GABARIT_H3_9")
    assert detached.height == 1
    assert detached["vehicle_max_height"][0] == 3.9


def test_a_pedestrian_area_is_a_ban_not_a_five_kilometre_speed_limit(clean_data):
    area = clean_data.filter(pl.col("measure_group_key") == "AIRE_PIETONNE")
    assert area.height == 2
    assert set(area["measure_type_"]) == {"noEntry"}
    assert area["measure_max_speed"].null_count() == area.height


def test_a_segment_without_geometry_is_dropped(clean_data):
    assert not any("Sans Geometrie" in label for label in clean_data["location_label"])
    assert clean_data["location_geometry"].null_count() == 0


def test_every_period_starts_today_and_never_ends(clean_data):
    """R-39: we never presume a commencement date the source does not carry.

    The start is French midnight carrying the offset of that day — the one shape DiaLog
    reads without shifting it (`integrations/local_time.py`).
    """
    import datetime
    from zoneinfo import ZoneInfo

    today = datetime.datetime.combine(
        datetime.date.today(), datetime.time(), tzinfo=ZoneInfo("Europe/Paris")
    ).isoformat()
    assert today.endswith(("+01:00", "+02:00"))
    assert set(clean_data["period_start_date"]) == {today}
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


# --- Pedestrian areas that are not on a road ----------------------------------------


def test_a_pedestrian_street_is_kept(clean_data):
    """Rue Saint Jean is the Vieux Lyon: a real ban a driver must not ignore."""
    labels = clean_data.filter(pl.col("measure_group_key") == "AIRE_PIETONNE")["location_label"]

    assert any("Saint Jean" in label for label in labels)


def test_a_private_lane_is_not_a_pedestrian_area_we_publish():
    """`domanialite` says private: it is not roadway open to traffic."""
    kept = _pedestrian_labels()

    assert not any("Tilleuls" in label for label in kept)


def test_a_nameless_segment_is_dropped():
    """295 segments read « Voie sans dénomination » — nothing to tell a driver."""
    assert not any("sans dénomination" in label for label in _pedestrian_labels())


def test_a_rural_track_is_dropped():
    """A towpath, a park promenade or a rural track is not a road."""
    assert not any("Chemin Rural" in label for label in _pedestrian_labels())


def test_the_filter_only_touches_pedestrian_areas(clean_data):
    """A private or nameless road keeps its speed limit and its tonnage limit.

    The filter runs after the measures are qualified, so it can only ever remove an
    `AIRE_PIETONNE`: being private says nothing about whether a 30 km/h limit is real.
    """
    others = clean_data.filter(pl.col("measure_group_key") != "AIRE_PIETONNE")

    assert others.height > 0
    assert "GABARIT_T3_5" in set(others["measure_group_key"])


def _pedestrian_labels() -> list[str]:
    source = DataSourceIntegration.__new__(DataSourceIntegration)
    clean = source.compute_clean_data(source.validate_raw_data(pl.read_csv(FIXTURE)))
    return clean.filter(pl.col("measure_group_key") == "AIRE_PIETONNE")["location_label"].to_list()
