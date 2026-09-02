"""Unit tests for the Lyon "chantiers perturbants" transformation."""

import json

import polars as pl
import pytest
from loguru import logger

from integrations.co_lyon.chantiers_perturbants.data_source_integration import (
    LONG_DURATION_WARNING_DAYS,
    compute_location_fields,
    compute_measure_fields,
    compute_period_fields,
    compute_regulation_fields,
    compute_time_slot_fields,
    compute_vehicle_fields,
)

GEOMETRY = json.dumps(
    {"type": "MultiPolygon", "coordinates": [[[[4.83, 45.76], [4.84, 45.76], [4.83, 45.77]]]]}
)


def row(**overrides):
    base = {
        "gid": 415731,
        "nom": "Rue Docteur Bouchut",
        "nomchantier": "Travaux de voirie",
        "commune1": "Lyon 3e Arrondissement",
        "insee": "69383",
        "precisionlocalisation": "Entre la rue Garibaldi et la rue du Lac",
        "debutchantier": "2026-08-25",
        "finchantier": "2026-10-09",
        "descripchantierinternet": None,
        "typeperturbation": "Circulation interdite",
        "url_document": "",
        "geometry": GEOMETRY,
    }
    base.update(overrides)
    return base


def frame(*rows):
    return pl.DataFrame(list(rows) or [row()]).with_columns(
        [
            pl.col("debutchantier").cast(pl.Utf8).str.to_date("%Y-%m-%d", strict=False),
            pl.col("finchantier").cast(pl.Utf8).str.to_date("%Y-%m-%d", strict=False),
        ]
    )


def timed(*rows):
    """The frame as `compute_period_fields` receives it: time slots already read."""
    return compute_time_slot_fields(frame(*rows))


@pytest.fixture
def warnings():
    """Collect the warnings a transformation emits, so silence can be asserted too."""
    messages: list[str] = []
    sink = logger.add(lambda message: messages.append(message), level="WARNING")
    yield messages
    logger.remove(sink)


# --- measure type ---------------------------------------------------------------


@pytest.mark.parametrize(
    "perturbation,expected",
    [
        ("Circulation interdite", "noEntry"),
        ("Circulation interdite de jour", "noEntry"),
        ("Circulation interdite de nuit", "noEntry"),
        ("Circulation alternée", "alternateRoad"),
        ("Circulation alternée de jour", "alternateRoad"),
    ],
)
def test_maps_known_perturbations(perturbation, expected):
    result = compute_measure_fields(frame(row(typeperturbation=perturbation)))
    assert result.get_column("measure_type_").to_list() == [expected]


@pytest.mark.parametrize(
    "perturbation",
    [
        "Circulation réduite",  # narrowed carriageway: no DiaLog type (R-32 bis)
        "Circulation réduite de nuit",
        "Circulation sens unique",  # would need a direction the layer lacks (R-32)
        "Une valeur jamais vue",
    ],
)
def test_drops_unmappable_perturbations(perturbation):
    result = compute_measure_fields(frame(row(typeperturbation=perturbation)))
    assert result.height == 0


def test_speed_is_never_invented():
    result = compute_measure_fields(frame())
    assert result.get_column("measure_max_speed").to_list() == [None]


# --- time slots -----------------------------------------------------------------


@pytest.mark.parametrize(
    "description,expected",
    [
        ("de 08h00 à 18h00", [("08:00", "18:00")]),
        ("7h-17h", [("07:00", "17:00")]),
        ("7h30 - 16h", [("07:30", "16:00")]),
        ("entre 6h et 17h", [("06:00", "17:00")]),
        ("de 09:00 à 16:30", [("09:00", "16:30")]),
        ("De 7h à 17h, sens Sud/Nord", [("07:00", "17:00")]),
        # Night work: the end precedes the start, which is how it is expressed.
        ("De 20h à 5h le lendemain", [("20:00", "05:00")]),
        # Several windows in one description, `/` separating them.
        (
            "11h-15h / 19h-00h / 00h-1h",
            [("11:00", "15:00"), ("19:00", "00:00"), ("00:00", "01:00")],
        ),
        # The same window stated twice is one window.
        ("de 09:00 à 16:00, puis de 09:00 à 16:00", [("09:00", "16:00")]),
        # Not hours: a direction, a lone reading, an impossible clock.
        ("Sens Ouest-Est", []),
        ("à partir de 8h", []),
        ("de 25h à 30h", []),
        (None, []),
    ],
)
def test_reads_hours_out_of_free_text(description, expected):
    result = timed(
        row(typeperturbation="Circulation interdite", descripchantierinternet=description)
    )
    slots = result.get_column("time_slot_clocks")[0].to_list()
    assert [(slot["start"], slot["end"]) for slot in slots] == expected


def test_drops_partial_day_restrictions_without_hours(warnings):
    """Publishing them around the clock would state what the source contradicts."""
    result = timed(
        row(typeperturbation="Circulation interdite de jour", descripchantierinternet=None)
    )
    assert result.height == 0
    assert any("part of the day" in message for message in warnings)


def test_keeps_partial_day_restrictions_whose_hours_are_readable():
    result = timed(
        row(
            typeperturbation="Circulation interdite de nuit",
            descripchantierinternet="De 21:00 à 05:00",
        )
    )
    assert result.height == 1


def test_uses_hours_even_when_the_type_does_not_declare_them():
    """`Circulation interdite` + `7h-17h` is not a round-the-clock closure."""
    result = timed(row(typeperturbation="Circulation interdite", descripchantierinternet="7h-17h"))
    assert result.height == 1
    assert result.get_column("time_slot_clocks")[0].to_list() == [
        {"start": "07:00", "end": "17:00"}
    ]


def test_a_type_without_hours_stays_round_the_clock():
    result = timed(row(typeperturbation="Circulation interdite", descripchantierinternet=None))
    assert result.height == 1
    assert result.get_column("time_slot_clocks")[0].to_list() == []


# --- period ---------------------------------------------------------------------


def test_period_is_temporary_and_bounded():
    result = compute_period_fields(timed())
    assert result.get_column("period_start_date").to_list() == ["2026-08-25T00:00:00+02:00"]
    # Closing on midnight would end the regulation as its final day begins.
    assert result.get_column("period_end_date").to_list() == ["2026-10-09T23:59:59+02:00"]
    assert result.get_column("period_is_permanent").to_list() == [False]
    assert result.get_column("period_recurrence_type").to_list() == ["everyDay"]


def test_period_carries_the_offset_of_its_own_day():
    """A winter site must say +01:00, or DiaLog shifts it by an hour."""
    result = compute_period_fields(timed(row(debutchantier="2026-01-15", finchantier="2026-02-10")))
    assert result.get_column("period_start_date").to_list() == ["2026-01-15T00:00:00+01:00"]


def test_time_slots_are_anchored_on_the_start_day():
    result = compute_period_fields(
        timed(row(descripchantierinternet="de 8h à 17h30", debutchantier="2026-08-25"))
    )
    assert result.get_column("period_time_slots")[0].to_list() == [
        {"start_time": "2026-08-25T08:00:00+02:00", "end_time": "2026-08-25T17:30:00+02:00"}
    ]


def test_a_measure_without_hours_carries_no_time_slot():
    result = compute_period_fields(timed())
    assert result.get_column("period_time_slots")[0].to_list() == []


@pytest.mark.parametrize(
    "start,end",
    [
        (None, "2026-10-09"),
        ("2026-08-25", None),
        ("2026-10-09", "2026-08-25"),  # ends before it starts
    ],
)
def test_period_drops_broken_dates(start, end):
    result = compute_period_fields(timed(row(debutchantier=start, finchantier=end)))
    assert result.height == 0


def test_multi_year_work_sites_are_reported_but_published(warnings):
    """Long durations are a judgement call, so they leave through a warning."""
    result = compute_period_fields(timed(row(debutchantier="2023-01-01", finchantier="2026-06-01")))
    assert result.height == 1
    assert any("Very long temporary restriction detected" in message for message in warnings)
    assert any("gid 415731" in message for message in warnings)
    assert LONG_DURATION_WARNING_DAYS == 365


def test_a_short_work_site_raises_no_duration_warning(warnings):
    compute_period_fields(timed(row(debutchantier="2026-01-01", finchantier="2026-03-01")))
    assert not any("Very long" in message for message in warnings)


# --- location, regulation, vehicles ---------------------------------------------


def test_location_passes_geometry_through_and_builds_a_label():
    result = compute_location_fields(frame())
    assert result.get_column("location_road_type").to_list() == ["rawGeoJSON"]
    assert json.loads(result.get_column("location_geometry")[0])["type"] == "MultiPolygon"
    assert result.get_column("location_label")[0] == (
        "Rue Docteur Bouchut – Lyon 3e Arrondissement (Entre la rue Garibaldi et la rue du Lac)"
    )


def test_location_drops_rows_without_geometry():
    assert compute_location_fields(frame(row(geometry=None))).height == 0


def test_identifier_is_the_gid_and_carries_no_commune():
    """A commune corrected afterwards must not create a second regulation."""
    lyon = compute_regulation_fields(frame(row(gid=415731, commune1="Francheville")))
    moved = compute_regulation_fields(frame(row(gid=415731, commune1="Tassin la Demi Lune")))
    assert lyon.get_column("regulation_identifier")[0] == "MGL-CHP-415731"
    assert (
        lyon.get_column("regulation_identifier")[0] == moved.get_column("regulation_identifier")[0]
    )


def test_identifier_namespace_is_isolated_from_the_other_channel():
    """813 Lyon regulations already reach DiaLog through another channel."""
    identifier = compute_regulation_fields(frame()).get_column("regulation_identifier")[0]
    for existing in ("LYON_2022RP40610", "VAULX_EN_VELIN_1378", "2023RP43879"):
        assert not existing.startswith("MGL-CHP-")
        assert identifier != existing


def test_regulation_fields_are_temporary_roadmaintenance():
    result = compute_regulation_fields(frame())
    assert result.get_column("regulation_category")[0] == "temporaryRegulation"
    assert result.get_column("regulation_subject")[0] == "roadMaintenance"
    assert result.get_column("regulation_title")[0] == "Travaux de voirie – Rue Docteur Bouchut"
    # `url_document` is empty on every row today, and empty is not a URL.
    assert result.get_column("regulation_document_url")[0] is None


def test_all_vehicles_by_default():
    result = compute_vehicle_fields(frame())
    assert result.get_column("vehicle_all_vehicles").to_list() == [True]
