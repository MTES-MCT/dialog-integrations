"""From pivot rows to API DTOs: the period, with or without daily time slots; the way rows
fold into measures and regulations."""

import polars as pl

from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.payloads import build_measure, build_period, build_regulations


def period(**overrides):
    measure = {
        "period_start_date": "2026-08-25T00:00:00+02:00",
        "period_end_date": "2026-10-09T23:59:59+02:00",
        "period_recurrence_type": "everyDay",
        "period_is_permanent": False,
        "period_time_slots": None,
    }
    measure.update(overrides)
    return build_period(measure)  # type: ignore[arg-type]


def test_a_period_without_time_slots_applies_around_the_clock():
    """Every existing source goes through this path: it must stay unchanged."""
    dto = period()
    assert dto.time_slots == []
    assert dto.start_time == "2026-08-25T00:00:00+02:00"
    assert dto.end_time == "2026-10-09T23:59:59+02:00"


def test_a_source_that_never_produced_the_column_is_unaffected():
    row = {
        "period_start_date": "2026-08-25T00:00:00+02:00",
        "period_end_date": "2026-10-09T23:59:59+02:00",
        "period_recurrence_type": "everyDay",
        "period_is_permanent": False,
    }
    assert build_period(row).time_slots == []  # type: ignore[arg-type]


def test_time_slots_become_api_objects():
    """The API expects SaveTimeSlotDTO objects, not the plain mappings the pivot holds."""
    dto = period(
        period_time_slots=[
            {"start_time": "2026-08-25T08:00:00+02:00", "end_time": "2026-08-25T17:30:00+02:00"},
            # A slot ending before it starts crosses midnight: night work.
            {"start_time": "2026-08-25T21:00:00+02:00", "end_time": "2026-08-25T05:00:00+02:00"},
        ]
    )
    slots = dto.time_slots
    assert isinstance(slots, list)
    assert [(slot.start_time, slot.end_time) for slot in slots] == [
        ("2026-08-25T08:00:00+02:00", "2026-08-25T17:30:00+02:00"),
        ("2026-08-25T21:00:00+02:00", "2026-08-25T05:00:00+02:00"),
    ]


# --- rows into measures and regulations ------------------------------------------


LINE = '{"type": "LineString", "coordinates": [[4.8, 45.7], [4.9, 45.8]]}'


def rows(*specs: tuple[str, str, int]) -> pl.DataFrame:
    """One SIG-like row per (regulation, measure group key, split order)."""
    return pl.DataFrame(
        [
            {
                "regulation_identifier": identifier,
                "regulation_category": "permanentRegulation",
                "regulation_subject": "other",
                "regulation_title": f"Arrêté {identifier}",
                "regulation_other_category_text": "Arrêté",
                "measure_type_": "speedLimitation",
                "measure_max_speed": 30,
                "measure_group_key": key,
                "regulation_split_order": order,
                "period_start_date": "2026-01-01T00:00:00+01:00",
                "period_end_date": None,
                "period_recurrence_type": "everyDay",
                "period_is_permanent": True,
                "location_road_type": "rawGeoJSON",
                "location_label": f"segment {order}",
                "location_geometry": LINE,
                "vehicle_all_vehicles": True,
            }
            for identifier, key, order in specs
        ]
    )


def regulations(df: pl.DataFrame, **opt_ins):
    built = build_regulations(df, PostApiRegulationsAddBodyStatus.DRAFT, build_measure, **opt_ins)
    return sorted(built, key=lambda r: str(r.identifier))


def test_without_opt_in_every_row_is_its_own_measure():
    """Every source in production goes through this path: it must stay unchanged."""
    [regulation] = regulations(rows(("A", "30", 1), ("A", "30", 2), ("A", "30", 3)))
    assert regulation.identifier == "A"
    assert [len(m.locations) for m in regulation.measures] == [1, 1, 1]  # type: ignore[union-attr]


def test_rows_sharing_a_group_key_become_one_measure_with_all_their_locations():
    """A SIG publishes one row per segment; R-28 wants one measure carrying N locations."""
    [regulation] = regulations(
        rows(("A", "30", 1), ("A", "30", 2), ("A", "noEntry", 3), ("A", "30", 4)),
        group_locations_by_measure=True,
    )
    measures = regulation.measures
    assert [len(m.locations) for m in measures] == [3, 1]  # type: ignore[union-attr]
    assert [loc.raw_geo_json.label for loc in measures[0].locations] == [  # type: ignore
        "segment 1",
        "segment 2",
        "segment 4",
    ]


def test_a_regulation_above_the_ceiling_is_cut_into_numbered_slices_along_the_split_order():
    """Past ~1 700 locations a POST times out; the ceiling is per regulation, not per measure."""
    df = rows(("A", "30", 5), ("A", "30", 1), ("A", "30", 4), ("A", "30", 2), ("A", "30", 3))
    built = regulations(df, max_locations_per_regulation=2)
    assert [r.identifier for r in built] == ["A-01", "A-02", "A-03"]
    labels = [[loc.raw_geo_json.label for m in r.measures for loc in m.locations] for r in built]  # type: ignore
    assert labels == [["segment 1", "segment 2"], ["segment 3", "segment 4"], ["segment 5"]]
    assert all(r.title == "Arrêté A" for r in built)


def test_a_regulation_within_the_ceiling_keeps_its_identifier():
    [regulation] = regulations(rows(("A", "30", 1), ("A", "30", 2)), max_locations_per_regulation=2)
    assert regulation.identifier == "A"
