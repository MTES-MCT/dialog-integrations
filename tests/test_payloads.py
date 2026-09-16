"""From pivot rows to API DTOs: the period, with and without daily time slots."""

from integrations.payloads import build_period


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
