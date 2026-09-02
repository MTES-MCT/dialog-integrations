"""Integration tests for all organizations - tests full pipeline with all data sources."""

import polars as pl
import pytest

from integrations.base_integration import BaseIntegration


@pytest.mark.parametrize(
    "organization",
    [
        "co_brest",
        "co_lyon",
    ],
)
def test_full_pipeline_integration(organization, monkeypatch):
    """Test the full pipeline with actual CSV data for all organizations, mocking only API calls."""
    # Create integration instance
    integration = BaseIntegration.from_organization(organization)

    # Get data sources and mock their fetch_raw_data to load actual CSV data
    for data_source_integration in integration.data_sources:

        def mock_fetch_raw_data(_, name=data_source_integration.name):
            return pl.read_csv(f"tests/{organization}/{name}.csv", separator=",")

        monkeypatch.setattr(data_source_integration, "fetch_raw_data", mock_fetch_raw_data)

    # Mock API-related methods
    monkeypatch.setattr(integration, "_integrate_regulations_add", lambda regs: None)
    monkeypatch.setattr(integration, "fetch_regulation_ids", lambda: [])

    # Run the full pipeline
    integration.integrate_regulations()


def period(**overrides):
    measure = {
        "period_start_date": "2026-08-25T00:00:00+02:00",
        "period_end_date": "2026-10-09T23:59:59+02:00",
        "period_recurrence_type": "everyDay",
        "period_is_permanent": False,
        "period_time_slots": None,
    }
    measure.update(overrides)
    integration = BaseIntegration.from_organization("co_brest")
    return integration.create_save_period_dto(measure)  # type: ignore[arg-type]


def test_period_without_time_slots_applies_around_the_clock():
    """Every existing integration goes through this path: it must stay unchanged."""
    dto = period()
    assert dto.time_slots == []
    assert dto.start_time == "2026-08-25T00:00:00+02:00"
    assert dto.end_time == "2026-10-09T23:59:59+02:00"


def test_period_time_slots_become_api_objects():
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
    assert dto.to_dict()["timeSlots"][0]["startTime"] == "2026-08-25T08:00:00+02:00"
