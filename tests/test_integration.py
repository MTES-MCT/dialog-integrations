"""Integration tests for all organizations - tests full pipeline with all data sources."""

import polars as pl
import pytest

from integrations.base_integration import BaseIntegration


@pytest.mark.parametrize(
    "organization",
    [
        "co_brest",
        "dp_sarthe",
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
    monkeypatch.setattr(integration, "_integrate_regulations", lambda regs: None)
    monkeypatch.setattr(integration, "fetch_regulation_ids", lambda: [])

    # Run the full pipeline
    integration.integrate_regulations()
