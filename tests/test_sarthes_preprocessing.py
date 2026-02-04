"""Tests for Sarthes preprocessing."""

import polars as pl
import pytest

from integrations.dp_sarthes.integration import Integration, compute_start_date
from integrations.dp_sarthes.schema import SarthesRawDataSchema


@pytest.fixture
def raw_data():
    """Load test data from data.csv."""
    # CSV has index column, read and drop it
    df = pl.read_csv("tests/data/dp_sarthes/data.csv")
    # Drop the index column (first column which has no name or is numeric)
    if df.columns[0] in ["", "column_1"] or df.columns[0].isdigit():
        df = df.drop(df.columns[0])
    return df


@pytest.fixture
def integration():
    """Create Sarthes integration instance."""
    return Integration.from_organization("dp_sarthes")


def test_validate_raw_data(integration, raw_data):
    """Test that validation succeeds and produces expected columns."""
    validated = integration.validate_raw_data(raw_data)

    expected_columns = set(SarthesRawDataSchema.to_schema().columns.keys())
    assert set(validated.columns) == expected_columns
    assert validated.height > 0


def test_preprocess_is_identity(integration, raw_data):
    """Test that Sarthes has no preprocessing (identity function)."""
    schema_columns = list(SarthesRawDataSchema.to_schema().columns.keys())
    df = raw_data.select(schema_columns)

    preprocessed = integration.preprocess_raw_data(df)

    assert preprocessed.columns == df.columns
    assert preprocessed.height == df.height


def test_compute_start_date_uses_annee():
    """Test that compute_start_date uses annee when present."""
    df = pl.DataFrame(
        {
            "annee": [2023.0, 2024.0],
            "date_modif": ["2023-05-15T10:00:00Z", "2024-08-20T14:30:00Z"],
        }
    )

    result = compute_start_date(df)

    assert result["period_start_date"][0] == "2023-01-01T00:00:00Z"
    assert result["period_start_date"][1] == "2024-01-01T00:00:00Z"


def test_compute_start_date_falls_back_to_date_modif():
    """Test that compute_start_date uses date_modif when annee is null."""
    from integrations.dp_sarthes.integration import compute_start_date

    df = pl.DataFrame(
        {
            "annee": [None, 2024.0],
            "date_modif": ["2023-05-15T10:00:00Z", "2024-08-20T14:30:00Z"],
        }
    )

    result = compute_start_date(df)

    assert result["period_start_date"][0] == "2023-05-15T10:00:00Z"
    assert result["period_start_date"][1] == "2024-01-01T00:00:00Z"


def test_compute_start_date_creates_all_period_fields():
    """Test that compute_start_date creates all required period fields."""
    df = pl.DataFrame(
        {
            "annee": [2023.0],
            "date_modif": ["2023-05-15T10:00:00Z"],
        }
    )

    result = compute_start_date(df)

    # Check all period fields exist
    assert "period_start_date" in result.columns
    assert "period_end_date" in result.columns
    assert "period_start_time" in result.columns
    assert "period_end_time" in result.columns
    assert "period_recurrence_type" in result.columns
    assert "period_is_permanent" in result.columns

    # Check values
    assert result["period_recurrence_type"][0] == "everyDay"
    assert result["period_is_permanent"][0] is True
    assert result["period_end_date"][0] is None


def test_compute_save_location_fields():
    """Test that compute_save_location_fields creates all required fields."""
    from api.dia_log_client.models import RoadTypeEnum
    from integrations.dp_sarthes.integration import compute_save_location_fields

    df = pl.DataFrame(
        {
            "loc_txt": ["Route de Paris", None, ""],
            "title": ["Title 1", "Title 2", "Title 3"],
            "geo_shape": [
                '{"type": "Point", "coordinates": [0, 0]}',
                '{"type": "LineString"}',
                '{"type": "Polygon"}',
            ],
        }
    )

    result = compute_save_location_fields(df)

    # Check all location fields exist
    assert "location_road_type" in result.columns
    assert "location_label" in result.columns
    assert "location_geometry" in result.columns

    # Check road_type is always RAWGEOJSON enum value
    assert result["location_road_type"][0] == RoadTypeEnum.RAWGEOJSON.value
    assert result["location_road_type"][1] == RoadTypeEnum.RAWGEOJSON.value
    assert result["location_road_type"][2] == RoadTypeEnum.RAWGEOJSON.value

    # Check label uses loc_txt when present, otherwise title
    assert result["location_label"][0] == "Route de Paris"
    assert result["location_label"][1] == "Title 2"
    assert result["location_label"][2] == "Title 3"

    # Check geometry is passed through from geo_shape
    assert result["location_geometry"][0] == '{"type": "Point", "coordinates": [0, 0]}'


def test_compute_save_location_fields_filters_null_geometry():
    """Test that rows with null geometry are filtered out."""
    from integrations.dp_sarthes.integration import compute_save_location_fields

    df = pl.DataFrame(
        {
            "loc_txt": ["Route 1", "Route 2", "Route 3"],
            "title": ["Title 1", "Title 2", "Title 3"],
            "geo_shape": ['{"type": "Point"}', None, '{"type": "LineString"}'],
        }
    )

    result = compute_save_location_fields(df)

    assert result.height == 2
    assert result["location_label"].to_list() == ["Route 1", "Route 3"]


def test_compute_regulation_fields(integration):
    """Test that compute_regulation_fields creates all required fields."""
    df = pl.DataFrame(
        {
            "id": ["reg-1", "reg-2"],
            "title": ["Speed limit 50", "Speed limit 30"],
        }
    )

    result = integration.compute_regulation_fields(df)

    # Check all regulation fields exist
    assert "regulation_identifier" in result.columns
    assert "regulation_status" in result.columns
    assert "regulation_category" in result.columns
    assert "regulation_subject" in result.columns
    assert "regulation_title" in result.columns
    assert "regulation_other_category_text" in result.columns

    # Check values
    assert result["regulation_identifier"].to_list() == ["reg-1", "reg-2"]
    assert result["regulation_title"].to_list() == ["Speed limit 50", "Speed limit 30"]
    assert result["regulation_other_category_text"][0] == "Limitation de vitesse"


def test_full_pipeline_integration(integration, monkeypatch):
    """Test the full pipeline with actual CSV data, mocking only API calls."""

    # Mock fetch_raw_data to load actual CSV data
    def mock_fetch_raw_data():
        df = pl.read_csv("tests/data/dp_sarthes/data.csv")
        # Drop index column if present
        if df.columns[0] in ["", "column_1"] or df.columns[0].isdigit():
            df = df.drop(df.columns[0])
        return df

    monkeypatch.setattr(integration, "fetch_raw_data", mock_fetch_raw_data)

    # Mock API-related methods
    monkeypatch.setattr(integration, "_integrate_regulations", lambda regs: None)
    monkeypatch.setattr(integration, "fetch_regulation_ids", lambda: [])

    # Run the full pipeline
    integration.integrate_regulations()
