"""Tests for Sarthe preprocessing."""

import polars as pl
import pytest

from integrations.dp_sarthe.integration import Integration
from integrations.dp_sarthe.limitations_vitesse.data_source_integration import (
    DataSourceIntegration,
)
from integrations.dp_sarthe.limitations_vitesse.schema import SartheRawDataSchema


@pytest.fixture
def raw_data():
    """Load test data from limitations_vitesse.csv."""
    # CSV has index column, read and drop it
    df = pl.read_csv("tests/dp_sarthe/limitations_vitesse.csv")
    # Drop the index column (first column which has no name or is numeric)
    if df.columns[0] in ["", "column_1"] or df.columns[0].isdigit():
        df = df.drop(df.columns[0])
    return df


@pytest.fixture
def integration():
    """Create Sarthe integration instance."""
    return Integration.from_organization("dp_sarthe")


@pytest.fixture
def data_source(integration):
    """Create Sarthe limitations_vitesse data source instance."""
    return DataSourceIntegration(integration.organization_settings, integration.client)


def test_validate_raw_data(data_source, raw_data):
    """Test that validation succeeds and produces expected columns."""
    validated = data_source.validate_raw_data(raw_data)

    expected_columns = set(SartheRawDataSchema.to_schema().columns.keys())
    assert set(validated.columns) == expected_columns
    assert validated.height > 0


def test_preprocess_is_identity(data_source, raw_data):
    """Test that Sarthe has no preprocessing (identity function)."""
    schema_columns = list(SartheRawDataSchema.to_schema().columns.keys())
    df = raw_data.select(schema_columns)

    preprocessed = data_source.preprocess_raw_data(df)

    assert preprocessed.columns == df.columns
    assert preprocessed.height == df.height


def test_compute_start_date_uses_annee():
    """Test that compute_start_date uses annee when present."""
    from integrations.dp_sarthe.limitations_vitesse.data_source_integration import (
        compute_start_date,
    )

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
    from integrations.dp_sarthe.limitations_vitesse.data_source_integration import (
        compute_start_date,
    )

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
    from integrations.dp_sarthe.limitations_vitesse.data_source_integration import (
        compute_start_date,
    )

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


def test_compute_location_fields():
    """Test that compute_location_fields creates all required fields."""
    from api.dia_log_client.models import RoadTypeEnum
    from integrations.dp_sarthe.limitations_vitesse.data_source_integration import (
        compute_location_fields,
    )

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

    result = compute_location_fields(df)

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


def test_compute_location_fields_filters_null_geometry():
    """Test that rows with null geometry are filtered out."""
    from integrations.dp_sarthe.limitations_vitesse.data_source_integration import (
        compute_location_fields,
    )

    df = pl.DataFrame(
        {
            "loc_txt": ["Route 1", "Route 2", "Route 3"],
            "title": ["Title 1", "Title 2", "Title 3"],
            "geo_shape": ['{"type": "Point"}', None, '{"type": "LineString"}'],
        }
    )

    result = compute_location_fields(df)

    assert result.height == 2
    assert result["location_label"].to_list() == ["Route 1", "Route 3"]


def test_compute_regulation_fields(data_source):
    """Test that compute_regulation_fields creates all required fields and builds id."""
    df = pl.DataFrame(
        {
            "loc_txt": ["Route A", "Route B"],
            "measure_max_speed": [50, 30],
            "longueur": [100, 200],
            "title": ["Speed limit 50", "Speed limit 30"],
        }
    )

    result = data_source.compute_regulation_fields(df)

    # Check all regulation fields exist
    assert "regulation_identifier" in result.columns
    assert "regulation_category" in result.columns
    assert "regulation_subject" in result.columns
    assert "regulation_title" in result.columns
    assert "regulation_other_category_text" in result.columns
    assert "id" in result.columns

    # Check values
    assert result.height == 2
    assert result["regulation_title"].to_list() == ["Speed limit 50", "Speed limit 30"]
    assert result["regulation_other_category_text"][0] == "Limitation de vitesse"
    # Check that id was created from hash
    assert all(len(id_val) == 32 for id_val in result["id"].to_list())  # MD5 hash is 32 chars


def test_compute_regulation_fields_drops_duplicates(data_source):
    """Test that compute_regulation_fields drops duplicate ids."""
    df = pl.DataFrame(
        {
            "loc_txt": ["Route A", "Route A", "Route B"],
            "measure_max_speed": [50, 50, 30],
            "longueur": [100, 100, 200],  # First two will have same hash
            "title": ["Speed limit 50", "Speed limit 50 duplicate", "Speed limit 30"],
        }
    )

    result = data_source.compute_regulation_fields(df)

    # Should drop the duplicates (both rows with same hash)
    assert result.height == 1
    assert result["regulation_title"][0] == "Speed limit 30"


def test_compute_measure_fields():
    """Test that compute_measure_fields computes both measure_max_speed and measure_type_."""
    from api.dia_log_client.models import MeasureTypeEnum
    from integrations.dp_sarthe.limitations_vitesse.data_source_integration import (
        compute_measure_fields,
    )

    df = pl.DataFrame(
        {
            "VITESSE": [50, 30, 90],
        }
    )

    result = compute_measure_fields(df)

    # Check both fields are created
    assert "measure_max_speed" in result.columns
    assert "measure_type_" in result.columns
    assert result.height == 3

    # Check measure_max_speed values
    assert result["measure_max_speed"].to_list() == [50, 30, 90]

    # Check all measures are SPEEDLIMITATION
    assert all(
        mt == MeasureTypeEnum.SPEEDLIMITATION.value for mt in result["measure_type_"].to_list()
    )


def test_compute_measure_fields_filters_invalid_vitesse():
    """Test that compute_measure_fields filters out rows with invalid VITESSE."""
    from integrations.dp_sarthe.limitations_vitesse.data_source_integration import (
        compute_measure_fields,
    )

    df = pl.DataFrame(
        {
            "VITESSE": [50, 0, None, -10, 150, 130],  # 0, None, -10, 150 are invalid; 130 is valid
        }
    )

    result = compute_measure_fields(df)

    # Should keep only valid values (50 and 130)
    assert result.height == 2
    assert result["measure_max_speed"].to_list() == [50, 130]


def test_compute_measure_fields_casts_vitesse_to_int():
    """Test that compute_measure_fields casts VITESSE to int."""
    from integrations.dp_sarthe.limitations_vitesse.data_source_integration import (
        compute_measure_fields,
    )

    df = pl.DataFrame(
        {
            "VITESSE": ["50", "30", "90"],  # String values
        }
    )

    result = compute_measure_fields(df)

    # Should cast to int
    assert result["measure_max_speed"].dtype == pl.Int64
    assert result["measure_max_speed"].to_list() == [50, 30, 90]
