"""Tests for Sarthe restriction gabarits preprocessing."""

import polars as pl
import pytest

from integrations.dp_sarthe.integration import Integration
from integrations.dp_sarthe.restrictions_gabarits.data_source_integration import (
    DataSourceIntegration,
    compute_location_fields,
    compute_measure_fields,
    compute_period_fields,
    compute_vehicle_fields,
)
from integrations.dp_sarthe.restrictions_gabarits.schema import (
    SartheRestrictionGabaritsRawDataSchema,
)


@pytest.fixture
def raw_data():
    """Load test data from restrictions_gabarits.csv."""
    return pl.read_csv("tests/dp_sarthe/restrictions_gabarits.csv", separator=",")


@pytest.fixture
def integration():
    """Create Sarthe integration instance."""
    return Integration.from_organization("dp_sarthe")


@pytest.fixture
def data_source(integration):
    """Create Sarthe restrictions_gabarits data source instance."""
    return DataSourceIntegration(integration.organization_settings, integration.client)


def test_validate_raw_data(data_source, raw_data):
    """Test that validation succeeds and produces expected columns."""
    validated = data_source.validate_raw_data(raw_data)

    expected_columns = set(SartheRestrictionGabaritsRawDataSchema.to_schema().columns.keys())
    assert set(validated.columns) == expected_columns
    assert validated.height > 0


def test_preprocess_is_identity(data_source, raw_data):
    """Test that Sarthe has no preprocessing (identity function)."""
    schema_columns = list(SartheRestrictionGabaritsRawDataSchema.to_schema().columns.keys())
    df = raw_data.select(schema_columns)

    preprocessed = data_source.preprocess_raw_data(df)

    assert preprocessed.columns == df.columns
    assert preprocessed.height == df.height


def test_compute_measure_fields():
    """Test that compute_measure_fields sets measure_type to NOENTRY
    and measure_max_speed to null."""
    from api.dia_log_client.models import MeasureTypeEnum

    df = pl.DataFrame(
        {
            "objectid": [1, 2, 3],
            "nature": ["Test 1", "Test 2", "Test 3"],
        }
    )

    result = compute_measure_fields(df)

    # Check both fields are created
    assert "measure_type_" in result.columns
    assert "measure_max_speed" in result.columns
    assert result.height == 3

    # Check all measures are NOENTRY
    assert all(mt == MeasureTypeEnum.NOENTRY.value for mt in result["measure_type_"].to_list())

    # Check all max_speed are null
    assert all(speed is None for speed in result["measure_max_speed"].to_list())


def test_compute_period_fields():
    """Test that compute_period_fields creates all required fields from date_creation."""
    df = pl.DataFrame(
        {
            "date_creation": [
                "2024-07-11T19:27:30+01:00",
                "2024-11-29T12:38:18+00:00",
                "2025-01-23T10:51:06+00:00",
            ],
        }
    )

    result = compute_period_fields(df)

    # Check all period fields exist
    assert "period_start_date" in result.columns
    assert "period_end_date" in result.columns
    assert "period_start_time" in result.columns
    assert "period_end_time" in result.columns
    assert "period_recurrence_type" in result.columns
    assert "period_is_permanent" in result.columns

    # Check values
    assert result["period_start_date"][0] == "2024-07-11T19:27:30+01:00"
    assert result["period_start_date"][1] == "2024-11-29T12:38:18+00:00"
    assert result["period_start_date"][2] == "2025-01-23T10:51:06+00:00"
    assert result["period_recurrence_type"][0] == "everyDay"
    assert result["period_is_permanent"][0] is True


def test_compute_period_fields_filters_null_date_creation():
    """Test that rows with null date_creation are filtered out."""
    df = pl.DataFrame(
        {
            "date_creation": ["2024-07-11T19:27:30+01:00", None, "2025-01-23T10:51:06+00:00"],
        }
    )

    result = compute_period_fields(df)

    # Should keep only valid dates
    assert result.height == 2
    assert result["period_start_date"].to_list() == [
        "2024-07-11T19:27:30+01:00",
        "2025-01-23T10:51:06+00:00",
    ]


def test_compute_location_fields():
    """Test that compute_location_fields creates all required fields."""
    from api.dia_log_client.models import RoadTypeEnum

    df = pl.DataFrame(
        {
            "localisation_curviligne": [
                "72_D0010 : Du 1+521 au 4+977 côté : Non latéralisé",
                "72_D0015 : Du 0+0 au 1+635 côté : Non latéralisé",
            ],
            "geo_shape": [
                '{"type": "LineString", "coordinates": [[0.0, 0.0], [1.0, 1.0]]}',
                '{"type": "Point", "coordinates": [0.0, 0.0]}',
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

    # Check label is from localisation_curviligne
    assert result["location_label"][0] == "72_D0010 : Du 1+521 au 4+977 côté : Non latéralisé"
    assert result["location_label"][1] == "72_D0015 : Du 0+0 au 1+635 côté : Non latéralisé"

    # Check geometry is passed through from geo_shape
    assert (
        result["location_geometry"][0]
        == '{"type": "LineString", "coordinates": [[0.0, 0.0], [1.0, 1.0]]}'
    )


def test_compute_location_fields_filters_null_geometry():
    """Test that rows with null geometry are filtered out."""
    df = pl.DataFrame(
        {
            "localisation_curviligne": ["Route 1", "Route 2", "Route 3"],
            "geo_shape": ['{"type": "Point"}', None, '{"type": "LineString"}'],
        }
    )

    result = compute_location_fields(df)

    assert result.height == 2
    assert result["location_label"].to_list() == ["Route 1", "Route 3"]


def test_compute_vehicle_fields_with_tonnage():
    """Test that compute_vehicle_fields handles tonnage correctly (converts to kg)."""
    df = pl.DataFrame(
        {
            "tonnage": [7.5, 3.5, 10.0],
            "hauteur": [0.0, 0.0, 0.0],
            "largeur": [0.0, 0.0, 0.0],
        }
    )

    result = compute_vehicle_fields(df)

    # Check vehicle fields
    assert "vehicle_all_vehicles" in result.columns
    assert "vehicle_heavyweight_max_weight" in result.columns
    assert "vehicle_max_height" in result.columns
    assert "vehicle_max_width" in result.columns

    # Check tonnage is converted from tons to kg (multiply by 1000)
    assert result["vehicle_heavyweight_max_weight"][0] == 7500.0  # 7.5 tons = 7500 kg
    assert result["vehicle_heavyweight_max_weight"][1] == 3500.0  # 3.5 tons = 3500 kg
    assert result["vehicle_heavyweight_max_weight"][2] == 10000.0  # 10.0 tons = 10000 kg

    # Check all_vehicles is False
    assert all(not v for v in result["vehicle_all_vehicles"].to_list())


def test_compute_vehicle_fields_with_height():
    """Test that compute_vehicle_fields handles hauteur correctly."""
    df = pl.DataFrame(
        {
            "tonnage": [5.0, 3.5],  # Must be non-zero to pass filter
            "hauteur": [3.9, 3.7],
            "largeur": [0.0, 0.0],
        }
    )

    result = compute_vehicle_fields(df)

    # Check height values
    assert result["vehicle_max_height"][0] == 3.9
    assert result["vehicle_max_height"][1] == 3.7

    # Check tonnage is converted to kg
    assert result["vehicle_heavyweight_max_weight"][0] == 5000.0
    assert result["vehicle_heavyweight_max_weight"][1] == 3500.0

    # Check width is None
    assert result["vehicle_max_width"][0] is None


def test_compute_vehicle_fields_with_width():
    """Test that compute_vehicle_fields handles largeur correctly."""
    df = pl.DataFrame(
        {
            "tonnage": [5.0, 3.5],  # Must be non-zero to pass filter
            "hauteur": [0.0, 0.0],
            "largeur": [2.5, 2.7],
        }
    )

    result = compute_vehicle_fields(df)

    # Check width values
    assert result["vehicle_max_width"][0] == 2.5
    assert result["vehicle_max_width"][1] == 2.7

    # Check tonnage is converted to kg
    assert result["vehicle_heavyweight_max_weight"][0] == 5000.0
    assert result["vehicle_heavyweight_max_weight"][1] == 3500.0

    # Check height is None
    assert result["vehicle_max_height"][0] is None


def test_compute_vehicle_fields_filters_tonnage_zero():
    """Test that rows with tonnage == 0 are filtered out."""
    df = pl.DataFrame(
        {
            "tonnage": [7.5, 0.0, 3.5, 0.0],
            "hauteur": [0.0, 3.9, 0.0, 0.0],
            "largeur": [0.0, 0.0, 2.5, 0.0],
        }
    )

    result = compute_vehicle_fields(df)

    # Should keep only rows where tonnage != 0
    assert result.height == 2
    # First and third rows have non-zero tonnage
    assert result["vehicle_heavyweight_max_weight"].to_list() == [7500.0, 3500.0]


def test_compute_regulation_fields(data_source):
    """Test that compute_regulation_fields creates all required fields."""
    df = pl.DataFrame(
        {
            "objectid": [1, 280, 8],
            "nature": ["Interdiction 7,5t", "Interdiction 7,5t", "Interdiction 3.5t en Transit"],
            "site": ["La Flèche", "Beaumont", "Connerré"],
        }
    )

    result = data_source.compute_regulation_fields(df)

    # Check all regulation fields exist
    assert "regulation_identifier" in result.columns
    assert "regulation_category" in result.columns
    assert "regulation_subject" in result.columns
    assert "regulation_title" in result.columns
    assert "regulation_other_category_text" in result.columns

    # Check values
    assert result.height == 3
    assert result["regulation_identifier"].to_list() == ["1", "280", "8"]
    assert result["regulation_title"][0] == "1 - Interdiction 7,5t - La Flèche"
    assert result["regulation_title"][1] == "280 - Interdiction 7,5t - Beaumont"
    assert result["regulation_title"][2] == "8 - Interdiction 3.5t en Transit - Connerré"
    assert result["regulation_other_category_text"][0] == "Circulation"


def test_compute_regulation_fields_drops_duplicates(data_source):
    """Test that compute_regulation_fields drops duplicate objectids."""
    df = pl.DataFrame(
        {
            "objectid": [1, 1, 2],  # First objectid is duplicated
            "nature": ["Nature A", "Nature A duplicate", "Nature B"],
            "site": ["Site A", "Site A duplicate", "Site B"],
        }
    )

    result = data_source.compute_regulation_fields(df)

    # Should drop the duplicates (both rows with objectid=1)
    assert result.height == 1
    assert result["regulation_identifier"][0] == "2"
    assert result["regulation_title"][0] == "2 - Nature B - Site B"
