"""Tests for Brest preprocessing."""

from datetime import datetime

import polars as pl
import pytest

from integrations.co_brest.integration import Integration
from integrations.co_brest.permanent_lineaire.data_source_integration import (
    DataSourceIntegration,
    ccompute_period_fields,
)
from integrations.co_brest.permanent_lineaire.schema import Schema


@pytest.fixture
def raw_data():
    """Load test data from permanent_lineaire.csv."""
    return pl.read_csv("tests/co_brest/permanent_lineaire.csv")


@pytest.fixture
def integration():
    """Create Brest integration instance."""
    return Integration.from_organization("co_brest")


@pytest.fixture
def data_source(integration):
    """Create Brest permanent_lineaire data source instance."""
    return DataSourceIntegration(integration.organization_settings, integration.client)


def test_validate_raw_data(data_source, raw_data):
    """Test that validation succeeds and produces expected columns."""
    validated = data_source.validate_raw_data(raw_data)

    expected_columns = set(Schema.to_schema().columns.keys())
    assert set(validated.columns) == expected_columns
    assert validated.height > 0


def test_preprocess_casts_booleans(data_source, raw_data):
    """Test that preprocessing casts VELO and CYCLO to boolean and filters empty NOARR."""
    schema_columns = list(Schema.to_schema().columns.keys())
    df = raw_data.select(schema_columns)

    preprocessed = data_source.preprocess_raw_data(df)

    assert preprocessed["VELO"].dtype == pl.Boolean
    assert preprocessed["CYCLO"].dtype == pl.Boolean
    assert all(v in [True, False] for v in preprocessed["VELO"].to_list())
    # Check that empty NOARR rows are filtered out
    assert all(noarr != "" for noarr in preprocessed["NOARR"].to_list())


def test_cast_boolean_column_oui_to_true(data_source):
    """Test that 'OUI' is cast to True."""
    df = pl.DataFrame({"test_col": ["OUI", "oui", "Oui"], "NOARR": ["A", "B", "C"]})

    result = df.with_columns(data_source.cast_boolean_column("test_col"))

    assert result["test_col"].dtype == pl.Boolean
    assert result["test_col"].to_list() == [True, True, True]


def test_cast_boolean_column_non_to_false(data_source):
    """Test that 'NON' is cast to False."""
    df = pl.DataFrame({"test_col": ["NON", "non", "Non"], "NOARR": ["A", "B", "C"]})

    result = df.with_columns(data_source.cast_boolean_column("test_col"))

    assert result["test_col"].dtype == pl.Boolean
    assert result["test_col"].to_list() == [False, False, False]


def test_cast_boolean_column_null_to_false(data_source):
    """Test that null values are filled with False."""
    df = pl.DataFrame({"test_col": ["OUI", None, "NON"], "NOARR": ["A", "B", "C"]})

    result = df.with_columns(data_source.cast_boolean_column("test_col"))

    assert result["test_col"].dtype == pl.Boolean
    assert result["test_col"].to_list() == [True, False, False]


def test_ccompute_period_fields():
    """Test that ccompute_period_fields creates all required fields."""

    df = pl.DataFrame(
        {
            "DT_MAT": [datetime(2023, 6, 15, 10, 30, 45), datetime(2024, 1, 1)],
            "NOARR": ["A", "B"],
        }
    )

    result = ccompute_period_fields(df)

    # Check all period fields exist
    assert "period_start_date" in result.columns
    assert "period_end_date" in result.columns
    assert "period_start_time" in result.columns
    assert "period_end_time" in result.columns
    assert "period_recurrence_type" in result.columns
    assert "period_is_permanent" in result.columns

    # Check values
    assert result["period_start_date"][0] == "2023-06-15T10:30:45Z"
    assert result["period_start_date"][1] == "2024-01-01T00:00:00Z"
    assert result["period_recurrence_type"][0] == "everyDay"
    assert result["period_is_permanent"][0] is True


def test_ccompute_period_fields_filters_null_dt_mat():
    """Test that rows with null DT_MAT are filtered out."""
    from datetime import datetime

    from integrations.co_brest.permanent_lineaire.data_source_integration import (
        ccompute_period_fields,
    )

    df = pl.DataFrame(
        {
            "DT_MAT": [datetime(2023, 6, 15), None, datetime(2024, 1, 1)],
            "NOARR": ["A", "B", "C"],
        }
    )

    result = ccompute_period_fields(df)

    assert result.height == 2
    assert result["NOARR"].to_list() == ["A", "C"]


def test_compute_location_fields():
    """Test that compute_location_fields creates all required fields."""
    from api.dia_log_client.models import RoadTypeEnum
    from integrations.co_brest.permanent_lineaire.data_source_integration import (
        compute_location_fields,
    )

    df = pl.DataFrame(
        {
            "LIBCO": ["Commune A", "Commune B"],
            "LIBRU": ["Rue 1", "Rue 2"],
            "geometry": [
                "POINT (150000 6850000)",  # Valid EPSG:2154 (Lambert 93) coordinates for Brest area
                "LINESTRING (150000 6850000, 150100 6850100)",
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

    # Check label is constructed from LIBCO and LIBRU
    assert result["location_label"][0] == "Commune A – Rue 1"
    assert result["location_label"][1] == "Commune B – Rue 2"

    # Check geometry is transformed to GeoJSON (should contain "type" and "coordinates")
    import json

    geom0 = json.loads(result["location_geometry"][0])
    assert "type" in geom0
    assert "coordinates" in geom0


def test_compute_location_fields_filters_null_geometry():
    """Test that rows with null geometry are filtered out."""
    from integrations.co_brest.permanent_lineaire.data_source_integration import (
        compute_location_fields,
    )

    df = pl.DataFrame(
        {
            "LIBCO": ["Commune A", "Commune B", "Commune C"],
            "LIBRU": ["Rue 1", "Rue 2", "Rue 3"],
            "geometry": [
                "POINT (150000 6850000)",  # Valid EPSG:2154 coordinates
                None,
                "LINESTRING (150000 6850000, 150100 6850100)",
            ],
        }
    )

    result = compute_location_fields(df)

    assert result.height == 2
    assert result["location_label"].to_list() == ["Commune A – Rue 1", "Commune C – Rue 3"]


def test_compute_regulation_fields(data_source):
    """Test that compute_regulation_fields creates all required fields and groups by NOARR."""
    df = pl.DataFrame(
        {
            "NOARR": ["REG001", "REG001", "REG002"],
            "DESCRIPTIF": ["Limitation Vitesse", "Limitation Vitesse", "Stationnement interdit"],
            "LIBRU": ["Rue A", "Rue B", "Rue C"],
            "LIEN_URL": [None, None, None],
        }
    )

    result = data_source.compute_regulation_fields(df)

    # Check all regulation fields exist
    assert "regulation_identifier" in result.columns
    assert "regulation_category" in result.columns
    assert "regulation_subject" in result.columns
    assert "regulation_title" in result.columns
    assert "regulation_other_category_text" in result.columns
    assert "regulation_document_url" in result.columns

    # Check values - all rows with same NOARR should have same regulation_title (from first row)
    assert result["regulation_identifier"].to_list() == ["REG001", "REG001", "REG002"]
    assert result["regulation_title"][0] == "Limitation Vitesse – Rue A"
    assert result["regulation_title"][1] == "Limitation Vitesse – Rue A"  # Same as first row
    assert result["regulation_title"][2] == "Stationnement interdit – Rue C"
    assert result["regulation_other_category_text"][0] == "Circulation"
    # Without URL, should be None
    assert result["regulation_document_url"][0] is None


def test_compute_regulation_fields_with_url(data_source):
    """Test that compute_regulation_fields includes URL
    in regulation_document_url when available."""
    df = pl.DataFrame(
        {
            "NOARR": ["REG001", "REG001", "REG002"],
            "DESCRIPTIF": ["Limitation Vitesse", "Limitation Vitesse", "Stationnement interdit"],
            "LIBRU": ["Rue A", "Rue B", "Rue C"],
            "LIEN_URL": [
                "https://example.com/arrete1.pdf",
                "https://example.com/arrete1.pdf",
                None,
            ],
        }
    )

    result = data_source.compute_regulation_fields(df)

    # Check that URL is in regulation_document_url
    assert result["regulation_document_url"][0] == "https://example.com/arrete1.pdf"
    assert result["regulation_document_url"][1] == "https://example.com/arrete1.pdf"
    # Without URL, should be None
    assert result["regulation_document_url"][2] is None
    # other_category_text should always be "Circulation"
    assert result["regulation_other_category_text"][0] == "Circulation"
    assert result["regulation_other_category_text"][2] == "Circulation"


def test_compute_measure_fields():
    """Test that compute_measure_fields computes both measure_type_ and measure_max_speed."""
    from api.dia_log_client.models import MeasureTypeEnum
    from integrations.co_brest.permanent_lineaire.data_source_integration import (
        compute_measure_fields,
    )

    df = pl.DataFrame(
        {
            "DESCRIPTIF": ["Limitation Vitesse", "Stationnement interdit", "Limitation Poids"],
            "SENS": [1, 1, 1],
            "VITEMAX": [50, 0, 0],
        }
    )

    result = compute_measure_fields(df)

    assert "measure_type_" in result.columns
    assert "measure_max_speed" in result.columns
    assert result.height == 3
    assert result["measure_type_"][0] == MeasureTypeEnum.SPEEDLIMITATION.value
    assert result["measure_type_"][1] == MeasureTypeEnum.PARKINGPROHIBITED.value
    assert result["measure_type_"][2] == MeasureTypeEnum.NOENTRY.value
    # Check measure_max_speed: only set for SPEEDLIMITATION
    assert result["measure_max_speed"][0] == 50
    assert result["measure_max_speed"][1] is None
    assert result["measure_max_speed"][2] is None


def test_compute_measure_fields_filters_invalid_descriptif():
    """Test that compute_measure_fields filters out rows with invalid DESCRIPTIF."""
    from integrations.co_brest.permanent_lineaire.data_source_integration import (
        compute_measure_fields,
    )

    df = pl.DataFrame(
        {
            "DESCRIPTIF": ["Limitation Vitesse", "Invalid Description", "Stationnement interdit"],
            "SENS": [1, 1, 1],
            "VITEMAX": [50, 30, 0],
        }
    )

    result = compute_measure_fields(df)

    # Should filter out the invalid description
    assert result.height == 2
    assert result["DESCRIPTIF"].to_list() == ["Limitation Vitesse", "Stationnement interdit"]


def test_compute_measure_fields_filters_sens_unique():
    """Test that compute_measure_fields filters Sens interdit/Sens unique with SENS=1."""
    from integrations.co_brest.permanent_lineaire.data_source_integration import (
        compute_measure_fields,
    )

    df = pl.DataFrame(
        {
            "DESCRIPTIF": [
                "Sens interdit / Sens unique",
                "Sens interdit / Sens unique",
                "Limitation Vitesse",
            ],
            "SENS": [1, 2, 1],  # First should be filtered, second kept
            "VITEMAX": [0, 0, 50],
        }
    )

    result = compute_measure_fields(df)

    # Should filter out "Sens interdit / Sens unique" with SENS=1
    assert result.height == 2
    assert result["DESCRIPTIF"].to_list() == ["Sens interdit / Sens unique", "Limitation Vitesse"]
    assert result["SENS"].to_list() == [2, 1]


def test_compute_measure_fields_filters_invalid_speed():
    """Test that compute_measure_fields filters out SPEEDLIMITATION with invalid VITEMAX."""
    from integrations.co_brest.permanent_lineaire.data_source_integration import (
        compute_measure_fields,
    )

    df = pl.DataFrame(
        {
            "DESCRIPTIF": ["Limitation Vitesse", "Limitation Vitesse", "Limitation Vitesse"],
            "SENS": [1, 1, 1],
            "VITEMAX": [50, 0, None],  # Second and third are invalid
        }
    )

    result = compute_measure_fields(df)

    # Should filter out invalid speed limitations
    assert result.height == 1
    assert result["measure_max_speed"][0] == 50
