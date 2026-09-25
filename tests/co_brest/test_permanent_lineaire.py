"""Tests for Brest preprocessing."""

from datetime import datetime

import polars as pl
import pytest

from integrations.co_brest.integration import Integration
from integrations.co_brest.permanent_lineaire.data_source_integration import (
    DataSourceIntegration,
    compute_period_fields,
    compute_regulation_fields,
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


def test_compute_period_fields():
    """Test that compute_period_fields creates all required fields."""

    df = pl.DataFrame(
        {
            "DT_MAT": [datetime(2023, 6, 15, 10, 30, 45), datetime(2024, 1, 1)],
            "NOARR": ["A", "B"],
            "CONDITION": [None, "interdit de 22H à 7H"],
            "DESCR": [None, None],
        }
    )

    result = compute_period_fields(df)

    assert "period_start_date" in result.columns
    assert "period_end_date" in result.columns
    # start_time / end_time are derived in payloads.build_period, not carried by the pivot.
    assert "period_start_time" not in result.columns
    assert "period_end_time" not in result.columns
    assert "period_recurrence_type" in result.columns
    assert "period_is_permanent" in result.columns

    assert result["period_start_date"][0] == "2023-06-15T00:00:00+02:00"
    assert result["period_start_date"][1] == "2024-01-01T00:00:00+01:00"
    assert result["period_recurrence_type"][0] == "everyDay"
    assert result["period_is_permanent"][0] is True
    # No text, around the clock; a night slot is anchored on DT_MAT's day, winter offset.
    assert result["period_time_slots"].to_list() == [
        None,
        [{"start_time": "2024-01-01T22:00:00+01:00", "end_time": "2024-01-01T07:00:00+01:00"}],
    ]


def test_compute_period_fields_filters_null_dt_mat():
    """Test that rows with null DT_MAT are filtered out."""
    from datetime import datetime

    from integrations.co_brest.permanent_lineaire.data_source_integration import (
        compute_period_fields,
    )

    df = pl.DataFrame(
        {
            "DT_MAT": [datetime(2023, 6, 15), None, datetime(2024, 1, 1)],
            "NOARR": ["A", "B", "C"],
            "CONDITION": [None, None, None],
            "DESCR": [None, None, None],
        },
        schema_overrides={"CONDITION": pl.Utf8, "DESCR": pl.Utf8},
    )

    result = compute_period_fields(df)

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

    assert "location_road_type" in result.columns
    assert "location_label" in result.columns
    assert "location_geometry" in result.columns

    assert result["location_road_type"][0] == RoadTypeEnum.RAWGEOJSON.value
    assert result["location_road_type"][1] == RoadTypeEnum.RAWGEOJSON.value

    assert result["location_label"][0] == "Commune A – Rue 1"
    assert result["location_label"][1] == "Commune B – Rue 2"

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

    result = compute_regulation_fields(df)

    assert "regulation_identifier" in result.columns
    assert "regulation_category" in result.columns
    assert "regulation_subject" in result.columns
    assert "regulation_title" in result.columns
    assert "regulation_other_category_text" in result.columns
    assert "regulation_document_url" in result.columns

    assert result["regulation_identifier"].to_list() == ["REG001-0", "REG001-0", "REG002-0"]
    assert result["regulation_title"][0] == "Limitation Vitesse – Rue A"
    assert result["regulation_title"][1] == "Limitation Vitesse – Rue A"  # Same as first row
    assert result["regulation_title"][2] == "Stationnement interdit – Rue C"
    assert result["regulation_other_category_text"][0] == "Circulation"
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

    result = compute_regulation_fields(df)

    assert result["regulation_document_url"][0] == "https://example.com/arrete1.pdf"
    assert result["regulation_document_url"][1] == "https://example.com/arrete1.pdf"
    assert result["regulation_document_url"][2] is None
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

    assert result.height == 2
    assert result["DESCRIPTIF"].to_list() == ["Limitation Vitesse", "Stationnement interdit"]


def test_discard_misleading_rows():
    """Test that discard_misleading_rows drops what DiaLog would publish wrong, and only that."""
    from integrations.co_brest.permanent_lineaire.data_source_integration import (
        discard_misleading_rows,
    )

    # (NOARR, DESCRIPTIF, SENS, CONDITION, DESCR, POIDS, HAUTEUR)
    rows = [
        ("one-way-0", "Sens interdit / Sens unique", 0, None, None, 0.0, 0.0),
        ("one-way-1", "Sens interdit / Sens unique", 1, None, None, 0.0, 0.0),
        ("one-direction", "Limitation Vitesse", 1, None, None, 0.0, 0.0),
        ("no-tonnage", "Interdit aux transports de marchandises", 0, None, None, None, None),
        ("unread-note", "Stationnement interdit", 0, None, "en épis", 0.0, 0.0),
        (
            "local-access-and-more",
            "Limitation Poids",
            0,
            "sauf desserte locale, sens V.C. 6 -> rue Danton",
            None,
            3.5,
            0.0,
        ),
        (
            "more-in-other-field",
            "Limitation Poids",
            0,
            "sauf desserte locale",
            "transport de marchandises",
            3.5,
            0.0,
        ),
        ("short-number", "Limitation Vitesse", 0, None, "30", 0.0, 0.0),
        ("truncated-exemption", "Interdit dans les 2 sens", 0, "sauf", None, 0.0, 0.0),
        ("invalid-clock", "Limitation Poids", 0, "interdit de 25H à 7H", None, 3.5, 0.0),
        ("height", "Limitation Hauteur", 0, None, None, 0.0, 2.1),
        ("blank-text", "Stationnement interdit", None, " ", None, 0.0, 0.0),
        ("local-access", "Limitation Poids", 0, " Sauf  desserte locale ", None, 3.5, 0.0),
        ("time-slot", "Limitation Poids", 0, "interdit de 22H à 7H", None, 3.5, 0.0),
        (
            "neutral-note",
            "Stationnement interdit aux poids-lourds",
            0,
            "interdit sur chaussée",
            None,
            3.5,
            0.0,
        ),
    ]
    columns = ["NOARR", "DESCRIPTIF", "SENS", "CONDITION", "DESCR", "POIDS", "HAUTEUR"]
    df = pl.DataFrame(rows, schema=columns, orient="row").with_columns(pl.lit(0.0).alias("LARGEUR"))

    result = discard_misleading_rows(df)

    assert result["NOARR"].to_list() == [
        "height",
        "blank-text",
        "local-access",
        "time-slot",
        "neutral-note",
    ]


def test_read_free_text():
    """Test that the free text is read in full, or reported as not read."""
    from integrations.co_brest.permanent_lineaire.data_source_integration import (
        read_free_text,
    )

    assert read_free_text("interdit entre 22H et 6H", "sauf desserte locale") == {
        "fully_read": True,
        "local_access": True,
        "time_slots": [["22:00", "06:00"]],
    }
    assert read_free_text("de 20h00 à 6h00", None)["time_slots"] == [["20:00", "06:00"]]
    assert read_free_text(None, "Voie verte")["fully_read"] is True
    # Days of the week are not read: the hours alone would publish every day.
    assert (
        read_free_text("les lundis , mardis de 8h15 à 9h15, de 11h30 à 12h15", None)["fully_read"]
        is False
    )
    assert read_free_text("excepté la desserte riveraine", None)["local_access"] is True
    assert read_free_text("excepté la desserte riveraine", None)["fully_read"] is True
    # Another exemption next to it is not read; nor is the producer's typo corrected.
    assert (
        read_free_text(
            "excepté la desserte riveraine et les véhicules d'entretien des ouvrages publics", None
        )["fully_read"]
        is False
    )
    assert read_free_text("sauf desserte rivearaine", None)["fully_read"] is False


def test_compute_vehicle_fields_restricts_to_the_threshold():
    """Test that a gauge limit targets the vehicles over it, never every vehicle (R-33)."""
    from integrations.co_brest.permanent_lineaire.data_source_integration import (
        compute_vehicle_fields,
    )

    df = pl.DataFrame(
        {
            "DESCRIPTIF": [
                "Limitation Hauteur",
                "Limitation Largeur",
                "Limitation Poids",
                "Interdit dans les 2 sens",
            ],
            "POIDS": [0.0, 0.0, 12.0, 0.0],
            "HAUTEUR": [1.9, 0.0, 0.0, 0.0],
            "LARGEUR": [0.0, 2.2, 0.0, 0.0],
            "CYCLO": [False, False, False, False],
            "VELO": [False, False, False, False],
            "CONDITION": ["", "", "", ""],
            "DESCR": ["", "", "", ""],
        }
    )

    result = compute_vehicle_fields(df)

    assert result["vehicle_restricted_types"].to_list() == [
        ["dimensions"],
        ["dimensions"],
        ["heavyGoodsVehicle"],
        None,
    ]
    assert result["vehicle_all_vehicles"].to_list() == [False, False, False, True]
    assert result["vehicle_max_height"].to_list() == [1.9, None, None, None]
    assert result["vehicle_heavyweight_max_weight"].to_list() == [None, None, 12.0, None]


def test_compute_vehicle_fields_reads_local_access():
    """Test that "sauf desserte locale" becomes the desserteLocale exemption, next to the
    exemptions the row already had."""
    from integrations.co_brest.permanent_lineaire.data_source_integration import (
        compute_vehicle_fields,
    )

    df = pl.DataFrame(
        {
            "DESCRIPTIF": [
                "Limitation Poids",
                "Interdit à  tous véhicules à moteur",
                "Interdit dans les 2 sens",
            ],
            "POIDS": [3.5, 0.0, 0.0],
            "HAUTEUR": [0.0, 0.0, 0.0],
            "LARGEUR": [0.0, 0.0, 0.0],
            "CYCLO": [False, False, False],
            "VELO": [False, False, False],
            "CONDITION": ["sauf desserte locale", None, None],
            "DESCR": [None, "Sauf desserte locale", None],
        }
    )

    result = compute_vehicle_fields(df)

    assert result["vehicle_exempted_types"].to_list() == [
        ["desserteLocale"],
        ["bicycle", "pedestrians", "desserteLocale"],
        None,
    ]
    assert "_local_access" not in result.columns


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

    assert result.height == 1
    assert result["measure_max_speed"][0] == 50
