from datetime import date, datetime
from zoneinfo import ZoneInfo

import polars as pl
import pytest

from integrations.api import DialogApi
from integrations.co_paris.eudonet.client import EQUALS, GREATER_OR_EQUAL, EudonetError
from integrations.co_paris.eudonet.data_source_integration import (
    DataSourceIntegration,
)
from integrations.co_paris.eudonet.extraction import (
    check_extraction_completeness,
    perimeter_criteria,
)
from integrations.co_paris.eudonet.raw_rows import (
    MEASURE_PARAMETER_PAIRS,
    RAW_COLUMNS,
    build_raw_dataframe,
    needs_locations,
)
from integrations.co_paris.eudonet.schema import EudonetRawDataSchema
from tests.co_paris.eudonet_fixture import load_fixture, read_eudonet_from_the_fixture


class StubSettings:
    organization = "co_paris"
    base_url = "http://offline.invalid"
    client_id = "offline"
    client_secret = "offline"
    eudonet_paris_credentials = None


@pytest.fixture
def tables():
    return load_fixture()


@pytest.fixture
def raw(tables):
    return build_raw_dataframe(tables)


@pytest.fixture
def source(monkeypatch):
    monkeypatch.delenv("EUDONET_PARIS_CREDENTIALS", raising=False)
    monkeypatch.setattr(DialogApi, "public_identifiers", lambda self, insee_code: [])
    return DataSourceIntegration(StubSettings(), client=None)  # type: ignore[arg-type]


def test_the_perimeter_is_permanent_in_force_or_temporary_still_running():
    where = perimeter_criteria(date(2026, 9, 8))

    permanent, temporary = where["WhereCustoms"]
    assert [c["Criteria"]["Value"] for c in permanent["WhereCustoms"]] == ["7", "96"]

    type_, end_date, states = temporary["WhereCustoms"]
    assert type_["Criteria"] == {"Field": "1108", "Operator": EQUALS, "Value": "8"}
    assert end_date["Criteria"] == {
        "Field": "1110",
        "Operator": GREATER_OR_EQUAL,
        "Value": "2026/09/08",
    }
    assert [c["Criteria"]["Value"] for c in states["WhereCustoms"]] == ["8640", "13789", "8642"]


def test_the_raw_dataframe_matches_the_column_contract(raw):
    assert raw.columns == list(RAW_COLUMNS)
    assert dict(raw.schema) == RAW_COLUMNS


def test_there_is_one_row_per_location_plus_the_childless_parents(raw, tables):
    carried = {m["FileId"] for m in tables["1200"] if needs_locations(m)}
    carried_locations = [row for row in tables["2700"] if row["1202_fid"] in carried]
    measures_with_location = {row["1202_fid"] for row in carried_locations}
    locationless_measures = [m for m in tables["1200"] if m["FileId"] not in measures_with_location]
    regulations_with_measure = {m["1101_fid"] for m in tables["1200"]}
    childless_regulations = [
        a for a in tables["1100"] if a["FileId"] not in regulations_with_measure
    ]

    assert raw.height == (
        len(carried_locations) + len(locationless_measures) + len(childless_regulations)
    )
    assert raw["l_file_id"].null_count() == len(locationless_measures) + len(childless_regulations)
    assert raw["m_file_id"].null_count() == len(childless_regulations)


def test_every_location_row_carries_its_own_measure_and_regulation(raw, tables):
    measure_of_location = {row["FileId"]: row["1202_fid"] for row in tables["2700"]}
    regulation_of_measure = {row["FileId"]: row["1101_fid"] for row in tables["1200"]}

    located = raw.filter(pl.col("l_file_id").is_not_null())
    assert located.height > 0
    for row in located.iter_rows(named=True):
        assert row["m_file_id"] == measure_of_location[row["l_file_id"]]
        assert row["a_file_id"] == regulation_of_measure[row["m_file_id"]]


def test_the_rows_do_not_depend_on_the_order_eudonet_serves_them(raw, tables):
    reversed_tables = {table: rows[::-1] for table, rows in tables.items()}
    assert build_raw_dataframe(reversed_tables).equals(raw)


def test_the_schema_declares_the_raw_columns_in_their_order():
    """Both declare the raw columns: a column added to one only would go unchecked."""
    assert list(EudonetRawDataSchema.to_schema().columns) == list(RAW_COLUMNS)


def test_the_fixture_validates_against_the_pandera_schema(source, raw):
    validated = source.validate_raw_data(raw)

    assert validated.height == raw.height
    assert set(validated.columns) == set(EudonetRawDataSchema.to_schema().columns)


def test_dates_come_from_the_database_value_not_the_displayed_one(raw, tables):
    regulation = next(a for a in tables["1100"] if a.get("1109_db"))
    row = raw.filter(pl.col("a_file_id") == regulation["FileId"]).row(0, named=True)

    expected = date(*(int(part) for part in regulation["1109_db"][:10].split("/")))
    assert row["a_start_date"] == expected
    assert raw.schema["a_start_date"] == pl.Date
    assert raw.schema["a_end_date"] == pl.Date


def test_a_permanent_regulation_has_no_end_date(raw):
    permanent = raw.filter(pl.col("a_type") == "Permanent")
    assert permanent.height > 0
    assert permanent["a_end_date"].null_count() == permanent.height


def test_eudonet_empty_strings_become_nulls(raw, tables):
    empty = [row for row in tables["2700"] if row.get("2755") == ""]
    assert empty, "the fixture no longer holds a location with an empty 2755"

    rows = raw.filter(pl.col("l_file_id").is_in([row["FileId"] for row in empty]))
    assert rows["l_point_house_number"].null_count() == rows.height
    for name, dtype in RAW_COLUMNS.items():
        if dtype == pl.Utf8:
            assert raw.filter(pl.col(name) == "").height == 0, name


def _pair_count(measure):
    return sum(1 for name, _ in MEASURE_PARAMETER_PAIRS if measure.get(str(name)))


def test_named_parameters_are_flattened_in_eudonet_order(raw, tables):
    measure = max(tables["1200"], key=_pair_count)
    assert _pair_count(measure) >= 2, "the fixture no longer holds a multi-parameter measure"

    row = raw.filter(pl.col("m_file_id") == measure["FileId"]).row(0, named=True)
    expected = [
        [measure[str(name)], measure.get(str(value)) or ""]
        for name, value in MEASURE_PARAMETER_PAIRS
        if measure.get(str(name))
    ]
    assert row["m_params"] == expected


def test_a_measure_without_parameters_gets_an_empty_list_not_a_null(raw, tables):
    measure = next(m for m in tables["1200"] if _pair_count(m) == 0)

    row = raw.filter(pl.col("m_file_id") == measure["FileId"]).row(0, named=True)
    assert row["m_params"] == []


def test_a_regulation_without_measure_gets_null_parameters():
    tables = {
        "1100": [{"FileId": 1, "1101": "2026T1", "1108": "Temporaire", "1107": "En vigueur"}],
        "1200": [],
        "2700": [],
    }

    row = build_raw_dataframe(tables).row(0, named=True)

    assert row["m_file_id"] is None
    assert row["m_params"] is None
    assert row["l_file_id"] is None


@pytest.mark.parametrize("located, refused", [(3, False), (2, True)])
def test_an_extraction_that_lost_its_locations_is_refused(located, refused):
    measures = [{"FileId": file_id} for file_id in range(1, 5)]
    locations = [{"1202_fid": file_id} for file_id in range(1, located + 1)]
    if refused:
        with pytest.raises(EudonetError, match="looks truncated"):
            check_extraction_completeness(measures, locations)
    else:
        check_extraction_completeness(measures, locations)


def test_the_extracted_tables_become_the_raw_columns(source, monkeypatch):
    read_eudonet_from_the_fixture(monkeypatch)

    raw = source.fetch_raw_data()

    assert raw.columns == list(RAW_COLUMNS)
    assert raw.height > 0


@pytest.mark.parametrize(
    "base_url, published",
    [("https://dialog.beta.gouv.fr", False), ("https://dialog-staging.osc-fr1.scalingo.io", True)],
)
def test_the_police_prefecture_is_never_published_on_the_production_host(base_url, published):
    settings = StubSettings()
    settings.base_url = base_url
    source = DataSourceIntegration(settings, client=None)  # type: ignore[arg-type]
    assert source._publishes_police_prefecture() is published


def test_without_credentials_the_failure_names_the_variable(source):
    with pytest.raises(EudonetError, match="EUDONET_PARIS_CREDENTIALS is not set"):
        source.fetch_raw_data()


def test_credentials_are_read_from_the_organization_settings(source, monkeypatch):
    seen = {}

    class Settings(StubSettings):
        eudonet_paris_credentials = '{"user": "someone"}'

    def record(client, today):
        seen["credentials"] = client.credentials
        seen["today"] = today
        return {}

    monkeypatch.setattr(
        "integrations.co_paris.eudonet.data_source_integration.extract_tables", record
    )
    source.organization_settings = Settings()  # type: ignore[assignment]
    source.fetch_raw_data()

    assert seen["credentials"] == {"user": "someone"}
    assert seen["today"] == datetime.now(ZoneInfo("Europe/Paris")).date()


def test_only_the_measures_dialog_can_carry_need_their_locations():
    assert needs_locations({"1202": "interdiction de stationnement"})
    assert needs_locations({"1202": "aire piétonne"})
    assert not needs_locations({"1202": "stationnement réservé"})
    assert not needs_locations({"1202": "interdiction de klaxonner"})
