"""Tests for Brest preprocessing."""

from pathlib import Path

import polars as pl
import pytest

from integrations.co_brest.integration import Integration
from integrations.co_brest.schema import BrestRawDataSchema


@pytest.fixture
def raw_data():
    """Load test data from data.csv."""
    return pl.read_csv("tests/data/co_brest/data.csv")


@pytest.fixture
def integration():
    """Create Brest integration instance."""
    return Integration.from_organization("co_brest")


def test_validate_raw_data(integration, raw_data):
    """Test that validation succeeds and produces expected columns."""
    validated = integration.validate_raw_data(raw_data)

    expected_columns = set(BrestRawDataSchema.to_schema().columns.keys())
    assert set(validated.columns) == expected_columns
    assert validated.height > 0


def test_preprocess_casts_booleans(integration, raw_data):
    """Test that preprocessing casts VELO and CYCLO to boolean."""
    schema_columns = list(BrestRawDataSchema.to_schema().columns.keys())
    df = raw_data.select(schema_columns)

    preprocessed = integration.preprocess_raw_data(df)

    assert preprocessed["VELO"].dtype == pl.Boolean
    assert preprocessed["CYCLO"].dtype == pl.Boolean
    assert all(v in [True, False] for v in preprocessed["VELO"].to_list())
