"""Tests for Sarthes preprocessing."""

from pathlib import Path

import polars as pl
import pytest

from integrations.dp_sarthes.integration import Integration
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
