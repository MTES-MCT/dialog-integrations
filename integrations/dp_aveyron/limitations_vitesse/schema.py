"""Schema for Aveyron limitations de vitesse parquet data."""

import pandera.polars as pa


class AveyronLimitationsVitesseRawDataSchema(pa.DataFrameModel):
    """Schema for the raw Aveyron limitations de vitesse data.

    The producer ships every numeric column as a string. Declaring the target types here
    makes `coerce` do the conversion once, and fail loudly if a value is not convertible.
    """

    geo_point_2d: bytes | None = pa.Field(nullable=True)
    geo_shape: bytes | None = pa.Field(nullable=True)
    num_arrete: str | None = pa.Field(nullable=True)
    route: str | None = pa.Field(nullable=True)
    cote: str | None = pa.Field(nullable=True)
    prd: int | None = pa.Field(nullable=True)
    abd: float | None = pa.Field(nullable=True)
    prf: int | None = pa.Field(nullable=True)
    abf: float | None = pa.Field(nullable=True)
    agglo: str | None = pa.Field(nullable=True)
    limit: int | None = pa.Field(nullable=True)
    sens: int | None = pa.Field(nullable=True)

    class Config(pa.DataFrameModel.Config):
        """Config for the schema."""

        strict = False  # Allow extra columns in raw data
        coerce = True  # Allow type coercion during validation
