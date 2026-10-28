"""Schema for Aveyron limitations de vitesse parquet data."""

import pandera.polars as pa


class AveyronLimitationsVitesseRawDataSchema(pa.DataFrameModel):
    """The producer ships numbers as strings: `coerce` converts them, or fails loudly."""

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
    # Free text qualifying `limit`: "50 PL>12t", "70 chaussee mouillee".
    limit_spec: str | None = pa.Field(nullable=True)
    sens: int | None = pa.Field(nullable=True)

    class Config(pa.DataFrameModel.Config):
        strict = False  # Allow extra columns in raw data
        coerce = True  # Allow type coercion during validation
