"""Schema for Aveuron restriction gabarits CSV data."""

import pandera.polars as pa


class AveyronLimitationsVitesseRawDataSchema(pa.DataFrameModel):
    """Schema for the raw Aveyron restriction gabarits data."""

    geo_point_2d: bytes | None = pa.Field(nullable=True)
    geo_shape: bytes | None = pa.Field(nullable=True)
    num_arrete: str  | None = pa.Field(nullable=True)
    route: str | None = pa.Field(nullable=True)
    prd: str | None = pa.Field(nullable=True)
    abd: str | None = pa.Field(nullable=True)
    prf: str | None = pa.Field(nullable=True)
    abf: str | None = pa.Field(nullable=True)
    agglo: str | None = pa.Field(nullable=True)
    limit: int | None = pa.Field(nullable=True)
    sens: int | None = pa.Field(nullable=True)

    class Config(pa.DataFrameModel.Config):
        """Config for the schema."""

        strict = False  # Allow extra columns in raw data
        coerce = True  # Allow type coercion during validation
