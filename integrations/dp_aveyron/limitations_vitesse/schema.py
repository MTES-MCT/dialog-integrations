"""Schema for Aveuron restriction gabarits CSV data."""

import pandera.polars as pa


class AveyronLimitationsVitesseRawDataSchema(pa.DataFrameModel):
    """Schema for the raw Aveyron restriction gabarits data."""

    geo_point_2d: bytes | None = pa.Field(nullable=True)
    geo_shape: bytes | None = pa.Field(nullable=True)
    objectid: int
    idroute: str | None = pa.Field(nullable=True)
    decalage: int | None = pa.Field(nullable=True)
    debut: int | None = pa.Field(nullable=True)
    fin: int | None = pa.Field(nullable=True)
    prdeb: int | None = pa.Field(nullable=True)
    absdeb: int | None = pa.Field(nullable=True)
    prfin: int | None = pa.Field(nullable=True)
    absfin: int | None = pa.Field(nullable=True)
    agglo: str | None = pa.Field(nullable=True)
    limit1: int | None = pa.Field(nullable=True)
    limit2: str | None = pa.Field(nullable=True)
    sens: int | None = pa.Field(nullable=True)

    class Config(pa.DataFrameModel.Config):
        """Config for the schema."""

        strict = False  # Allow extra columns in raw data
        coerce = True  # Allow type coercion during validation
