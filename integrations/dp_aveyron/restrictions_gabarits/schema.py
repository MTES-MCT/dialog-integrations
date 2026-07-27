"""Schema for Aveuron restriction gabarits CSV data."""

import pandera.polars as pa


class AveyronPrescriptionsRoutieresRawDataSchema(pa.DataFrameModel):
    """Schema for the raw Aveyron restriction gabarits data."""

    geo_point_2d: bytes | None = pa.Field(nullable=True)
    geo_shape: bytes | None = pa.Field(nullable=True)
    route: str | None = pa.Field(nullable=True)
    prd: int | None = pa.Field(nullable=True)
    abd: int | None = pa.Field(nullable=True)
    prf: int | None = pa.Field(nullable=True)
    abf: int | None = pa.Field(nullable=True)
    # commune: str | None = pa.Field(nullable=True)
    prescripti: str | None = pa.Field(nullable=True)
    panneau: str | None = pa.Field(nullable=True)
    numero_dar: str | None = pa.Field(nullable=True)
    date_darre: str | None = pa.Field(nullable=True)
    observatio: str | None = pa.Field(nullable=True)

    class Config(pa.DataFrameModel.Config):
        """Config for the schema."""

        strict = False  # Allow extra columns in raw data
        coerce = True  # Allow type coercion during validation
