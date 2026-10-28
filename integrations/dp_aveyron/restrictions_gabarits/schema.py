"""Schema for Aveyron restriction gabarits data."""

import pandera.polars as pa


class AveyronPrescriptionsRoutieresRawDataSchema(pa.DataFrameModel):
    """The producer ships numbers as strings: `coerce` converts them, or fails loudly."""

    geo_point_2d: bytes | None = pa.Field(nullable=True)
    geo_shape: bytes | None = pa.Field(nullable=True)
    route: str | None = pa.Field(nullable=True)
    prd: int | None = pa.Field(nullable=True)
    abd: float | None = pa.Field(nullable=True)
    prf: int | None = pa.Field(nullable=True)
    abf: float | None = pa.Field(nullable=True)
    prescripti: str | None = pa.Field(nullable=True)
    panneau: str | None = pa.Field(nullable=True)
    numero_dar: str | None = pa.Field(nullable=True)
    date_darre: str | None = pa.Field(nullable=True)
    observatio: str | None = pa.Field(nullable=True)

    class Config(pa.DataFrameModel.Config):
        strict = False  # Allow extra columns in raw data
        coerce = True  # Allow type coercion during validation
