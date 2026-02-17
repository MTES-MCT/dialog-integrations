import pandera.polars as pa


class SartheRawDataSchema(pa.DataFrameModel):
    """Schema for raw data from Sarthe API - only columns we actually use."""

    infobulle: str | None = pa.Field(nullable=True)
    VITESSE: float | None = pa.Field(nullable=True)  # Will be cast to int during validation
    annee: float | None = pa.Field(nullable=True)
    date_modif: str
    geo_shape: str
    loc_txt: str | None = pa.Field(nullable=True)
    longueur: float | None = pa.Field(nullable=True)

    class Config(pa.DataFrameModel.Config):
        strict = False  # Allow extra columns in raw data
        coerce = True  # Allow type coercion during validation
