from datetime import datetime

import pandera.polars as pa


class Schema(pa.DataFrameModel):
    """Schema for raw data from Brest shapefile after minimal casting
    - only columns we actually use."""

    NOARR: str | None = pa.Field(nullable=True)
    DESCRIPTIF: str | None = pa.Field(nullable=True)
    LIBRU: str | None = pa.Field(nullable=True)
    LIBCO: str | None = pa.Field(nullable=True)
    geometry: str | None = pa.Field(nullable=True)
    SENS: int | None = pa.Field(nullable=True)
    VELO: bool  # After boolean casting
    CYCLO: bool  # After boolean casting
    VITEMAX: int | None = pa.Field(nullable=True)
    POIDS: float | None = pa.Field(nullable=True)
    HAUTEUR: float | None = pa.Field(nullable=True)
    LARGEUR: float | None = pa.Field(nullable=True)
    DT_MAT: datetime | None = pa.Field(nullable=True)

    class Config(pa.DataFrameModel.Config):
        strict = False  # Allow extra columns
        coerce = True  # Allow type coercion during validation
