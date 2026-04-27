import pandera.polars as pa
import polars as pl


class SartheChantiersRoutiersSchema(pa.DataFrameModel):
    """Schema for raw data from Sarthe API - Chantiers Routiers - only columns we actually use."""

    objectid: int
    longueur: int | None = pa.Field(nullable=True)
    loc_txt: str | None = pa.Field(nullable=True)
    # commentaires: str | None = pa.Field(nullable=True)
    # maitre_ouvrage: str | None = pa.Field(nullable=True)
    date_fin: pl.Datetime(time_unit="ms", time_zone="Europe/Berlin")  # type: ignore
    nature_trvx: str | None = pa.Field(nullable=True)
    mode_exp: str | None = pa.Field(nullable=True)
    date_debut: pl.Datetime(time_unit="ms", time_zone="Europe/Berlin")  # type: ignore
    geo_shape: bytes
    geo_point_2d: bytes

    class Config(pa.DataFrameModel.Config):
        strict = False  # Allow extra columns in raw data
        coerce = True  # Allow type coercion during validation
