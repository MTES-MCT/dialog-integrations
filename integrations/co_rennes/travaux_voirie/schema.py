"""Schema for Rennes Travaux Voirie."""

import pandera.polars as pa
import polars as pl


class RennesTravauxVoirieRawDataSchema(pa.DataFrameModel):
    id: int
    type: str
    date_deb: pl.Datetime(time_unit="ms", time_zone="Europe/Berlin")  # type: ignore
    date_fin: pl.Datetime(time_unit="ms", time_zone="Europe/Berlin")  # type: ignore
    geo_shape: bytes | None = pa.Field(nullable=True)
    localisation: str | None = pa.Field(nullable=True)
    quartier: str | None = pa.Field(nullable=True)
    commune: str | None = pa.Field(nullable=True)
    libelle: str | None = pa.Field(nullable=True)
