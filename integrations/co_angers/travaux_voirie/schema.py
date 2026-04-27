"""Schema for Angers Travaux Voirie."""

import pandera.polars as pa
import polars as pl


class AngersTravauxVoirieRawDataSchema(pa.DataFrameModel):
    id: int
    description: str
    startAt: pl.Datetime(time_unit="ms", time_zone="Europe/Berlin")  # type: ignore
    endAt: pl.Datetime(time_unit="ms", time_zone="Europe/Berlin")  # type: ignore
    location: bytes | None = pa.Field(nullable=True)
    address: str | None = pa.Field(nullable=True)
    title: str | None = pa.Field(nullable=True)
