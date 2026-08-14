"""Schema for Issy-les-Moulineaux Travaux Voirie"""

from datetime import date

import pandera.polars as pa
import polars as pl


class IssylesMoulineauxTravauxRawDataSchema(pa.DataFrameModel):
    reference: str
    type_travaux: str | None = pa.Field(nullable=True)
    mesure_titre: list[str] = pa.Field(nullable=True)
    mesures: list[str] = pa.Field(nullable=True)
    rue_principal: str | None = pa.Field(nullable=True)
    commune: str | None = pa.Field(nullable=True)
    description: str | None = pa.Field(nullable=True)
    date_debut: date | None = pa.Field(nullable=True)
    date_fin: date | None = pa.Field(nullable=True)
    geolocalisation: pl.Struct = pa.Field(nullable=True)
    url: str | None = pa.Field(nullable=True)
