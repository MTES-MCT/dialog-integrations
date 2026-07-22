"""Schema for Issy-les-Moulineaux Travaux Voirie"""

import pandera.polars as pa
from datetime import date


class IssylesMoulineauxTravauxRawDataSchema(pa.DataFrameModel):
    reference: str
    type_travaux: str | None = pa.Field(nullable=True)
    mesure_titre: str | None = pa.Field(nullable=True)
    rue_principal: str | None = pa.Field(nullable=True)
    commune: str | None = pa.Field(nullable=True)
    description: str | None = pa.Field(nullable=True)
    date_debut: date | None = pa.Field(nullable=True)
    date_fin: date | None = pa.Field(nullable=True)
    geolocalisation: bytes | None = pa.Field(nullable=True)
    url: str | None = pa.Field(nullable=True)
