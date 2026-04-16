"""Schema for Nantes Circulation Chantier."""

import pandera.polars as pa


class NantesCirculationChantierRawDataSchema(pa.DataFrameModel):
    objectid: int
    contrainte_auto: str | None = pa.Field(nullable=True)
    date_debut: int | None = pa.Field(nullable=True)
    date_fin: int | None = pa.Field(nullable=True)
    geometry: str | None = pa.Field(nullable=True)
    voie: str | None = pa.Field(nullable=True)
    commune: str | None = pa.Field(nullable=True)
    motif: str | None = pa.Field(nullable=True)
    nature: str | None = pa.Field(nullable=True)
    type_chantier: str | None = pa.Field(nullable=True)
