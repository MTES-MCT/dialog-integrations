"""Schema for Rennes Circulation Interdite."""

import pandera.polars as pa


class RennesCirculationInterditeRawDataSchema(pa.DataFrameModel):
    id: int
    sens_circule: str | None = pa.Field(nullable=True)
    nom_voie: str | None = pa.Field(nullable=True)
    nom_commune: str | None = pa.Field(nullable=True)
    code_insee: int | None = pa.Field(nullable=True)
    geo_shape: bytes | None = pa.Field(nullable=True)
