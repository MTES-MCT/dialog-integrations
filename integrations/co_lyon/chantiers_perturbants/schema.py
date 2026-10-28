"""Schema for the Métropole de Lyon "chantiers perturbants" WFS layer, used columns only.

A column that disappears upstream fails the whole integration rather than losing data.
"""

from datetime import date

import pandera.polars as pa


class LyonChantiersPerturbantsRawDataSchema(pa.DataFrameModel):
    gid: int
    nom: str | None = pa.Field(nullable=True)
    nomchantier: str | None = pa.Field(nullable=True)
    commune1: str | None = pa.Field(nullable=True)
    insee: str | None = pa.Field(nullable=True)
    precisionlocalisation: str | None = pa.Field(nullable=True)
    debutchantier: date | None = pa.Field(nullable=True)
    finchantier: date | None = pa.Field(nullable=True)
    descripchantierinternet: str | None = pa.Field(nullable=True)
    typeperturbation: str | None = pa.Field(nullable=True)
    url_document: str | None = pa.Field(nullable=True)
    geometry: str | None = pa.Field(nullable=True)

    class Config(pa.DataFrameModel.Config):
        strict = False
        coerce = True
