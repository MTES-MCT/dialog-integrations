"""Schema for the Métropole de Lyon "chantiers perturbants" WFS layer.

Only the columns we actually use are declared. Anything else is dropped before
validation, and a column that disappears upstream fails the whole integration —
which is the point: a silent schema change must not become silent data loss.
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
        strict = False  # Allow extra columns
        coerce = True  # Allow type coercion during validation
