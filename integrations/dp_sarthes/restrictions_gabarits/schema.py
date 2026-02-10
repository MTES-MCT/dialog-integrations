"""Schema for Sarthe restriction gabarits CSV data."""

import pandera.polars as pa


class SarthesRestrictionGabaritsRawDataSchema(pa.DataFrameModel):
    """Schema for the raw Sarthe restriction gabarits data."""

    objectid: int
    localisation_curviligne: str | None = pa.Field(nullable=True)
    communes: str | None = pa.Field(nullable=True)
    longueur: float | None = pa.Field(nullable=True)
    type_de_restriction: str | None = pa.Field(nullable=True)
    nature: str | None = pa.Field(nullable=True)
    largeur: float | None = pa.Field(nullable=True)
    tonnage: float | None = pa.Field(nullable=True)
    hauteur: float | None = pa.Field(nullable=True)
    arrete: str | None = pa.Field(nullable=True)
    agglo: str | None = pa.Field(nullable=True)
    observation: str | None = pa.Field(nullable=True)
    atd: str | None = pa.Field(nullable=True)
    site: str | None = pa.Field(nullable=True)
    autorisation: str | None = pa.Field(nullable=True)
    commentaires: str | None = pa.Field(nullable=True)
    date_creation: str | None = pa.Field(nullable=True)
    date_modification: str | None = pa.Field(nullable=True)
    geo_shape: str | None = pa.Field(nullable=True)
    geo_point_2d: str | None = pa.Field(nullable=True)

    class Config(pa.DataFrameModel.Config):
        """Config for the schema."""

        strict = False  # Allow extra columns in raw data
        coerce = True  # Allow type coercion during validation
