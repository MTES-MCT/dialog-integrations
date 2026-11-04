import pandera.polars as pa


class LimogesLimitationTonnageRawDataSchema(pa.DataFrameModel):
    """The weight stays text (`"3.5"`): coerced here, one unreadable value would fail the
    whole validation. The order number too: `08001442` keeps its leading zero."""

    geometry: str | None = pa.Field(nullable=True)
    num_arrete: str | None = pa.Field(nullable=True)
    id_voie: float | None = pa.Field(nullable=True)
    tonnage: str | None = pa.Field(nullable=True)
    nom_voie: str | None = pa.Field(nullable=True)

    class Config(pa.DataFrameModel.Config):
        strict = False
        coerce = True
