"""Schema for the Métropole de Lyon `pvochausseetrottoir` WFS layer.

The layer carries 53 columns describing the road network — pavement, slopes, bus lanes,
traffic counts. Only the columns below say something a DiaLog regulation can express:
the speed limit, the vehicle dimension limits, the free-text field where the underlying
order number hides, and the geometry.
"""

import pandera.polars as pa


class LyonChausseesTrottoirsRawDataSchema(pa.DataFrameModel):
    """Schema for the raw Métropole de Lyon roadway data."""

    # Segment identity. `codetroncon` is unique across the layer and comes from the road
    # reference system rather than from the export, which makes it a better candidate
    # than a `gid` — its stability over time is still unverified (R-22).
    codetroncon: str
    codefuv: str | None = pa.Field(nullable=True)

    # Location, as text.
    nomvoie1: str | None = pa.Field(nullable=True)
    commune1: str | None = pa.Field(nullable=True)
    insee1: str | None = pa.Field(nullable=True)

    # The measures themselves.
    limitationvitesse: str | None = pa.Field(nullable=True)
    limitationtonnage: float | None = pa.Field(nullable=True)
    limitationhauteur: float | None = pa.Field(nullable=True)
    limitationlargeur: float | None = pa.Field(nullable=True)
    limitationlongueur: float | None = pa.Field(nullable=True)

    # Free text, hand-typed by 58 municipalities. Holds the order number when there is
    # one, and the "Proposition zone apaisable" annotation when the zone is only a
    # project. Null on 34 % of the rows.
    precisionreglementation: str | None = pa.Field(nullable=True)

    # GeoJSON LineString, serialized as a string, EPSG:4326 in longitude/latitude order.
    geometry: str | None = pa.Field(nullable=True)

    class Config(pa.DataFrameModel.Config):
        """Config for the schema."""

        strict = False  # Allow extra columns in raw data
        coerce = True  # Allow type coercion during validation
