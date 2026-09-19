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
    # than a `gid`. It is not stable for good: segments get split and merged, ~20 a month,
    # as the producer confirmed (R-22).
    codetroncon: str
    codefuv: str | None = pa.Field(nullable=True)

    # Location, as text.
    nomvoie1: str | None = pa.Field(nullable=True)
    commune1: str | None = pa.Field(nullable=True)
    insee1: str | None = pa.Field(nullable=True)

    # Who manages the road. Used to drop pedestrian areas on a private domain, which are
    # not roads open to traffic. Says nothing about the API's refusals: measured, it does
    # not predict them.
    domanialite: str | None = pa.Field(nullable=True)

    # Zone à trafic limité: no entry except local access. True on 338 segments of the
    # Presqu'île (Lyon 1er and 2e); a measure of its own, not a speed.
    ztl: bool | None = pa.Field(nullable=True)

    # What the producer itself calls the calmed-traffic zone: "Zone 30", "Zone de
    # rencontre", "Aire Piétonne". The agreement with `limitationvitesse` is exact wherever
    # the column is filled, which is what makes it trustworthy — but it is filled on 12 %
    # of the 5 km/h rows only (counts: `ai/docs/vers-l-equipe.md`). See
    # `discard_unlabelled_pedestrian_areas`.
    reglementationzca: str | None = pa.Field(nullable=True)

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
