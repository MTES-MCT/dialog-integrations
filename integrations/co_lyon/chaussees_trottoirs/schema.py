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
    # than a `gid`. It is not stable for good: the producer confirmed on 2026-09-07 that
    # segments get split and merged, ~20 a month (R-22, measured at ~0,04 % per month).
    codetroncon: str
    codefuv: str | None = pa.Field(nullable=True)

    # Location, as text.
    nomvoie1: str | None = pa.Field(nullable=True)
    commune1: str | None = pa.Field(nullable=True)
    insee1: str | None = pa.Field(nullable=True)

    # Qui administre la voie. Sert à écarter les aires piétonnes de domaine privé, qui ne
    # sont pas de la voirie ouverte à la circulation. Ne dit rien du refus de l'API : c'est
    # mesuré, la domanialité ne le prédit pas.
    domanialite: str | None = pa.Field(nullable=True)

    # Zone à trafic limité : accès interdit sauf desserte locale. Vrai sur 338 tronçons
    # de la Presqu'île (Lyon 1er et 2e), et c'est une mesure à soi, pas une vitesse.
    ztl: bool | None = pa.Field(nullable=True)

    # What the producer itself calls the calmed-traffic zone: "Zone 30" (15 206 rows,
    # all at 30 km/h), "Zone de rencontre" (1 632, all at 20) and "Aire Piétonne" (472,
    # all at 5). The agreement with `limitationvitesse` is exact wherever the column is
    # filled, which is what makes it trustworthy — but it is filled on 12 % of the 5 km/h
    # rows only. See `discard_unlabelled_pedestrian_areas`.
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
