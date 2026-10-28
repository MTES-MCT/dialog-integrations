"""Schema for the Métropole de Lyon `pvochausseetrottoir` WFS layer.

Of its 53 columns, only these say something a DiaLog regulation can express.
"""

import pandera.polars as pa


class LyonChausseesTrottoirsRawDataSchema(pa.DataFrameModel):
    # From the road reference system, so better than a `gid`, but not stable for good:
    # segments get split and merged, ~20 a month, as the producer confirmed (R-22).
    codetroncon: str
    codefuv: str | None = pa.Field(nullable=True)

    nomvoie1: str | None = pa.Field(nullable=True)
    commune1: str | None = pa.Field(nullable=True)
    insee1: str | None = pa.Field(nullable=True)

    # Drops pedestrian areas on a private domain. Measured, it does not predict the API's
    # refusals.
    domanialite: str | None = pa.Field(nullable=True)

    # Zone à trafic limité: no entry except local access. True on 338 segments of the
    # Presqu'île (Lyon 1er and 2e); a measure of its own, not a speed.
    ztl: bool | None = pa.Field(nullable=True)

    # The producer's own label for the zone: agrees exactly with `limitationvitesse` wherever
    # filled, but filled on 12 % of the 5 km/h rows only.
    reglementationzca: str | None = pa.Field(nullable=True)

    limitationvitesse: str | None = pa.Field(nullable=True)
    limitationtonnage: float | None = pa.Field(nullable=True)
    limitationhauteur: float | None = pa.Field(nullable=True)
    limitationlargeur: float | None = pa.Field(nullable=True)
    limitationlongueur: float | None = pa.Field(nullable=True)

    # Hand-typed by 58 municipalities: the order number if any, "Proposition zone apaisable"
    # for a mere project. Null on 34 % of the rows.
    precisionreglementation: str | None = pa.Field(nullable=True)

    # GeoJSON LineString, serialized as a string, EPSG:4326 in longitude/latitude order.
    geometry: str | None = pa.Field(nullable=True)

    class Config(pa.DataFrameModel.Config):
        strict = False
        coerce = True
