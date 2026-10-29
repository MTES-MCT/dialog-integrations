"""The organisation's perimeter as DiaLog sees it, and the filter that applies it upstream.

DiaLog refuses an emprise that does not intersect the organisation's geometry, and the
refusal takes the whole regulation down. That geometry is reproducible: the
geo.api.gouv.fr contours of the organisation's communes, unioned, simplified with a
tolerance per code type, then a plain `ST_Intersects` with the geometry as sent. The recipe
is DiaLog's (`OrganizationAdministrativeBoundariesGeometry`): if it changes, follow it.

The rebuild is not exact (Douglas-Peucker depends on the ring's start vertex, which PostGIS
and GEOS choose differently), so an emprise must also touch the exact union of the
contours: what only the simplification's slack accepts lies on the neighbour's territory.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

import polars as pl
import requests
from loguru import logger
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union
from shapely.prepared import prep

from toolkit.connectors.download import DOWNLOAD_TIMEOUT

GEO_API_URL = "https://geo.api.gouv.fr"

# `ST_SimplifyPreserveTopology` tolerance applied by DiaLog, in degrees, per code type.
SIMPLIFICATION_BY_CODE_TYPE = {
    "insee": 0.0,
    "epci": 0.002,
    "departement": 0.001,
    "region": 0.003,
}

QUERY_BY_CODE_TYPE = {
    "insee": lambda code: (f"communes/{code}", {"fields": "contour"}),
    "epci": lambda code: ("communes", {"codeEpci": code, "fields": "contour"}),
    "departement": lambda code: ("communes", {"codeDepartement": code, "fields": "contour"}),
    "region": lambda code: ("communes", {"codeRegion": code, "fields": "contour"}),
}


@dataclass(frozen=True)
class Perimeter:
    """An organisation's territory, built the way DiaLog builds it."""

    code_type: str
    code: str
    # The geometry DiaLog stores, rebuilt: unioned then simplified.
    geometry: BaseGeometry
    # The union before simplification: an emprise must touch both.
    exact: BaseGeometry | None = None

    @classmethod
    def fetch(cls, code_type: str, code: str, *, url: str = GEO_API_URL) -> "Perimeter":
        """Download the communes' contours and rebuild DiaLog's organisation geometry."""
        if code_type not in QUERY_BY_CODE_TYPE:
            raise ValueError(
                f"Unknown code type {code_type!r}, expected one of {sorted(QUERY_BY_CODE_TYPE)}"
            )
        path, params = QUERY_BY_CODE_TYPE[code_type](code)
        logger.info(f"Downloading commune contours for {code_type} {code} from {url}")
        response = requests.get(f"{url}/{path}", params=params, timeout=DOWNLOAD_TIMEOUT)
        response.raise_for_status()
        payload = response.json()
        communes = payload if isinstance(payload, list) else [payload]
        contours = [shape(c["contour"]) for c in communes if c.get("contour")]
        if not contours:
            raise ValueError(f"geo.api.gouv.fr returned no contour for {code_type} {code}")
        logger.info(f"{len(contours)} commune contour(s) for {code_type} {code}")
        return cls.build(code_type, code, contours)

    @classmethod
    def build(cls, code_type: str, code: str, contours: list[BaseGeometry]) -> "Perimeter":
        """Union then simplify, with DiaLog's tolerance for this code type."""
        exact = unary_union(contours)
        tolerance = SIMPLIFICATION_BY_CODE_TYPE[code_type]
        geometry = exact.simplify(tolerance, preserve_topology=True) if tolerance else exact
        return cls(code_type=code_type, code=code, geometry=geometry, exact=exact)

    def intersects(self, geojson: str | None) -> bool | None:
        """DiaLog's test for one emprise, plus the exact contour; None when the geometry is
        missing or unreadable, which the API would refuse for another reason."""
        if geojson is None:
            return None
        try:
            geometry = shape(json.loads(geojson))
        except Exception:  # noqa: BLE001 — anything shapely or json refuses to read
            return None
        if not self.geometry.intersects(geometry):
            return False
        return self.exact is None or self.exact.intersects(geometry)


def discard_outside_perimeter(
    df: pl.DataFrame, perimeter: Perimeter, geometry_column: str = "geometry"
) -> pl.DataFrame:
    """Drop the rows whose geometry DiaLog would refuse for this organisation.

    Rows without a readable geometry are kept: this filter only reproduces the competence
    check.
    """
    prepared = prep(perimeter.geometry)
    exact = prep(perimeter.exact) if perimeter.exact is not None else None

    def keep(geojson: str | None) -> bool:
        if geojson is None:
            return True
        try:
            geometry = shape(json.loads(geojson))
        except Exception:  # noqa: BLE001 — anything shapely or json refuses to read
            return True
        return prepared.intersects(geometry) and (exact is None or exact.intersects(geometry))

    inside = df.get_column(geometry_column).map_elements(
        keep, return_dtype=pl.Boolean, skip_nulls=False
    )
    n_dropped = df.height - inside.sum()
    if n_dropped:
        logger.info(
            f"Discarding {n_dropped} rows outside the {perimeter.code_type} {perimeter.code} "
            "perimeter (DiaLog would refuse them, or they lie in the slack its simplification "
            "adds along the boundary)"
        )
    return df.filter(inside)
