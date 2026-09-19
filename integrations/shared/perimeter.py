"""The organisation's perimeter as DiaLog sees it, and the filter that applies it upstream.

DiaLog refuses an emprise when its geometry does not intersect the geometry of the
organisation posting it (« L'organisation ne semble pas avoir les compétences pour
intervenir sur ce linéaire de route »), and the refusal takes the whole regulation down
(R-77). The check is reproducible, because the back end builds the organisation's
geometry from public data and a fixed recipe (`OrganizationAdministrativeBoundariesGeometry`,
`OrganizationRepository` in the DiaLog repository):

1. the communes' contours come from `https://geo.api.gouv.fr/communes?...&fields=contour`,
   selected by the organisation's administrative code — one commune (INSEE), a
   département, a région, or an EPCI such as a métropole;
2. they are unioned (`ST_Union`) and simplified (`ST_SimplifyPreserveTopology`) with a
   tolerance that depends on the code type, in degrees: 0 for a commune, 0.002 for an
   EPCI, 0.001 for a département, 0.003 for a région;
3. the test is a plain `ST_Intersects` in EPSG:4326 between the geometry **as sent** and
   that stored geometry — no buffer, no length fraction, one test per emprise.

Rebuilding the same geometry here and dropping what does not intersect it removes those
refusals before they cost a POST, without a hand-maintained blocklist. The simplification
matters: at 0.002° (~160-220 m) the stored contour cuts corners, so a segment can be a few
hundred metres outside the real boundary and still be accepted, or a few metres inside
and refused. The recipe is DiaLog's, not ours: if the back end changes its tolerance or
its source, this file must follow.

The rebuild is not exact: Douglas-Peucker on a closed ring depends on the ring's start
vertex, which the union engine decides — PostGIS there, GEOS here. Only the geometry
DiaLog stores would match exactly. Hence the second test: an emprise must also touch the
**exact** union of the contours, before simplification. Where the two simplifications
disagree, the emprise lies in the slack the simplification adds along the boundary,
outside the communes themselves — a road on the neighbour's territory. This drops a few
emprises the API would have accepted, and needs no blocklist.

Measurements (Lyon, residual segments, cost of the exact-contour test): R-77 in
`ai/docs/regles-metier.md`, and `ai/docs/vers-l-equipe.md`.
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

GEO_API_URL = "https://geo.api.gouv.fr"
HTTP_TIMEOUT = (10, 120)

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
    # The union before simplification. An emprise must touch both (see the module doc).
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
        response = requests.get(f"{url}/{path}", params=params, timeout=HTTP_TIMEOUT)
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
        """DiaLog's test for one emprise: `ST_Intersects(sent geometry, organisation geometry)`.

        None when the geometry is missing or unreadable — the caller decides what to do
        with it; the API would refuse it for a different reason.
        """
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
    check, and the source's own geometry rules deal with the rest. A row must touch the
    simplified geometry DiaLog stores and the exact union of the contours: see the module
    doc for why both.
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
