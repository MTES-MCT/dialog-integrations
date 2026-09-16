"""From the street sections DiaLog computes for a zone to the ones worth publishing.

A `zone` location is a polygon; DiaLog turns it into the BD TOPO road sections it
intersects, **clipped at the polygon's edge**, and exposes the result as the location's
effective `geometry`. Two things make that result say more than the producer did:

- **slivers**: any street whose centreline enters the polygon by a couple of metres
  yields a piece of a couple of metres, and a `noEntry` on that piece closes the
  neighbouring street for a satnav. On the Lyon work sites published on 2026-09-15,
  284 of 1 031 pieces were shorter than 5 m — 2 % of the linear, half of the regulations;
- **parallel roads**: a polygon wide enough to hold the street it is about *and* the
  ones running alongside closes them all. Boulevard des Droits de l'Homme
  (Vaulx-en-Velin, 2026-09-16): 1.1 km of polygon, 4.4 km of sections — both
  carriageways, the service road and rue Auguste Brunel, for a gas main.

Nothing in the API lets us filter what the zone computes, so the integration reads the
computed geometry back and:

1. republishes the location as `rawGeoJSON` holding only the pieces at least
   `MIN_SECTION_LENGTH_M` long (metres, after projection to RGF93 / Lambert-93,
   EPSG:2154);
2. refuses the regulation altogether when a zone's sections add up to more than
   `MAX_SECTIONS_PER_LENGTH` times the polygon's length (half its perimeter): the
   polygon then covers several roads side by side, and nothing tells which one the
   restriction is about. Measured on the 208 Lyon sites: median 0.93, a dual
   carriageway or a square tops out at 1.95, then route de Genas at 2.3 and 2.6 (two
   carriageways, the busway platform, the interchange ramps) and the boulevard at 3.8.
   The threshold sits in the gap between 1.95 and 2.3; crossing streets and junctions
   never bring the ratio near it.

Everything is computed from the polygon and the geometry DiaLog returned: no further
call, no street naming — the two were tried on 2026-09-16 and flagged crossings and
dead ends as often as real overlaps.
"""

import json

from pyproj import Transformer
from shapely.geometry import LineString, MultiLineString, shape
from shapely.ops import transform

MIN_SECTION_LENGTH_M = 5.0
MAX_SECTIONS_PER_LENGTH = 2.1

_TO_METRES = Transformer.from_crs("EPSG:4326", "EPSG:2154", always_xy=True).transform


def line_pieces(geometry: str | None) -> list[LineString]:
    """Every LineString inside a GeoJSON geometry, whatever wraps it."""
    if not geometry:
        return []
    shp = shape(json.loads(geometry))
    pieces: list[LineString] = []
    for part in getattr(shp, "geoms", None) or [shp]:
        if isinstance(part, LineString):
            pieces.append(part)
        else:
            # A nested MultiLineString inside a GeometryCollection.
            pieces.extend(
                p for p in getattr(part, "geoms", None) or [] if isinstance(p, LineString)
            )
    return pieces


def length_m(piece: LineString) -> float:
    return transform(_TO_METRES, piece).length


def long_pieces(
    geometry: str | None, min_length_m: float = MIN_SECTION_LENGTH_M
) -> list[LineString]:
    return [piece for piece in line_pieces(geometry) if length_m(piece) >= min_length_m]


def sections_of(geometry: str | None, min_length_m: float = MIN_SECTION_LENGTH_M) -> str | None:
    """The pieces of `geometry` at least `min_length_m` long, as a MultiLineString.

    Returns None when nothing is long enough: the caller then keeps the zone as it is,
    which is what DiaLog would have published anyway.
    """
    kept = long_pieces(geometry, min_length_m)
    if not kept:
        return None
    return json.dumps(MultiLineString(kept).__geo_interface__)


def sections_per_length(
    polygon: str | None,
    geometry: str | None,
    min_length_m: float = MIN_SECTION_LENGTH_M,
) -> float:
    """How many road lengths the polygon holds: sections linear over polygon length.

    The polygon's length is half its perimeter, a fair estimate for the elongated
    footprints work sites are drawn as. 1 is one road, a dual carriageway stays under
    2, beyond that several roads side by side.
    """
    if not polygon:
        return 0.0
    half_perimeter = transform(_TO_METRES, shape(json.loads(polygon))).length / 2
    if half_perimeter <= 0:
        return 0.0
    return sum(length_m(piece) for piece in long_pieces(geometry, min_length_m)) / half_perimeter
