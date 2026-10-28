"""From the street sections DiaLog computes for a zone to the ones worth publishing.

DiaLog clips the sections at the polygon's edge: a street entering it by two metres yields a
two-metre piece, and a `noEntry` on it closes that street for a satnav. A polygon wide
enough also holds the roads running alongside, and nothing tells which one is meant.
Street names are not used: they flagged crossings and dead ends as often as real overlaps.
"""

import json

from pyproj import Transformer
from shapely.geometry import LineString, MultiLineString, shape
from shapely.ops import transform

MIN_SECTION_LENGTH_M = 5.0
# Between a dual carriageway (up to ~1.95 on Lyon's work sites) and the first parallel
# roads (2.3); crossing streets never bring the ratio near it.
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
    """The pieces of `geometry` at least `min_length_m` long, as a MultiLineString; None
    when nothing is long enough, and the caller keeps the zone as DiaLog computed it."""
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

    The polygon's length is half its perimeter, fair for the elongated footprints of work
    sites. 1 is one road, a dual carriageway stays under 2.
    """
    if not polygon:
        return 0.0
    half_perimeter = transform(_TO_METRES, shape(json.loads(polygon))).length / 2
    if half_perimeter <= 0:
        return 0.0
    return sum(length_m(piece) for piece in long_pieces(geometry, min_length_m)) / half_perimeter
