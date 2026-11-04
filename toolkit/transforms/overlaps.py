"""Traces of one layer compared on the ground, in metres (Lambert-93)."""

import geopandas as gpd
import numpy as np
import polars as pl
import shapely

# A twin carriageway, or a trace drawn per side, lay up to 20 m from the other on the
# layers read on 2026-10-31.
TWIN_DISTANCE = 20
TWIN_SHARE = 0.5
CONTRADICTION_BUFFER = 3
CONTRADICTION_OVERLAP = 10
REDRAW_BUFFER = 3
REDRAW_SHARE = 0.5
REDRAW_MIN_LENGTH = 10


def twin_traces(
    df: pl.DataFrame,
    geometry: str,
    name: str | None = None,
    ignore: pl.Series | None = None,
) -> pl.Series:
    """Whether another trace, not touching it (of the same `name`, if given), runs within
    `TWIN_DISTANCE` along at least `TWIN_SHARE` of either.

    A trace in `ignore` (a redraw `drawn_twice` already leaves out) is no one's twin: the
    trace it redraws stays.
    """
    traces = _metric(df[geometry])
    left, right = _pairs(traces, TWIN_DISTANCE)
    keep = ~shapely.intersects(traces[left], traces[right])
    if ignore is not None:
        ignored = ignore.to_numpy()
        keep &= ~ignored[left] & ~ignored[right]
    if name is not None:
        keep &= _same(df[name], left, right)
    left, right = left[keep], right[keep]
    alongside = _length_within(traces[left], traces[right], TWIN_DISTANCE)
    return _flag(df.height, left, right, alongside >= TWIN_SHARE * shapely.length(traces[left]))


def contradicted_geometry(
    df: pl.DataFrame, value: str, geometry: str, by: str | None = None
) -> pl.Series:
    """Whether another trace (of the same `by`, if given) setting another `value` overlaps
    it over more than `CONTRADICTION_OVERLAP`. A missing value contradicts nothing: the
    rule on that value judges its row."""
    traces = _metric(df[geometry])
    left, right = _pairs(traces, CONTRADICTION_BUFFER)
    values = df[value].to_numpy()
    missing = df[value].is_null().to_numpy()
    keep = (values[left] != values[right]) & ~missing[left] & ~missing[right]
    if by is not None:
        keep &= _same(df[by], left, right)
    left, right = left[keep], right[keep]
    overlap = _length_within(traces[left], traces[right], CONTRADICTION_BUFFER)
    return _flag(df.height, left, right, overlap > CONTRADICTION_OVERLAP)


def drawn_twice(df: pl.DataFrame, geometry: str, by: str | None = None) -> pl.Series:
    """Whether a longer trace (of the same `by`, if given) lies within `REDRAW_BUFFER` of it
    along at least `REDRAW_SHARE` of its length and over more than `REDRAW_MIN_LENGTH`,
    touching it or not. Only the shorter of a pair is flagged, the later one at equal length.
    """
    traces = _metric(df[geometry])
    left, right = _pairs(traces, REDRAW_BUFFER)
    if by is not None:
        same = _same(df[by], left, right)
        left, right = left[same], right[same]
    lengths = shapely.length(traces)
    shorter = (lengths[left] < lengths[right]) | (
        (lengths[left] == lengths[right]) & (left > right)
    )
    left, right = left[shorter], right[shorter]
    shared = _length_within(traces[left], traces[right], REDRAW_BUFFER)
    flagged = np.zeros(df.height, dtype=bool)
    redrawn = (shared >= REDRAW_SHARE * lengths[left]) & (shared > REDRAW_MIN_LENGTH)
    flagged[left[redrawn]] = True
    return pl.Series(flagged)


def _same(values: pl.Series, left: np.ndarray, right: np.ndarray) -> np.ndarray:
    """Whether both rows of each pair carry the same value; never when either has none."""
    known = values.is_not_null().to_numpy()
    raw = values.to_numpy()
    return known[left] & known[right] & (raw[left] == raw[right])


def _metric(geojson: pl.Series) -> np.ndarray:
    traces = gpd.GeoSeries(shapely.from_geojson(geojson.to_numpy()), crs=4326)
    return traces.to_crs(2154).to_numpy()


def _pairs(traces: np.ndarray, distance: float) -> tuple[np.ndarray, np.ndarray]:
    left, right = shapely.STRtree(traces).query(traces, predicate="dwithin", distance=distance)
    distinct = left != right
    return left[distinct], right[distinct]


def _length_within(traces: np.ndarray, others: np.ndarray, distance: float) -> np.ndarray:
    return shapely.length(shapely.intersection(traces, shapely.buffer(others, distance)))


def _flag(height: int, left: np.ndarray, right: np.ndarray, hit: np.ndarray) -> pl.Series:
    flagged = np.zeros(height, dtype=bool)
    flagged[left[hit]] = True
    flagged[right[hit]] = True
    return pl.Series(flagged)
