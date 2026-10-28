"""Date the undated permanent periods at the moment of writing (R-39).

The start date stays null up to the snapshot, so it never reads as a change: dated with the
run day earlier, every such regulation would look modified each morning. A creation takes
the run day; an update keeps the date DiaLog holds for the matching measure.

Measures are matched by signature (type, speed, exemptions, dimensions) unless both sides
hold a single one. Restricted types stay out of it: DiaLog reads them back empty.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, time
from typing import Any
from zoneinfo import ZoneInfo

from api.dia_log_client.models import PostApiRegulationsAddBody
from api.dia_log_client.types import Unset
from sync.closure import _parse, _to_paris_iso

PARIS = ZoneInfo("Europe/Paris")

# Save DTO name → `maxCharacteristics` name.
DIMENSION_ATTRIBUTES = (
    ("heavyweight_max_weight", "weight"),
    ("max_height", "height"),
    ("max_width", "width"),
    ("max_length", "length"),
)


def run_day(now: datetime | None = None) -> str:
    """French midnight of the day with that day's offset (`2026-09-19T00:00:00+02:00`): the
    one shape DiaLog reads without shifting it."""
    instant = (now or datetime.now(PARIS)).astimezone(PARIS)
    return datetime.combine(instant.date(), time(), tzinfo=PARIS).isoformat(timespec="seconds")


def has_undated_period(regulation: PostApiRegulationsAddBody) -> bool:
    """True when at least one period of the regulation still has no start date."""
    return any(
        _is_undated(period) for measure in regulation.measures or [] for period in _periods(measure)
    )


def date_creation(regulation: PostApiRegulationsAddBody, day: str) -> int:
    """Date every undated period on `day`; return how many were dated."""
    dated = 0
    for measure in regulation.measures or []:
        dated += _date_measure(measure, day)
    return dated


def date_update(regulation: PostApiRegulationsAddBody, read: Mapping[str, Any], day: str) -> int:
    """Date every undated period on what DiaLog holds (`read`, the GET of the regulation
    being replaced) for its measure, else on `day`; return how many were dated."""
    measures = list(regulation.measures or [])
    read_measures = [m for m in read.get("measures") or [] if isinstance(m, Mapping)]
    matched = _match_measures(measures, read_measures)
    dated = 0
    for index, measure in enumerate(measures):
        kept = _earliest_start(matched[index]) if index in matched else None
        dated += _date_measure(measure, kept or day)
    return dated


# --- Measures ----------------------------------------------------------------------


def _is_undated(period: Any) -> bool:
    start = getattr(period, "start_date", None)
    return start is None or isinstance(start, Unset)


def _date_measure(measure: Any, day: str) -> int:
    dated = 0
    for period in _periods(measure):
        if _is_undated(period):
            period.start_date = day
            period.start_time = day
            dated += 1
    return dated


def _match_measures(sent: Sequence[Any], read: list[Mapping[str, Any]]) -> dict[int, Mapping]:
    """Which read measure each sent measure corresponds to, by index of the sent one.

    One measure on both sides is the same measure, whatever its shape: the regulation is the
    unit of identity. A sent measure with no counterpart is new.
    """
    if len(sent) == 1 and len(read) == 1:
        return {0: read[0]}
    remaining = list(read)
    matched: dict[int, Mapping] = {}
    for index, measure in enumerate(sent):
        signature = _sent_signature(measure)
        for position, candidate in enumerate(remaining):
            if _read_signature(candidate) == signature:
                matched[index] = remaining.pop(position)
                break
    return matched


def _sent_signature(measure: Any) -> tuple:
    vehicle_set = measure.vehicle_set
    exempted = getattr(vehicle_set, "exempted_types", None)
    dimensions = {
        name: float(getattr(vehicle_set, attribute))
        for attribute, name in DIMENSION_ATTRIBUTES
        if isinstance(getattr(vehicle_set, attribute, None), (int, float))
    }
    return (
        _plain(measure.type_),
        None if isinstance(measure.max_speed, Unset) else measure.max_speed,
        tuple(sorted(_plain(item) for item in exempted)) if isinstance(exempted, list) else (),
        tuple(sorted(dimensions.items())),
    )


def _read_signature(measure: Mapping[str, Any]) -> tuple:
    vehicle_set = measure.get("vehicleSet")
    vehicle_set = vehicle_set if isinstance(vehicle_set, Mapping) else {}
    exempted = [
        str(item.get("name") if isinstance(item, Mapping) else item)
        for item in vehicle_set.get("exemptedTypes") or []
    ]
    dimensions = {
        str(item.get("name")): float(value)
        for item in vehicle_set.get("maxCharacteristics") or []
        if isinstance(item, Mapping)
        for value in [item.get("value")]
        if isinstance(value, (int, float, str))
    }
    return (
        measure.get("type"),
        measure.get("maxSpeed"),
        tuple(sorted(exempted)),
        tuple(sorted(dimensions.items())),
    )


def _earliest_start(measure: Mapping[str, Any]) -> str | None:
    starts = [
        parsed
        for period in measure.get("periods") or []
        if isinstance(period, Mapping)
        for parsed in [_parse(period.get("startDateTime"))]
        if parsed is not None
    ]
    return _to_paris_iso(min(starts)) if starts else None


def _plain(value: Any) -> str:
    return str(getattr(value, "value", value))


def _periods(measure: Any) -> list[Any]:
    periods = getattr(measure, "periods", None)
    return list(periods) if isinstance(periods, list) else []
