"""Date the undated permanent periods at the moment of writing (R-39).

A permanent regulation whose source records no commencement date leaves `startDate`
null all the way through the pivot, the payload, the digest and the snapshot: "in
force, since a day nobody recorded". Null on both sides of the comparison, the date can
never register as a change — dated with the day of the run, every such regulation
looked modified each morning.

The null is resolved only when DiaLog is written:

- a **creation** is dated the day of the run, at French midnight: "in force when we
  published it";
- an **update** keeps the date DiaLog already holds for that measure, read back through
  `GET /api/regulations/{identifier}`. A measure DiaLog does not hold is new and takes
  the day of the run.

A regulation is matched to its read counterpart measure by measure: directly when both
hold a single one (Lyon's inventory, most of Aveyron), by signature otherwise — type,
speed, exemptions and dimension limits. The written and read forms of a vehicle set
differ (`ai/docs/api-dialog.md`, piège 5: the restricted types come back empty and the
dimensions move to `maxCharacteristics`), so the restricted types stay out of it.

Only the periods left undated are touched. A period the source dated keeps its date,
and a temporary period is never undated.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, time
from typing import Any
from zoneinfo import ZoneInfo

from api.dia_log_client.models import PostApiRegulationsAddBody
from api.dia_log_client.types import Unset
from integrations.sync.closure import _parse, _to_paris_iso

PARIS = ZoneInfo("Europe/Paris")

# Dimension limits as the save DTO names them → as `maxCharacteristics` names them.
DIMENSION_ATTRIBUTES = (
    ("heavyweight_max_weight", "weight"),
    ("max_height", "height"),
    ("max_width", "width"),
    ("max_length", "length"),
)


def run_day(now: datetime | None = None) -> str:
    """French midnight of the day, carrying the real offset of that day.

    `2026-09-19T00:00:00+02:00`: the one shape DiaLog reads without shifting it
    (`integrations/shared/local_time.py`).
    """
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
    """Date every undated period on what DiaLog holds for its measure, else on `day`.

    `read` is the `GET /api/regulations/{identifier}` response of the regulation being
    replaced. Returns how many periods were dated.
    """
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

    One measure on both sides is the same measure, whatever its shape: the regulation
    is the unit of identity, not the measure. Otherwise a sent measure takes the first
    read measure of the same signature not taken yet; one with no counterpart is new.
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
