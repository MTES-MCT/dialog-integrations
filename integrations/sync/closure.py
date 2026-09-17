"""Close a regulation that left its source, instead of deleting it.

Some sources are snapshots of what is live: a work site that ends, or is withdrawn,
simply disappears. Deleting the regulation would erase the history of that site;
closing it keeps the regulation in DiaLog and only brings its end date back to the
last day the source still listed it — the day before the run.

The source no longer has the row, so the regulation is rebuilt from what DiaLog
returns on `GET /api/regulations/{identifier}` and sent back through
`PUT /api/regulations`. The read format is not the write format: dates come back in
UTC, time slots dated 1970, vehicle types as objects, and `allVehicles` is not
returned at all. `save_payload_from_read` does that translation and refuses what it
cannot translate faithfully, rather than sending a regulation that lost a detail.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

PARIS = ZoneInfo("Europe/Paris")

ROAD_TYPE_RAW_GEOJSON = "rawGeoJSON"
ROAD_TYPE_ZONE = "zone"
NUMBERED_ROAD_KEYS = {"departmentalRoad": "departmentalRoad", "nationalRoad": "nationalRoad"}


class ClosureNotRebuildable(Exception):
    """The read regulation holds something the write format cannot carry back."""


def closing_instant(now: datetime | None = None) -> datetime:
    """The end date given to a closed regulation: the end of yesterday, Europe/Paris.

    The previous run saw the regulation in the source, today's run does not; the last
    moment it is known to have applied is the end of the previous day.
    """
    now = (now or datetime.now(tz=PARIS)).astimezone(PARIS)
    yesterday = now.date() - timedelta(days=1)
    return datetime.combine(yesterday, time(23, 59, 59), tzinfo=PARIS)


def save_payload_from_read(read: Mapping[str, Any]) -> dict:
    """Rebuild a POST/PUT body from a `GET /api/regulations/{identifier}` response."""
    return {
        "identifier": read.get("identifier"),
        "status": read.get("status"),
        "category": read.get("category"),
        "subject": read.get("subject"),
        "otherCategoryText": read.get("otherCategoryText"),
        "title": read.get("title"),
        "measures": [_measure_from_read(measure) for measure in read.get("measures") or []],
    }


def close_payload(payload: Mapping[str, Any], closed_at: datetime) -> tuple[dict, bool]:
    """A copy of the payload with every period ending at `closed_at` at the latest.

    A permanent period becomes a temporary one ending at `closed_at`. A period that had
    not started yet ends when it starts: the site was cancelled, the regulation keeps
    its dates and applies for no time at all. Returns the copy and whether anything
    moved; nothing moving means the regulation had already ended on its own.
    """
    closed = json.loads(json.dumps(payload))
    changed = False
    end_text = _to_paris_iso(closed_at)
    for measure in closed.get("measures") or []:
        for period in measure.get("periods") or []:
            start = _parse(period.get("startDate"))
            end = None if period.get("isPermanent") else _parse(period.get("endDate"))
            new_end = closed_at if start is None else max(closed_at, start)
            if end is not None and end <= new_end:
                continue
            period["isPermanent"] = False
            period["endDate"] = end_text if new_end == closed_at else _to_paris_iso(new_end)
            period["endTime"] = period["endDate"]
            changed = True
    return closed, changed


def is_ended(digest: Mapping[str, Any], closed_at: datetime) -> bool:
    """True when every period of a snapshot digest ends at or before `closed_at`.

    Used to skip, without a single API call, the regulations that already expired on
    their own. Unknown shapes are reported as not ended: the closure then reads the
    regulation and decides on what DiaLog actually holds.
    """
    periods = [
        period
        for measure in digest.get("measures") or []
        if isinstance(measure, Mapping)
        for period in measure.get("periods") or []
        if isinstance(period, Mapping)
    ]
    if not periods:
        return False
    for period in periods:
        if period.get("isPermanent"):
            return False
        end = _parse(period.get("endDate"))
        if end is None or end > closed_at:
            return False
    return True


def _measure_from_read(measure: Mapping[str, Any]) -> dict:
    return {
        "type": measure.get("type"),
        "maxSpeed": measure.get("maxSpeed"),
        "vehicleSet": _vehicle_set_from_read(measure.get("vehicleSet")),
        "periods": [_period_from_read(period) for period in measure.get("periods") or []],
        "locations": [_location_from_read(location) for location in measure.get("locations") or []],
    }


def _vehicle_set_from_read(vehicle_set: Any) -> dict:
    """`{restrictedTypes: [{name}], exemptedTypes: [{name}], maxCharacteristics: []}`
    back to the save DTO. Dimensions and "other" types are not translated: their read
    format has not been observed, and guessing would silently drop a restriction."""
    if not isinstance(vehicle_set, Mapping):
        return {"allVehicles": True}
    if vehicle_set.get("maxCharacteristics"):
        raise ClosureNotRebuildable("vehicle dimensions (maxCharacteristics) cannot be rebuilt")
    restricted = _type_names(vehicle_set.get("restrictedTypes"))
    exempted = _type_names(vehicle_set.get("exemptedTypes"))
    if "other" in restricted or "other" in exempted:
        raise ClosureNotRebuildable("an 'other' vehicle type cannot be rebuilt")

    rebuilt: dict[str, Any] = {"allVehicles": not restricted}
    if restricted:
        rebuilt["restrictedTypes"] = restricted
    if exempted:
        rebuilt["exemptedTypes"] = exempted
    if vehicle_set.get("critairTypes"):
        rebuilt["critairTypes"] = _type_names(vehicle_set.get("critairTypes"))
    return rebuilt


def _type_names(items: Any) -> list[str]:
    names: list[str] = []
    for item in items or []:
        name = item.get("name") if isinstance(item, Mapping) else item
        if name is None:
            raise ClosureNotRebuildable(f"unreadable vehicle type {item!r}")
        names.append(str(name))
    return names


def _period_from_read(period: Mapping[str, Any]) -> dict:
    """UTC instants back to Europe/Paris ISO strings, which is how the pipeline writes
    them; the time slots go back as read (the API returns them as it stores them)."""
    start = _parse(period.get("startDateTime"))
    end = _parse(period.get("endDateTime"))
    rebuilt: dict[str, Any] = {
        "startDate": _to_paris_iso(start) if start else None,
        "startTime": _to_paris_iso(start) if start else None,
        "endDate": _to_paris_iso(end) if end else None,
        "endTime": _to_paris_iso(end) if end else None,
        "recurrenceType": period.get("recurrenceType"),
        "isPermanent": end is None,
        "timeSlots": [
            {"startTime": slot.get("startTime"), "endTime": slot.get("endTime")}
            for slot in period.get("timeSlots") or []
            if isinstance(slot, Mapping)
        ],
    }
    if period.get("dailyRange") is not None:
        rebuilt["dailyRange"] = period["dailyRange"]
    return rebuilt


def _location_from_read(location: Mapping[str, Any]) -> dict:
    """Locations keep their type, except a `zone`: DiaLog already turned it into
    street sections, and `PUT` answers 500 on a zone (S-14), so the sections are sent
    as `rawGeoJSON` — the effective geometry does not change."""
    road_type = location.get("roadType")
    geometry = location.get("geometry")
    if road_type == ROAD_TYPE_RAW_GEOJSON:
        label = (location.get("rawGeoJSON") or {}).get("label")
        return {"roadType": road_type, "rawGeoJSON": {"label": label, "geometry": geometry}}
    if road_type == ROAD_TYPE_ZONE:
        label = (location.get("zone") or {}).get("label")
        if not geometry:
            raise ClosureNotRebuildable("a zone without computed geometry cannot be rebuilt")
        return {
            "roadType": ROAD_TYPE_RAW_GEOJSON,
            "rawGeoJSON": {"label": label, "geometry": geometry},
        }
    if road_type == "namedStreet" and isinstance(location.get("namedStreet"), Mapping):
        return {"roadType": road_type, "namedStreet": dict(location["namedStreet"])}
    if road_type in NUMBERED_ROAD_KEYS and isinstance(location.get("numberedRoad"), Mapping):
        return {
            "roadType": road_type,
            NUMBERED_ROAD_KEYS[road_type]: dict(location["numberedRoad"]),
        }
    raise ClosureNotRebuildable(f"location of type {road_type!r} cannot be rebuilt")


def _parse(text: Any) -> datetime | None:
    if not isinstance(text, str) or not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=PARIS)
    return parsed


def _to_paris_iso(instant: datetime) -> str:
    return instant.astimezone(PARIS).isoformat(timespec="seconds")
