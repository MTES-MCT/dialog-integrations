"""The digest of a regulation payload: what one run compares with the next."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator, Mapping
from typing import Any

from api.dia_log_client.models import PostApiRegulationsAddBody

# Regulation fields whose change must trigger an update.
DIGESTED_REGULATION_FIELDS = ("title", "category", "subject", "otherCategoryText")


# Mirrors of startDate / endDate rebuilt at send time; keeping them would only add noise.
VOLATILE_PERIOD_FIELDS = ("startTime", "endTime")


# A digest is a plain JSON structure, kept readable so a field-by-field diff can be
# rendered from two snapshots without going back to the source.
Digest = dict[str, Any]


def _without_empty_lists(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {k: _without_empty_lists(v) for k, v in value.items() if v != []}
    if isinstance(value, list):
        return [_without_empty_lists(v) for v in value]
    return value


def compute_regulation_digest(regulation: PostApiRegulationsAddBody) -> Digest:
    """Reduce a regulation payload to what a change must be detected on."""
    payload = regulation.to_dict()
    digest: Digest = {field: payload.get(field) for field in DIGESTED_REGULATION_FIELDS}
    digest["measures"] = [_digest_measure(measure) for measure in payload.get("measures") or []]
    return digest


def fingerprint(digest: Digest) -> str:
    """Stable hash of a digest: canonical JSON, sorted keys, empty lists dropped.

    An empty list says the same thing as an absent key (`timeSlots: []` is "around the
    clock"). Dropping them here, on the stored digest as well as on today's, keeps a
    snapshot built before a key existed — or before it was left out — from reading as a
    change of every regulation.
    """
    canonical = json.dumps(
        _without_empty_lists(digest),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _digest_measure(measure: Any) -> Digest:
    if not isinstance(measure, Mapping):
        return {"unexpected": str(measure)}
    return {
        "type": measure.get("type"),
        "maxSpeed": measure.get("maxSpeed"),
        "periods": [_digest_period(period) for period in measure.get("periods") or []],
        "vehicleSet": _digest_vehicle_set(measure.get("vehicleSet")),
        "locations": [_digest_location(location) for location in measure.get("locations") or []],
    }


def _digest_period(period: Any) -> Digest:
    if not isinstance(period, Mapping):
        return {"unexpected": str(period)}
    return {key: value for key, value in period.items() if key not in VOLATILE_PERIOD_FIELDS}


def _digest_vehicle_set(vehicle_set: Any) -> Digest | None:
    if not isinstance(vehicle_set, Mapping):
        return None
    # Lists are sorted: the order the source happens to produce is not a change.
    return {
        key: sorted(value, key=str) if isinstance(value, list) else value
        for key, value in vehicle_set.items()
    }


def _digest_location(location: Any) -> Digest:
    if not isinstance(location, Mapping):
        return {"unexpected": str(location)}

    digest: Digest = {"roadType": location.get("roadType")}
    for key in ("namedStreet", "departmentalRoad", "nationalRoad"):
        value = location.get(key)
        if isinstance(value, Mapping):
            digest[key] = dict(value)

    # Both carry a label and a GeoJSON geometry; a zone is digested on the polygon we
    # send, not on the sections DiaLog derives from it.
    for key in ("rawGeoJSON", "zone"):
        drawn = location.get(key)
        if isinstance(drawn, Mapping):
            digest[key] = {
                "label": drawn.get("label"),
                "geometry": _digest_geometry(drawn.get("geometry")),
            }
    return digest


def _digest_geometry(geometry: Any) -> Digest | None:
    """Reduce a geometry to type, point count, bounding box and hash.

    Storing full geometries would make the snapshot unreadable and huge; these four
    values move as soon as the geometry does.
    """
    if geometry is None:
        return None

    parsed: Any = geometry
    if isinstance(geometry, str):
        try:
            parsed = json.loads(geometry)
        except ValueError:
            return {"hash": _hash_text(geometry)}

    if not isinstance(parsed, Mapping):
        return {"hash": _hash_text(str(parsed))}

    canonical = json.dumps(parsed, sort_keys=True, separators=(",", ":"), default=str)
    digest: Digest = {
        "type": parsed.get("type"),
        "hash": _hash_text(canonical),
    }
    positions = list(_iter_positions(parsed.get("coordinates")))
    digest["points"] = len(positions)
    if positions:
        longitudes = [position[0] for position in positions]
        latitudes = [position[1] for position in positions]
        digest["bbox"] = [
            round(min(longitudes), 6),
            round(min(latitudes), 6),
            round(max(longitudes), 6),
            round(max(latitudes), 6),
        ]
    return digest


def _iter_positions(coordinates: Any) -> Iterator[list[float]]:
    """Yield every [lon, lat] position of an arbitrarily nested coordinate array."""
    if not isinstance(coordinates, list) or not coordinates:
        return
    first = coordinates[0]
    if isinstance(first, (int, float)) and len(coordinates) >= 2:
        second = coordinates[1]
        if isinstance(second, (int, float)):
            yield [float(first), float(second)]
        return
    for item in coordinates:
        yield from _iter_positions(item)


def _hash_text(text: str) -> str:
    # 16 hex characters: enough to separate geometries, short enough to stay readable.
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]
