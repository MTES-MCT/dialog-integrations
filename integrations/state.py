"""Snapshot of what the pipeline last sent to DiaLog, used to detect updates.

Update detection never re-reads DiaLog: the API rewrites part of what it receives
(`startTime` read back in Europe/Paris, `documentUrl` dropped, `timeSlots` dated to
1970). Comparing against a re-read would report differences that are not ours and
republish the whole corpus every day. We compare against a digest of our own payload.

The snapshot lives in `state/{organization}/{source}.json.gz`; `DIALOG_STATE_DIR`
moves it (in CI it is restored from the Actions cache).

Missing snapshot means *no update at all*, and the snapshot is rebuilt from what we
produced. A lost cache costs one day of updates, never a mass rewrite.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
from collections.abc import Iterator, Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from loguru import logger

from api.dia_log_client.models import PostApiRegulationsAddBody

STATE_DIR_ENV = "DIALOG_STATE_DIR"
DEFAULT_STATE_DIR = "state"
SNAPSHOT_VERSION = 1

# Regulation fields whose change must trigger an update.
DIGESTED_REGULATION_FIELDS = ("title", "category", "subject", "otherCategoryText")
# Mirrors of startDate / endDate rebuilt at send time; keeping them would only add noise.
VOLATILE_PERIOD_FIELDS = ("startTime", "endTime")

# A digest is a plain JSON structure, kept readable so a field-by-field diff can be
# rendered from two snapshots without going back to the source.
Digest = dict[str, Any]


def state_dir(base_dir: Path | str | None = None) -> Path:
    """Directory holding the snapshots, overridable with DIALOG_STATE_DIR."""
    if base_dir is not None:
        return Path(base_dir)
    return Path(os.getenv(STATE_DIR_ENV) or DEFAULT_STATE_DIR)


class SnapshotStore:
    """Read and write the digest snapshot of one data source."""

    def __init__(self, organization: str, source: str, base_dir: Path | str | None = None):
        self.organization = organization
        self.source = source
        self.path = state_dir(base_dir) / organization / f"{source}.json.gz"

    def exists(self) -> bool:
        return self.path.exists()

    def load(self) -> dict[str, Digest] | None:
        """Return the stored digests, or None when there is no usable snapshot.

        An unreadable snapshot is treated as absent: that costs a day of updates,
        whereas guessing its content could cost a mass rewrite.
        """
        if not self.path.exists():
            logger.info(f"No snapshot at {self.path}: no update will be detected this run")
            return None

        try:
            with gzip.open(self.path, "rt", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, ValueError) as e:
            logger.warning(f"Unreadable snapshot {self.path} ({e}), treated as missing")
            return None

        regulations = payload.get("regulations") if isinstance(payload, dict) else None
        if not isinstance(regulations, dict):
            logger.warning(f"Malformed snapshot {self.path}, treated as missing")
            return None

        logger.info(f"Loaded {len(regulations)} digest(s) from {self.path}")
        return regulations

    def save(self, digests: Mapping[str, Digest]) -> None:
        payload = {
            "version": SNAPSHOT_VERSION,
            "organization": self.organization,
            "source": self.source,
            "updatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "regulations": dict(digests),
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # Write then rename: an interrupted run must not leave a truncated snapshot,
        # which would be read as "missing" and skip a day of updates.
        temporary = self.path.with_suffix(".tmp")
        with gzip.open(temporary, "wt", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
        temporary.replace(self.path)
        logger.info(f"Wrote {len(payload['regulations'])} digest(s) to {self.path}")


def compute_regulation_digest(regulation: PostApiRegulationsAddBody) -> Digest:
    """Reduce a regulation payload to what a change must be detected on."""
    payload = regulation.to_dict()
    digest: Digest = {field: payload.get(field) for field in DIGESTED_REGULATION_FIELDS}
    digest["measures"] = [_digest_measure(measure) for measure in payload.get("measures") or []]
    return digest


def fingerprint(digest: Digest) -> str:
    """Stable hash of a digest: canonical JSON, sorted keys."""
    canonical = json.dumps(
        digest, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str
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

    raw_geo_json = location.get("rawGeoJSON")
    if isinstance(raw_geo_json, Mapping):
        digest["rawGeoJSON"] = {
            "label": raw_geo_json.get("label"),
            "geometry": _digest_geometry(raw_geo_json.get("geometry")),
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
