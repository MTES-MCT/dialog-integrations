"""Creating a regulation whose locations are zones, in four calls instead of one.

A `zone` location is a polygon; DiaLog turns it into the BD TOPO street sections it
intersects and publishes those. Two things make that result say more than the producer
did — clipped slivers of neighbouring streets, and polygons wide enough to hold several
roads side by side — and nothing in the API lets us filter what the zone computes
(`shared/zone_sections.py` has the measurements). So the regulation is created in four
calls:

1. POST as a draft, with the zones, so that DiaLog does the polygon → sections
   conversion;
2. GET the regulation back: each location now carries the sections as its effective
   geometry;
3. DELETE the draft;
4. POST the same regulation, with the target status, where each zone became a
   `rawGeoJSON` of its sections at least `min_length_m` long.

Four and not three: `PUT /api/regulations` answers 500 on any regulation holding a zone
(S-14), while it works on the others.

A zone whose sections add up to more than `max_sections_per_length` times the polygon's
length covers several parallel roads; nothing says which one is closed, so the
regulation is **refused**: draft deleted, nothing created. Fallbacks keep the regulation
on the platform whatever else fails after the draft: sections unreadable or draft not
deletable → the draft is published as it is (zone as computed); recreation refused → the
original zone payload is recreated.
"""

import json
from typing import Literal

from loguru import logger

from api.dia_log_client.models import (
    PostApiRegulationsAddBody,
    PostApiRegulationsAddBodyStatus,
    RoadTypeEnum,
)
from integrations.api import DialogApi
from integrations.shared.zone_sections import sections_of, sections_per_length

Outcome = Literal["created", "refused", "failed"]


class ZoneCoversParallelRoads(Exception):
    """The polygon holds several roads side by side: the restriction cannot be placed."""


def has_zone(regulation: PostApiRegulationsAddBody) -> bool:
    return any(
        location.road_type == RoadTypeEnum.ZONE
        for measure in regulation.measures or []
        for location in measure.locations or []
    )


def create_zone_regulation(
    api: DialogApi,
    regulation: PostApiRegulationsAddBody,
    *,
    min_length_m: float,
    max_sections_per_length: float,
) -> Outcome:
    """The four calls, with their fallbacks. `regulation` carries the target status.

    The thresholds are the caller's: `BaseIntegration` holds their defaults, an
    organization may override them, `shared/zone_sections.py` defines them.
    """
    identifier = str(regulation.identifier)
    target_status = regulation.status

    draft = PostApiRegulationsAddBody.from_dict(regulation.to_dict())
    draft.status = PostApiRegulationsAddBodyStatus.DRAFT
    if not api.add(draft):
        return "failed"

    original = regulation.to_dict()
    try:
        sections = computed_sections(api, identifier)
        payload = zones_as_sections(original, sections, min_length_m, max_sections_per_length)
    except ZoneCoversParallelRoads as e:
        logger.warning(f"{identifier}: refused, the zone covers several parallel roads — {e}")
        if not api.delete(identifier):
            logger.error(f"{identifier}: refused but its draft could not be deleted")
        return "refused"
    except Exception as e:
        logger.warning(f"{identifier}: sections unreadable ({e}), publishing the zone as is")
        return _promote(api, identifier, target_status)
    if payload == original:
        return _promote(api, identifier, target_status)

    if not api.delete(identifier):
        logger.warning(f"{identifier}: draft not deleted, publishing the zone as is")
        return _promote(api, identifier, target_status)
    if api.add(PostApiRegulationsAddBody.from_dict(payload)):
        return "created"
    logger.warning(f"{identifier}: sections refused, recreating the zone as computed")
    return "created" if api.add(regulation) else "failed"


def computed_sections(api: DialogApi, identifier: str) -> list[list[str | None]]:
    """The effective geometry of every location, as [measure][location]."""
    content = api.get(identifier)
    if content is None:
        raise RuntimeError("GET failed")
    return [
        [location.get("geometry") for location in measure.get("locations") or []]
        for measure in content.get("measures") or []
    ]


def zones_as_sections(
    payload: dict,
    sections: list[list[str | None]],
    min_length_m: float,
    max_sections_per_length: float,
) -> dict:
    """The regulation payload with every zone replaced by its filtered sections.

    `sections` comes from the GET of the freshly created draft, indexed like the
    payload's measures and locations — the API returns them in the order they were
    sent. A mismatch in shape, or a zone whose sections are all too short, keeps that
    zone untouched. A zone whose sections add up to more than
    `max_sections_per_length` times its own length raises `ZoneCoversParallelRoads`.
    """
    payload = json.loads(json.dumps(payload))  # deep copy
    for m, measure in enumerate(payload.get("measures") or []):
        for n, location in enumerate(measure.get("locations") or []):
            if location.get("roadType") != RoadTypeEnum.ZONE.value:
                continue
            try:
                computed = sections[m][n]
            except IndexError:
                logger.warning(f"Measure {m} location {n}: no computed geometry, zone kept")
                continue
            zone = location.get("zone") or {}
            ratio = sections_per_length(zone.get("geometry"), computed, min_length_m)
            if ratio > max_sections_per_length:
                raise ZoneCoversParallelRoads(
                    f"{zone.get('label')}: {ratio:.1f} road lengths of sections for one "
                    f"polygon length (limit {max_sections_per_length:g})"
                )
            kept = sections_of(computed, min_length_m)
            if kept is None:
                logger.warning(
                    f"Measure {m} location {n}: no section ≥ {min_length_m} m, zone kept"
                )
                continue
            measure["locations"][n] = {
                "roadType": RoadTypeEnum.RAWGEOJSON.value,
                "rawGeoJSON": {"label": zone.get("label"), "geometry": kept},
            }
    return payload


def _promote(api: DialogApi, identifier: str, target_status) -> Outcome:
    """Bring the draft to the target status; nothing to do when the target is draft."""
    if target_status != PostApiRegulationsAddBodyStatus.PUBLISHED:
        return "created"
    return "created" if api.publish(identifier) else "failed"
