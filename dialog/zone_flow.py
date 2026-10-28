"""Creating a regulation whose locations are zones, in four calls instead of one.

DiaLog turns a zone polygon into the street sections it intersects, slivers of neighbouring
streets and parallel roads included, and the API cannot filter them. So: POST a draft, GET
its computed sections, DELETE it, POST it again with each zone as a `rawGeoJSON` of its long
enough sections. Not a PUT: it answers 500 on any regulation holding a zone.

A zone covering parallel roads is refused. Whatever fails after the draft, the regulation
stays on the platform: as the computed zone, or recreated from the original payload.
"""

import json
from typing import Literal

from loguru import logger

from api.dia_log_client.models import (
    PostApiRegulationsAddBody,
    PostApiRegulationsAddBodyStatus,
    RoadTypeEnum,
)
from dialog.calls import DialogApi
from dialog.zone_sections import sections_of, sections_per_length

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
    """The four calls, with their fallbacks. `regulation` carries the target status."""
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

    `sections` is indexed like the payload: the API returns locations in the order sent. A
    zone with no matching or long enough section is kept; one covering parallel roads raises
    `ZoneCoversParallelRoads`.
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
