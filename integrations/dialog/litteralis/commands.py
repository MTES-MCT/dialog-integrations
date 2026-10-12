"""What the PHP transformer builds, one dataclass per Symfony command it fills.

Fields keep the defaults of the PHP commands, so that a value the transformer never
sets reads the same on both sides. `oracle_dump` writes them in the layout the PHP
oracle uses, for a field-by-field comparison.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

SOURCE_LITTERALIS = "litteralis"
CAN_USE_RAW_GEOJSON = "canUseRawGeoJSON"


@dataclass
class TimeSlot:
    start_time: datetime
    end_time: datetime


@dataclass
class Period:
    start_date: datetime | None = None
    start_time: datetime | None = None
    end_date: datetime | None = None
    end_time: datetime | None = None
    is_permanent: bool = False
    recurrence_type: str | None = None
    time_slots: list[TimeSlot] = field(default_factory=list)


@dataclass
class VehicleSet:
    all_vehicles: bool | None = None
    restricted_types: list[str] = field(default_factory=list)
    other_restricted_type_text: str | None = None
    heavyweight_max_weight: float | None = None
    exempted_types: list[str] = field(default_factory=list)
    other_exempted_type_text: str | None = None


@dataclass
class RawGeoJSONLocation:
    label: str
    geometry: str
    road_type: str = "rawGeoJSON"


@dataclass
class Measure:
    type: str
    max_speed: int | None = None
    permissions: list[str] = field(default_factory=list)
    vehicle_set: VehicleSet | None = None
    periods: list[Period] = field(default_factory=list)
    locations: list[RawGeoJSONLocation] = field(default_factory=list)


@dataclass
class GeneralInfo:
    identifier: str
    category: str
    subject: str
    title: str
    other_category_text: str | None = None
    source: str = SOURCE_LITTERALIS


@dataclass
class ImportCommand:
    general_info: GeneralInfo
    measures: list[Measure]
    download_url: str | None


def dump_date(moment: datetime | None) -> str | None:
    """PHP `format('Y-m-d\\TH:i:s.uP')`."""
    if moment is None:
        return None
    offset = moment.strftime("%z")
    # `%Y` does not pad years below 1000; PHP's `Y` writes at least four digits.
    return (
        f"{moment.year:04d}" + moment.strftime("-%m-%dT%H:%M:%S.%f") + f"{offset[:3]}:{offset[3:]}"
    )


def oracle_dump(command: ImportCommand | None) -> dict[str, Any] | None:
    """The command in the layout of the PHP oracle's `dumpCommand`."""
    if command is None:
        return None
    info = command.general_info
    return {
        "generalInfo": {
            "identifier": info.identifier,
            "category": info.category,
            "subject": info.subject,
            "otherCategoryText": info.other_category_text,
            "title": info.title,
            "source": info.source,
        },
        "measures": [_dump_measure(measure) for measure in command.measures],
        "downloadUrl": command.download_url,
    }


def _dump_measure(measure: Measure) -> dict[str, Any]:
    vehicle_set = measure.vehicle_set or VehicleSet()
    return {
        "type": measure.type,
        "maxSpeed": measure.max_speed,
        "createdAt": None,
        "permissions": list(measure.permissions),
        "vehicleSet": {
            "allVehicles": vehicle_set.all_vehicles,
            "restrictedTypes": list(vehicle_set.restricted_types),
            "otherRestrictedTypeText": vehicle_set.other_restricted_type_text,
            "heavyweightMaxWeight": vehicle_set.heavyweight_max_weight,
            "exemptedTypes": list(vehicle_set.exempted_types),
            "otherExemptedTypeText": vehicle_set.other_exempted_type_text,
            "critairTypes": [],
            "maxWidth": None,
            "maxLength": None,
            "maxHeight": None,
        },
        "periods": [
            {
                "startDate": dump_date(period.start_date),
                "startTime": dump_date(period.start_time),
                "endDate": dump_date(period.end_date),
                "endTime": dump_date(period.end_time),
                "isPermanent": period.is_permanent,
                "recurrenceType": period.recurrence_type,
                "dailyRange": None,
                "timeSlots": [
                    {"startTime": dump_date(slot.start_time), "endTime": dump_date(slot.end_time)}
                    for slot in period.time_slots
                ],
            }
            for period in measure.periods
        ],
        "locations": [
            {
                "roadType": location.road_type,
                "rawGeoJSON": {"label": location.label, "geometry": location.geometry},
            }
            for location in measure.locations
        ],
    }
