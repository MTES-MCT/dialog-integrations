"""From the commands the PHP import would handle to the API calls that store the same
rows: one POST of the regulation, then one POST of its document URL.

The back end does not treat both routes alike. Two compensations keep the stored rows
identical to what the PHP import stores:

- time slots: the import stores the UTC clock it computed (Paris time on the day of
  the run). The API keeps only the wall clock of what it receives, reads it in
  Europe/Paris on 1970-01-01 (UTC+1, whatever the season), and stores that in UTC. We
  therefore send the stored UTC clock as an instant of 1970-01-01, written in UTC+1:
  the API reads back exactly that clock minus one hour. This relies on the API's
  time slot conversion as of 2026-09-08 (back end PR #2049);
- the document URL: the import validates it with the regulation, the API in a second
  call. `storage_verdict` judges it beforehand, as the import's validator would.

What the API cannot reproduce is listed in the module docstring of `executor.py`.
"""

from datetime import datetime, timedelta, timezone

from api.dia_log_client.models import (
    MeasureTypeEnum,
    PeriodRecurrenceTypeEnum,
    PostApiRegulationsAddBody,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodyStatus,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
    SaveLocationDTO,
    SaveMeasureDTO,
    SavePeriodDTO,
    SaveRawGeoJSONDTO,
    SaveTimeSlotDTO,
    SaveVehicleSetDTO,
)
from integrations.dialog.litteralis.commands import ImportCommand, Measure, Period, VehicleSet
from integrations.dialog.litteralis.url_validator import is_valid_https_url

# The title the PHP import gives every document it attaches.
STORAGE_TITLE = "dl.sogelink.fr"

# Europe/Paris on 1970-01-01, the day the API anchors a time slot's clock on.
API_TIME_SLOT_ZONE = timezone(timedelta(hours=1))


def regulation_body(
    command: ImportCommand, status: PostApiRegulationsAddBodyStatus
) -> PostApiRegulationsAddBody:
    info = command.general_info
    return PostApiRegulationsAddBody(
        identifier=info.identifier,
        category=PostApiRegulationsAddBodyCategory(info.category),
        status=status,
        subject=PostApiRegulationsAddBodySubject(info.subject),
        title=info.title,
        other_category_text=info.other_category_text,
        measures=[_measure(measure) for measure in command.measures],  # type: ignore[arg-type]
    )


def _measure(measure: Measure) -> SaveMeasureDTO:
    params: dict = {
        "type_": MeasureTypeEnum(measure.type),
        "periods": [_period(period) for period in measure.periods],
        "locations": [
            SaveLocationDTO(
                road_type=RoadTypeEnum(location.road_type),
                raw_geo_json=SaveRawGeoJSONDTO(label=location.label, geometry=location.geometry),
            )
            for location in measure.locations
        ],
        "vehicle_set": _vehicle_set(measure.vehicle_set or VehicleSet()),
    }
    if measure.max_speed is not None:
        params["max_speed"] = measure.max_speed
    return SaveMeasureDTO(**params)


def _period(period: Period) -> SavePeriodDTO:
    return SavePeriodDTO(
        start_date=_instant(period.start_date),
        start_time=_instant(period.start_time),
        end_date=_instant(period.end_date),
        end_time=_instant(period.end_time),
        recurrence_type=PeriodRecurrenceTypeEnum(period.recurrence_type),
        is_permanent=period.is_permanent,
        time_slots=[
            SaveTimeSlotDTO(
                start_time=api_time_slot_clock(slot.start_time),
                end_time=api_time_slot_clock(slot.end_time),
            )
            for slot in period.time_slots
        ],
    )


def _instant(moment: datetime | None) -> str | None:
    """The instant with the offset PHP held, which the API parses back unchanged."""
    return moment.isoformat() if moment is not None else None


def api_time_slot_clock(stored: datetime) -> str:
    """What to send for the API to store the UTC clock of `stored`.

    `stored` carries the clock the PHP import stores (its UTC time of day). The API
    stores the wall clock it receives minus one hour, hence `1970-01-01T07:00:00+01:00`
    for a slot the import stores at 06:00.
    """
    utc_clock = stored.astimezone(timezone.utc)
    anchored = datetime(1970, 1, 1, utc_clock.hour, utc_clock.minute, tzinfo=timezone.utc)
    return anchored.astimezone(API_TIME_SLOT_ZONE).isoformat()


def _vehicle_set(vehicle_set: VehicleSet) -> SaveVehicleSetDTO:
    """Every field the import sets; empty ones are left to the API's identical defaults."""
    fields = {
        "all_vehicles": vehicle_set.all_vehicles,
        "restricted_types": vehicle_set.restricted_types,
        "other_restricted_type_text": vehicle_set.other_restricted_type_text,
        "heavyweight_max_weight": vehicle_set.heavyweight_max_weight,
        "exempted_types": vehicle_set.exempted_types,
        "other_exempted_type_text": vehicle_set.other_exempted_type_text,
    }
    return SaveVehicleSetDTO(**{k: v for k, v in fields.items() if v not in (None, [])})


def storage_verdict(command: ImportCommand) -> str:
    """What the PHP import would do with the regulation's document URL.

    - `none`: no `shorturl`, no document;
    - `attach`: a valid URL, attached after the regulation;
    - `empty`: `""` passes the import's validation and is stored as such, but the API
      refuses an empty URL: the regulation is created without its (empty) document;
    - `invalid`: not an https URL with a TLD; in PHP the whole regulation fails.
    """
    url = command.download_url
    if url is None:
        return "none"
    if url == "":
        return "empty"
    return "attach" if is_valid_https_url(url) else "invalid"
