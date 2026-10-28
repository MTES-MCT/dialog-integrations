"""A canonical row's columns mapped to the API DTOs, by prefix."""

from api.dia_log_client.models import (
    MeasureTypeEnum,
    RoadTypeEnum,
    SaveLocationDTO,
    SaveMeasureDTO,
    SaveNamedStreetDTO,
    SaveNumberedRoadDTO,
    SavePeriodDTO,
    SaveRawGeoJSONDTO,
    SaveTimeSlotDTO,
    SaveVehicleSetDTO,
    SaveZoneDTO,
)
from canonical.regulation_measure import RegulationMeasure


def build_measure(
    measure: RegulationMeasure, locations: list[RegulationMeasure] | None = None
) -> SaveMeasureDTO:
    """One measure, located by the `locations` rows if given, else by `measure` itself."""
    location_rows = locations if locations is not None else [measure]
    params = {
        "type_": MeasureTypeEnum(measure["measure_type_"]),
        "periods": [build_period(measure)],
        "locations": [build_location(row) for row in location_rows],
        "vehicle_set": build_vehicle_set(measure),
    }

    if measure["measure_type_"] == MeasureTypeEnum.SPEEDLIMITATION.value:
        if measure.get("measure_max_speed"):
            params["max_speed"] = int(measure["measure_max_speed"])  # type: ignore

    return SaveMeasureDTO(**params)


def build_period(measure: RegulationMeasure) -> SavePeriodDTO:
    """The `period_*` columns, prefix stripped.

    The API takes the day from `startDate` and the clock, read in Europe/Paris, from
    `startTime`: both are given the same value. No time slot means around the clock.
    """
    period_fields = _fields_with_prefix(measure, "period_")
    period_fields["start_time"] = period_fields.get("start_date")
    period_fields["end_time"] = period_fields.get("end_date")

    time_slots = period_fields.pop("time_slots", None) or []
    period_fields["time_slots"] = [
        SaveTimeSlotDTO(start_time=slot["start_time"], end_time=slot["end_time"])
        for slot in time_slots
    ]
    return SavePeriodDTO(**period_fields)


def build_location(measure: RegulationMeasure) -> SaveLocationDTO:
    """The `location_*` columns, wrapped in the DTO matching `location_road_type`."""
    road_type = RoadTypeEnum(measure["location_road_type"])
    location_fields = _fields_with_prefix(measure, "location_")
    location_fields.pop("road_type", None)

    if road_type == RoadTypeEnum.RAWGEOJSON:
        return SaveLocationDTO(
            road_type=road_type,
            raw_geo_json=SaveRawGeoJSONDTO(**location_fields),
        )
    if road_type == RoadTypeEnum.ZONE:
        # DiaLog computes the street sections the polygon covers.
        return SaveLocationDTO(road_type=road_type, zone=SaveZoneDTO(**location_fields))
    if road_type in (RoadTypeEnum.DEPARTMENTALROAD, RoadTypeEnum.NATIONALROAD):
        numbered = SaveNumberedRoadDTO(**location_fields)
        if road_type == RoadTypeEnum.NATIONALROAD:
            return SaveLocationDTO(road_type=road_type, national_road=numbered)
        return SaveLocationDTO(road_type=road_type, departmental_road=numbered)
    if road_type == RoadTypeEnum.LANE:
        # No geometry is sent: DiaLog geocodes the street and its bounds.
        return SaveLocationDTO(
            road_type=road_type, named_street=SaveNamedStreetDTO(**location_fields)
        )
    raise Exception(f"Location saving not implemented for  RoadType {road_type.value}")


def build_vehicle_set(measure: RegulationMeasure) -> SaveVehicleSetDTO:
    """The `vehicle_*` columns, without the empty ones.

    `all_vehicles=True` alone stays alone: the API reads any other field as a
    restriction, so an empty list must not be sent next to it.
    """
    vehicle_fields = _fields_with_prefix(measure, "vehicle_")
    cleaned = {k: v for k, v in vehicle_fields.items() if v not in (None, [], {})}

    if cleaned.get("all_vehicles") is True and len(cleaned) == 1:
        return SaveVehicleSetDTO(all_vehicles=True)
    return SaveVehicleSetDTO(**cleaned)


def _fields_with_prefix(measure: RegulationMeasure, prefix: str) -> dict:
    return {
        key.replace(prefix, "", 1): value
        for key, value in measure.items()
        if key.startswith(prefix)
    }
