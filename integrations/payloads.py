"""From pivot rows (`RegulationMeasure`) to the API payloads.

Pure functions: no network, no organization. A row's columns are mapped to the DTOs by
prefix — `period_*`, `location_*`, `vehicle_*`, with the prefix stripped — and rows are
grouped by `regulation_identifier` into one regulation carrying one measure per row.

Two opt-ins, set by the data source, change that last step: grouping the rows that
share a `measure_group_key` into one measure carrying all their locations, and cutting
a regulation with too many locations for a single POST into numbered slices.
"""

from collections.abc import Callable

import polars as pl
from loguru import logger

from api.dia_log_client.models import (
    MeasureTypeEnum,
    PostApiRegulationsAddBody,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodyStatus,
    PostApiRegulationsAddBodySubject,
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
from integrations.base_data_source_integration import RegulationMeasure

# `build_measure(row)` or `build_measure(row, locations)`: see `build_measure`.
MeasureBuilder = Callable[..., SaveMeasureDTO]


def build_regulations(
    clean_data: pl.DataFrame,
    status: PostApiRegulationsAddBodyStatus,
    build_measure: MeasureBuilder,
    *,
    group_locations_by_measure: bool = False,
    max_locations_per_regulation: int | None = None,
) -> list[PostApiRegulationsAddBody]:
    """Group the rows by `regulation_identifier`, one measure per row.

    The regulation fields are read on the first row of the group: every row of a
    regulation carries the same values. A row whose measure cannot be built is logged
    and skipped; a regulation left without any measure is skipped as a whole.

    Two opt-ins, driven by the data source (see `BaseDataSourceIntegration`):

    - `group_locations_by_measure`: rows sharing a `measure_group_key` collapse into a
      single measure carrying all of their locations, instead of one measure per row;
    - `max_locations_per_regulation`: a regulation carrying more rows than the ceiling
      is cut into `IDENTIFIER-01`, `IDENTIFIER-02`… slices, each its own regulation.
    """
    regulations = []

    for _, group_df in clean_data.group_by("regulation_identifier"):
        slices = _split_regulation_rows(group_df, max_locations_per_regulation)

        for index, slice_df in enumerate(slices):
            measures = _build_measures(slice_df, build_measure, group_locations_by_measure)
            if not measures:
                continue

            first_row = slice_df.row(0, named=True)
            identifier = first_row["regulation_identifier"]
            if len(slices) > 1:
                identifier = f"{identifier}-{index + 1:02d}"

            regulations.append(_build_regulation(first_row, identifier, status, measures))

    return regulations


def _build_measures(
    slice_df: pl.DataFrame, build_measure: MeasureBuilder, group_by_measure: bool
) -> list[SaveMeasureDTO]:
    """The measures of one regulation (or one slice of it).

    Without grouping, every row is its own measure with its own location: the
    historical behaviour of every source in production. With grouping, the rows
    sharing a `measure_group_key` become one measure carrying all their locations.
    """
    measures = []

    if not group_by_measure:
        for row in slice_df.iter_rows(named=True):
            try:
                measures.append(build_measure(row))
            except Exception as e:
                logger.error(f"Error creating measure: {e}")
        return measures

    for _, measure_df in slice_df.group_by("measure_group_key", maintain_order=True):
        rows = list(measure_df.iter_rows(named=True))
        try:
            measures.append(build_measure(rows[0], rows))
        except Exception as e:
            logger.error(f"Error creating measure: {e}")
    return measures


def _split_regulation_rows(group_df: pl.DataFrame, max_locations: int | None) -> list[pl.DataFrame]:
    """Cut a regulation into slices small enough for a single POST.

    The API is not bounded by payload size but by how long it takes to persist the
    locations: past roughly 1 700 of them the request dies on a server-side timeout,
    and splitting the same total across several measures does not help, the ceiling
    is per regulation. Slices are cut along `regulation_split_order`, so a source that
    ranks its rows geographically gets spatially coherent slices.
    """
    if not max_locations or group_df.height <= max_locations:
        return [group_df]

    if "regulation_split_order" in group_df.columns:
        group_df = group_df.sort("regulation_split_order", nulls_last=True)

    slices = [
        group_df.slice(offset, max_locations) for offset in range(0, group_df.height, max_locations)
    ]
    logger.info(
        f"Regulation {group_df.row(0, named=True)['regulation_identifier']} carries "
        f"{group_df.height} locations, above the {max_locations} ceiling: "
        f"splitting into {len(slices)} regulations"
    )
    return slices


def _build_regulation(
    first_row: dict,
    identifier: str,
    status: PostApiRegulationsAddBodyStatus,
    measures: list[SaveMeasureDTO],
) -> PostApiRegulationsAddBody:
    regulation = PostApiRegulationsAddBody(
        identifier=identifier,
        category=PostApiRegulationsAddBodyCategory(first_row["regulation_category"]),
        status=PostApiRegulationsAddBodyStatus(status),
        subject=PostApiRegulationsAddBodySubject(first_row["regulation_subject"]),
        title=first_row["regulation_title"],
        other_category_text=first_row.get("regulation_other_category_text"),
        measures=measures,  # type: ignore
    )

    # The document URL is not a field of the generated model; it goes as an extra.
    if first_row.get("regulation_document_url"):
        regulation.additional_properties["documentUrl"] = first_row["regulation_document_url"]

    return regulation


def build_measure(
    measure: RegulationMeasure, locations: list[RegulationMeasure] | None = None
) -> SaveMeasureDTO:
    """One measure: its type, one period, one vehicle set, and its locations.

    `measure` carries the measure, period and vehicle fields. `locations` optionally
    carries the rows whose locations belong to this measure: one measure covering N
    road segments, which is what a SIG-sourced regulation looks like. When omitted,
    the measure gets the single location held by `measure` itself.
    """
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

    `startTime` and `endTime` are mirrored from the dates. The API splits a single
    instant across two fields: it takes the day from `startDate` and the clock from
    `startTime`, and reads that clock in Europe/Paris.

    `period_time_slots` is the one period field that is not a scalar: it carries the
    daily slots, which the API expects as `SaveTimeSlotDTO` objects rather than plain
    mappings. Absent or null, the measure applies around the clock.
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
        # A polygon drawn by the producer; DiaLog computes the street sections it
        # covers. Same fields as rawGeoJSON: a label and a GeoJSON geometry.
        return SaveLocationDTO(road_type=road_type, zone=SaveZoneDTO(**location_fields))
    if road_type in (RoadTypeEnum.DEPARTMENTALROAD, RoadTypeEnum.NATIONALROAD):
        numbered = SaveNumberedRoadDTO(**location_fields)
        if road_type == RoadTypeEnum.NATIONALROAD:
            return SaveLocationDTO(road_type=road_type, national_road=numbered)
        return SaveLocationDTO(road_type=road_type, departmental_road=numbered)
    if road_type == RoadTypeEnum.LANE:
        # A named street: city and road names, bounded by house numbers or crossing
        # streets. No geometry is sent, DiaLog geocodes it.
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
