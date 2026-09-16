"""From pivot rows (`RegulationMeasure`) to the API payloads.

Pure functions: no network, no organization. A row's columns are mapped to the DTOs by
prefix — `period_*`, `location_*`, `vehicle_*`, with the prefix stripped — and rows are
grouped by `regulation_identifier` into one regulation carrying one measure per row.
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
    SaveNumberedRoadDTO,
    SavePeriodDTO,
    SaveRawGeoJSONDTO,
    SaveVehicleSetDTO,
)
from integrations.base_data_source_integration import RegulationMeasure

MeasureBuilder = Callable[[RegulationMeasure], SaveMeasureDTO]


def build_regulations(
    clean_data: pl.DataFrame,
    status: PostApiRegulationsAddBodyStatus,
    build_measure: MeasureBuilder,
) -> list[PostApiRegulationsAddBody]:
    """Group the rows by `regulation_identifier`, one measure per row.

    The regulation fields are read on the first row of the group: every row of a
    regulation carries the same values. A row whose measure cannot be built is logged
    and skipped; a regulation left without any measure is skipped as a whole.
    """
    regulations = []

    for _, group_df in clean_data.group_by("regulation_identifier"):
        measures = []
        for row in group_df.iter_rows(named=True):
            try:
                measures.append(build_measure(row))  # type: ignore
            except Exception as e:
                logger.error(f"Error creating measure: {e}")

        if not measures:
            continue

        first_row = group_df.row(0, named=True)
        regulation = PostApiRegulationsAddBody(
            identifier=first_row["regulation_identifier"],
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

        regulations.append(regulation)

    return regulations


def build_measure(measure: RegulationMeasure) -> SaveMeasureDTO:
    """One measure: its type, one period, one location, one vehicle set."""
    params = {
        "type_": MeasureTypeEnum(measure["measure_type_"]),
        "periods": [build_period(measure)],
        "locations": [build_location(measure)],
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
    """
    period_fields = _fields_with_prefix(measure, "period_")
    period_fields["start_time"] = period_fields.get("start_date")
    period_fields["end_time"] = period_fields.get("end_date")
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
    if road_type in (RoadTypeEnum.DEPARTMENTALROAD, RoadTypeEnum.NATIONALROAD):
        numbered = SaveNumberedRoadDTO(**location_fields)
        if road_type == RoadTypeEnum.NATIONALROAD:
            return SaveLocationDTO(road_type=road_type, national_road=numbered)
        return SaveLocationDTO(road_type=road_type, departmental_road=numbered)
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
