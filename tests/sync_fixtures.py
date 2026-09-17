"""Builders shared by the synchronization tests. Not a test module."""

import json

import polars as pl

from api.dia_log_client.models import (
    MeasureTypeEnum,
    PostApiRegulationsAddBody,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodyStatus,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
    SaveLocationDTO,
    SaveMeasureDTO,
    SavePeriodDTO,
    SaveRawGeoJSONDTO,
    SaveVehicleSetDTO,
)

LINE = {"type": "LineString", "coordinates": [[-4.486, 48.39], [-4.484, 48.392]]}


def geometry(coordinates=None) -> str:
    return json.dumps({"type": "LineString", "coordinates": coordinates or LINE["coordinates"]})


def build_regulation(
    identifier: str,
    title: str = "Travaux rue Exemple",
    max_speed: int = 30,
    geometry_json: str | None = None,
    label: str = "rue Exemple",
) -> PostApiRegulationsAddBody:
    """A one-measure regulation, the smallest payload the pipeline can send."""
    measure = SaveMeasureDTO(
        type_=MeasureTypeEnum.SPEEDLIMITATION,
        max_speed=max_speed,
        periods=[SavePeriodDTO(start_date="2026-01-01", end_date="2026-02-01")],
        locations=[
            SaveLocationDTO(
                road_type=RoadTypeEnum.RAWGEOJSON,
                raw_geo_json=SaveRawGeoJSONDTO(label=label, geometry=geometry_json or geometry()),
            )
        ],
        vehicle_set=SaveVehicleSetDTO(all_vehicles=True),
    )
    return PostApiRegulationsAddBody(
        identifier=identifier,
        category=PostApiRegulationsAddBodyCategory.TEMPORARYREGULATION,
        status=PostApiRegulationsAddBodyStatus.DRAFT,
        subject=PostApiRegulationsAddBodySubject.ROADMAINTENANCE,
        title=title,
        other_category_text=None,
        measures=[measure],  # type: ignore[arg-type]
    )


def measure_rows(identifiers, max_speed: int = 30) -> pl.DataFrame:
    """One clean row per identifier, in the pivot's column names."""
    return pl.DataFrame(
        [
            {
                "regulation_identifier": identifier,
                "regulation_category": "temporaryRegulation",
                "regulation_subject": "roadMaintenance",
                "regulation_title": f"Travaux {identifier}",
                "regulation_other_category_text": None,
                "measure_type_": "speedLimitation",
                "measure_max_speed": max_speed,
                "period_start_date": "2026-01-01",
                "period_end_date": "2026-02-01",
                "location_road_type": "rawGeoJSON",
                "location_label": f"rue {identifier}",
                "location_geometry": geometry(),
                "vehicle_all_vehicles": True,
            }
            for identifier in identifiers
        ]
    )
