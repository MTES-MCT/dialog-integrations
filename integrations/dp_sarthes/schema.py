from typing import TypedDict

import pandera.polars as pa

from api.dia_log_client.models import PeriodRecurrenceTypeEnum


class SarthesRawDataSchema(pa.DataFrameModel):
    """Schema for raw data from Sarthes API - only columns we actually use."""

    infobulle: str | None = pa.Field(nullable=True)
    VITESSE: float | None = pa.Field(nullable=True)  # Will be cast to int during validation
    annee: float | None = pa.Field(nullable=True)
    date_modif: str
    geo_shape: str
    loc_txt: str | None = pa.Field(nullable=True)
    longueur: float | None = pa.Field(nullable=True)

    class Config:
        strict = False  # Allow extra columns in raw data
        coerce = True  # Allow type coercion during validation


class SarthesMeasure(TypedDict):
    """Schema for clean data after processing."""

    # Period fields (prefixed with period_)
    period_start_date: str | None
    period_end_date: str | None
    period_start_time: str | None
    period_end_time: str | None
    period_recurrence_type: PeriodRecurrenceTypeEnum | None
    period_is_permanent: bool | None
    # Location fields (prefixed with location_)
    location_road_type: str
    location_label: str
    location_geometry: str
    # Regulation fields (prefixed with regulation_)
    regulation_identifier: str
    regulation_status: str
    regulation_category: str
    regulation_subject: str
    regulation_title: str
    regulation_other_category_text: str
    # Measure fields
    measure_type_: str
    measure_max_speed: int
    # Vehicle fields (prefixed with vehicle_)
    vehicle_all_vehicles: bool
    vehicle_heavyweight_max_weight: float | None
    vehicle_max_height: float | None
    vehicle_max_width: float | None
    vehicle_exempted_types: list[str] | None
    vehicle_restricted_types: list[str] | None
    vehicle_other_exempted_type_text: str | None
