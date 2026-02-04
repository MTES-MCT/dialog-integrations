from datetime import datetime
from typing import TypedDict

import pandera.polars as pa

from integrations.shared import typed_dict_to_polars_schema


class BrestRawDataSchema(pa.DataFrameModel):
    """Schema for raw data from Brest shapefile after minimal casting
    - only columns we actually use."""

    NOARR: str | None = pa.Field(nullable=True)
    DESCRIPTIF: str | None = pa.Field(nullable=True)
    LIBRU: str | None = pa.Field(nullable=True)
    LIBCO: str | None = pa.Field(nullable=True)
    geometry: str | None = pa.Field(nullable=True)
    SENS: int | None = pa.Field(nullable=True)
    VELO: bool  # After boolean casting
    CYCLO: bool  # After boolean casting
    VITEMAX: int | None = pa.Field(nullable=True)
    POIDS: float | None = pa.Field(nullable=True)
    HAUTEUR: float | None = pa.Field(nullable=True)
    LARGEUR: float | None = pa.Field(nullable=True)
    DT_MAT: datetime | None = pa.Field(nullable=True)

    class Config:
        strict = False  # Allow extra columns
        coerce = True  # Allow type coercion during validation


class BrestMeasure(TypedDict):
    NOARR: str
    DESCRIPTIF: str
    LIBRU: str
    LIBCO: str
    geometry: str
    SENS: int
    VELO: bool
    CYCLO: bool
    VITEMAX: int | None
    POIDS: float
    HAUTEUR: float
    LARGEUR: float
    DT_MAT: datetime | None
    # Period fields (prefixed with period_)
    period_start_date: str | None
    period_end_date: str | None
    period_start_time: str | None
    period_end_time: str | None
    period_recurrence_type: str | None
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
    measure_max_speed: int | None
    # Vehicle fields (prefixed with vehicle_)
    vehicle_all_vehicles: bool
    vehicle_heavyweight_max_weight: float | None
    vehicle_max_height: float | None
    vehicle_max_width: float | None
    vehicle_exempted_types: list[str] | None
    vehicle_restricted_types: list[str] | None
    vehicle_other_exempted_type_text: str | None


BREST_SCHEMA = typed_dict_to_polars_schema(BrestMeasure)
