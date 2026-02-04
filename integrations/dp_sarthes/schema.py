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

    id: str
    title: str
    max_speed: int
    geometry: str
    label: str
    # Period fields (prefixed with period_)
    period_start_date: str | None
    period_end_date: str | None
    period_start_time: str | None
    period_end_time: str | None
    period_recurrence_type: PeriodRecurrenceTypeEnum | None
    period_is_permanent: bool | None
