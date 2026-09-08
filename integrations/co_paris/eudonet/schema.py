"""Raw data contract for Paris / Eudonet: one row per location.

`a_` comes from table 1100 (arrêtés), `m_` from 1200 (mesures), `l_` from 2700
(localisation). Only the regulation's own identity is required: a regulation without a
measure, or a measure without a location, must reach `compute_clean_data` so that the
funnel can count it.
"""

from datetime import date, datetime

import pandera.polars as pa


class EudonetRawDataSchema(pa.DataFrameModel):
    # Regulation (table 1100)
    a_file_id: int
    a_identifier: str
    a_title_html: str | None = pa.Field(nullable=True)
    a_type: str
    a_state: str
    a_start_date: date | None = pa.Field(nullable=True)
    a_end_date: date | None = pa.Field(nullable=True)
    a_signed_at: date | None = pa.Field(nullable=True)
    a_service: str | None = pa.Field(nullable=True)
    a_reason: str | None = pa.Field(nullable=True)
    a_modified_at: datetime | None = pa.Field(nullable=True)

    # Measure (table 1200)
    m_file_id: int | None = pa.Field(nullable=True)
    m_type: str | None = pa.Field(nullable=True)
    # The nine (name, value) parameter pairs that carry vehicles, speed, gauge, time slots.
    m_params: list[list[str]] | None = pa.Field(nullable=True)
    m_modified_at: datetime | None = pa.Field(nullable=True)

    # Location (table 2700)
    l_file_id: int | None = pa.Field(nullable=True)
    l_scope: str | None = pa.Field(nullable=True)
    l_road_name: str | None = pa.Field(nullable=True)
    l_district: str | None = pa.Field(nullable=True)
    l_side: str | None = pa.Field(nullable=True)
    l_direction: str | None = pa.Field(nullable=True)
    l_status: str | None = pa.Field(nullable=True)
    l_from_house_number: str | None = pa.Field(nullable=True)
    l_from_road_name: str | None = pa.Field(nullable=True)
    l_from_address_label: str | None = pa.Field(nullable=True)
    l_from_complement: str | None = pa.Field(nullable=True)
    l_to_house_number: str | None = pa.Field(nullable=True)
    l_to_road_name: str | None = pa.Field(nullable=True)
    l_to_address_label: str | None = pa.Field(nullable=True)
    l_to_complement: str | None = pa.Field(nullable=True)
    l_point_house_number: str | None = pa.Field(nullable=True)
    l_point_address_label: str | None = pa.Field(nullable=True)
    l_point_complement: str | None = pa.Field(nullable=True)
    l_axis_name: str | None = pa.Field(nullable=True)
    l_modified_at: datetime | None = pa.Field(nullable=True)

    class Config(pa.DataFrameModel.Config):
        strict = False
        coerce = True
