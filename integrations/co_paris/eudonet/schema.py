"""Raw data contract for Paris / Eudonet: one row per location.

`a_` comes from table 1100 (arrêtés), `m_` from 1200 (mesures), `l_` from 2700
(localisation).
"""

from datetime import date

import pandera.polars as pa


class EudonetRawDataSchema(pa.DataFrameModel):
    a_file_id: int
    a_identifier: str | None = pa.Field(nullable=True)
    a_title_html: str | None = pa.Field(nullable=True)
    a_type: str
    a_state: str
    a_start_date: date | None = pa.Field(nullable=True)
    a_end_date: date | None = pa.Field(nullable=True)
    a_service: str | None = pa.Field(nullable=True)
    a_reason: str | None = pa.Field(nullable=True)
    m_file_id: int | None = pa.Field(nullable=True)
    m_type: str | None = pa.Field(nullable=True)
    m_params: list[list[str]] | None = pa.Field(nullable=True)
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
    l_to_house_number: str | None = pa.Field(nullable=True)
    l_to_road_name: str | None = pa.Field(nullable=True)
    l_to_address_label: str | None = pa.Field(nullable=True)
    l_point_house_number: str | None = pa.Field(nullable=True)
    l_point_address_label: str | None = pa.Field(nullable=True)
    l_from_location_complement: str | None = pa.Field(nullable=True)
    l_to_location_complement: str | None = pa.Field(nullable=True)
    l_point_location_complement: str | None = pa.Field(nullable=True)

    class Config(pa.DataFrameModel.Config):
        strict = False
        coerce = True
