from typing import Literal, TypedDict, get_type_hints


class RegulationMeasure(TypedDict):
    """The pivot: one row of a source's clean data. A column not declared here is
    dropped silently by `select_regulation_measure_fields` (D-08)."""

    # Period fields (prefixed with period_)
    period_start_date: str | None
    period_end_date: str | None
    period_recurrence_type: str | None
    period_is_permanent: bool | None
    period_time_slots: list[dict[str, str]] | None
    # Location fields (prefixed with location_)
    location_road_type: str
    location_label: str | None
    location_geometry: str | None
    location_administrator: str | None
    location_road_number: str | None
    location_from_department_code: str | None
    location_from_point_number: str | None
    location_from_abscissa: int | None
    location_from_side: str | None
    location_to_department_code: str | None
    location_to_point_number: str | None
    location_to_abscissa: int | None
    location_to_side: str | None
    location_direction: str | None
    location_city_code: str | None
    location_city_label: str | None
    location_road_name: str | None
    location_from_point_type: Literal["houseNumber", "intersection"] | None
    location_from_house_number: str | None
    location_from_road_name: str | None
    location_to_point_type: Literal["houseNumber", "intersection"] | None
    location_to_house_number: str | None
    location_to_road_name: str | None
    # Regulation fields (prefixed with regulation_)
    regulation_identifier: str
    regulation_category: str
    regulation_subject: str
    regulation_title: str
    regulation_other_category_text: str
    regulation_document_url: str | None
    # Measure fields
    measure_type_: str
    measure_max_speed: int | None
    # Grouping fields — how rows collapse into measures and regulations.
    #
    # A source row is not always a measure: a SIG publishes one row per road segment,
    # and a single measure ("30 km/h") covers thousands of them. Rows sharing
    # `(regulation_identifier, measure_group_key)` become ONE measure carrying N
    # locations, provided the data source sets `group_locations_by_measure`.
    #
    # `regulation_split_order` orders the locations of a regulation that has to be cut
    # into several POSTs (see `max_locations_per_regulation`): rows are sorted by it, so
    # a source that fills it with a geographic ranking keeps each slice spatially
    # coherent instead of arbitrary.
    measure_group_key: str | None
    regulation_split_order: int | None
    # Vehicle fields (prefixed with vehicle_)
    vehicle_all_vehicles: bool
    vehicle_heavyweight_max_weight: float | None
    vehicle_max_height: float | None
    vehicle_max_width: float | None
    vehicle_max_length: float | None
    vehicle_exempted_types: list[str] | None
    vehicle_restricted_types: list[str] | None
    vehicle_other_exempted_type_text: str | None
    vehicle_other_restricted_type_text: str | None


PIVOT_FAMILIES = ("period_", "location_", "regulation_", "measure_", "vehicle_")


def unknown_pivot_columns(columns: list[str]) -> list[str]:
    """Columns with a pivot prefix that the pivot does not declare."""
    declared = set(get_type_hints(RegulationMeasure))
    return sorted(c for c in columns if c.startswith(PIVOT_FAMILIES) and c not in declared)
