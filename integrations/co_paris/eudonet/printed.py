"""What a record's own fields print into a text, for the readers of free texts and titles to cut
before they judge what is left."""

import re

from .locations import SCOPE_POINT, SCOPE_SECTION
from .vocabulary import MULTI_VALUE_SEPARATOR

# Where the numbers of the bounds are written: a number in a text must be one of them.
BOUND_NUMBER_COLUMNS = (
    "l_from_house_number",
    "l_from_road_name",
    "l_from_address_label",
    "l_to_house_number",
    "l_to_road_name",
    "l_to_address_label",
    "l_point_house_number",
    "l_point_address_label",
)
# What the mail merge prints of a location into its measure's article.
LOCATION_PRINTED_COLUMNS = (
    "l_road_name",
    "l_district",
    "l_side",
    "l_direction",
    *BOUND_NUMBER_COLUMNS,
    "l_from_number_complement",
    "l_to_number_complement",
    "l_point_number_complement",
    "l_from_location_complement",
    "l_to_location_complement",
    "l_point_location_complement",
    "l_reserved_space_type",
    "l_article_details",
    "l_displayed_length",
)
_DIGITS = re.compile(r"\d+")
# "5ème arrondissement" is also typed "5e arrondissement".
_DISTRICT_NUMBER = re.compile(r"^(\d+)(?:ème|er|e)\b")


def bound_numbers(location: dict) -> frozenset[str]:
    return frozenset(
        str(int(digits))
        for column in BOUND_NUMBER_COLUMNS
        for digits in _DIGITS.findall(location[column] or "")
    )


def parameter_spans(measure: dict) -> list[str]:
    """The parameter values, alone and after each formula the mail merge prints before an
    optional one."""
    values = []
    for pair in measure["m_params"] or []:
        if len(pair) == 2 and pair[1].strip():
            values.append(pair[1].strip())
            for part in pair[1].split(MULTI_VALUE_SEPARATOR):
                # Printed with or without its preposition: "aux véhicules de secours".
                values += [part.strip(), re.sub(r"^(aux|à|au|des)\s+", "", part.strip())]
    formulas = [formula for formula in measure["m_option_formulas"] or [] if formula]
    return [*values, *formulas, *(f"{formula} {value}" for formula in formulas for value in values)]


def district_variants(location: dict) -> list[str]:
    district = location["l_district"] or ""
    return [
        _DISTRICT_NUMBER.sub(rf"\g<1>{suffix}", part.strip())
        for part in district.split(";")
        for suffix in ("e", "ème", "er")
    ]


def place_forms(location: dict) -> list[str]:
    """How the mail merge prints a number of places: "sur 4 places", "4 place.s"."""
    counts = {
        value.strip()
        for value in (location["l_reserved_places"], location["l_article_details"])
        if value and value.strip().isdigit()
    }
    kind = (location["l_reserved_space_type"] or "").strip()
    forms = ("sur {} places", "sur {} place", "{} place.s", "{} places", "sur {} emplacements")
    printed = [form.format(count) for count in counts for form in forms]
    return [*printed, *(f"{form} {kind}" for form in printed if kind)]


def location_spans(location: dict) -> list[str]:
    return [
        *(location[column] or "" for column in LOCATION_PRINTED_COLUMNS),
        *district_variants(location),
    ]


def narrower_than_the_street(location: dict) -> bool:
    """A point, or a section with at least one bound: a section named alone is the whole street."""
    if location["l_scope"] == SCOPE_POINT:
        return True
    return location["l_scope"] == SCOPE_SECTION and any(
        location[column] for column in BOUND_NUMBER_COLUMNS
    )


def all_narrower(locations: list[dict]) -> bool:
    return bool(locations) and all(narrower_than_the_street(location) for location in locations)
