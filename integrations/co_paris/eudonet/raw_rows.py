from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

import polars as pl
from loguru import logger

from .vocabulary import UNREAD_MEASURE_LABELS

TABLE_REGULATIONS = 1100
TABLE_MEASURES = 1200
TABLE_LOCATIONS = 2700
# The articles of an act as signed: its complementary articles (one row each, under the act)
# and its measures' articles (mail merge, one row per measure). Table 1400 only holds a nightly
# copy of both, late or missing on some acts (2026-11-18).
TABLE_COMPLEMENTARY_ARTICLES = 2200
TABLE_MEASURE_ARTICLES = 3200

# Link columns as *returned*: their `_fid` is the parent FileId.
FIELD_REGULATION_LINK = 1101
FIELD_MEASURE_LINK = 1202
# To *filter* on the parent, the criterion field is the parent table number: a criterion on
# `1202` matched nothing on every batch (2026-09-16).
FILTER_MEASURE_OF_LOCATION = TABLE_MEASURES

DB_DATE_FORMATS = ("%Y/%m/%d %H:%M:%S", "%Y/%m/%d")


def displayed_value(row: dict[str, Any], field: int) -> str | None:
    """The displayed value of a field. Eudonet's empty string means "not filled in"."""
    value = row.get(str(field))
    if value is None:
        return None
    return str(value) or None


def db_value(row: dict[str, Any], field: int) -> str | None:
    """The database value: a link to a record the API account cannot see displays empty."""
    value = row.get(f"{field}_db")
    if value is None:
        return None
    return str(value) or None


def db_date(row: dict[str, Any], field: int) -> date | None:
    """A `DbValue` date, e.g. "2026/09/08 00:00:00". Out-of-range years are kept as-is."""
    value = row.get(f"{field}_db")
    if value is None or str(value) == "":
        return None
    for pattern in DB_DATE_FORMATS:
        try:
            return datetime.strptime(str(value), pattern).date()
        except ValueError:
            continue
    logger.warning(f"Unreadable Eudonet date {value!r} on field {field}")
    return None


@dataclass(frozen=True)
class Field:
    descid: int
    dtype: Any = pl.Utf8
    read: Callable[[dict[str, Any], int], Any] = displayed_value


REGULATION_FIELDS = {
    "a_identifier": Field(1101),
    "a_title_html": Field(1102),
    "a_type": Field(1108),
    "a_state": Field(1107),
    "a_start_date": Field(1109, pl.Date, db_date),
    "a_end_date": Field(1110, pl.Date, db_date),
    "a_service": Field(1105),
    "a_reason": Field(1114),
    # Free text and links that change what the act says; read only to drop the act.
    "a_notes": Field(1194),
    "a_key_1": Field(1103),
    "a_key_2": Field(1122),
    "a_next_version": Field(1139, read=db_value),
    "a_repealing_act": Field(1140, read=db_value),
    "a_repealed_by_clone": Field(1153, read=db_value),
    "a_repealed_by_multiple": Field(1155, read=db_value),
}
# `1202` on table 1200 is the measure name (catalog), on table 2700 the link to the measure.
MEASURE_NAME = Field(1202)
MEASURE_PARAGRAPH = Field(1254)
MEASURE_NOTES = Field(1294)
# The catalog formula that opens the measure's article ("Le stationnement est interdit").
MEASURE_OPENING = Field(1203)
# The formulas the mail merge prints before the optional parameters 1 to 3
# ("Toutefois ces dispositions ne sont pas applicables" + "aux véhicules de secours").
MEASURE_OPTION_FORMULAS = (1236, 1204, 1207)
COMPLEMENTARY_ARTICLE_TEXT = 2205
MEASURE_ARTICLE_TEXT = 3204
# (parameter name, parameter value): names from catalogs 1218/1221, values from 1219/1222.
MEASURE_PARAMETER_PAIRS = [
    (1218, 1219),
    (1223, 1224),
    (1226, 1227),
    (1221, 1222),
    (1234, 1235),
    (1237, 1238),
    (1240, 1241),
    (1243, 1244),
    (1246, 1247),
]
LOCATION_FIELDS = {
    "l_scope": Field(2705),
    "l_road_name": Field(2710),
    "l_district": Field(2708),
    "l_side": Field(2712),
    "l_direction": Field(2711),
    "l_status": Field(2768),
    "l_from_house_number": Field(2720),
    "l_from_road_name": Field(2730),
    "l_from_address_label": Field(2736),
    "l_to_house_number": Field(2737),
    "l_to_road_name": Field(2740),
    "l_to_address_label": Field(2751),
    "l_point_house_number": Field(2755),
    "l_point_address_label": Field(2754),
    "l_from_location_complement": Field(2742),
    "l_to_location_complement": Field(2760),
    "l_point_location_complement": Field(2766),
    # Closed catalogs on each bound: "au droit du", "en vis à vis du", "vers"…
    "l_from_number_complement": Field(2721),
    "l_to_number_complement": Field(2731),
    "l_point_number_complement": Field(2749),
    "l_from_side": Field(2724),
    "l_to_side": Field(2745),
    "l_from_direction": Field(2722),
    "l_to_direction": Field(2743),
    # Free text and reserved places that narrow the location; read only to drop it.
    "l_notes": Field(2794),
    "l_reserved_space_type": Field(2733),
    "l_reserved_places": Field(2726),
    "l_reserved_length": Field(2734),
    "l_displayed_length": Field(2718),
    # What the mail merge prints after the location ("1 place.s ZL permanente").
    "l_article_details": Field(2715),
}

REGULATION_QUERY_FIELDS = [field.descid for field in REGULATION_FIELDS.values()]
MEASURE_QUERY_FIELDS = [
    FIELD_REGULATION_LINK,
    MEASURE_NAME.descid,
    MEASURE_PARAGRAPH.descid,
    MEASURE_NOTES.descid,
    MEASURE_OPENING.descid,
    *MEASURE_OPTION_FORMULAS,
    *(descid for pair in MEASURE_PARAMETER_PAIRS for descid in pair),
]
LOCATION_QUERY_FIELDS = [FIELD_MEASURE_LINK, *(f.descid for f in LOCATION_FIELDS.values())]
COMPLEMENTARY_ARTICLE_QUERY_FIELDS = [FIELD_REGULATION_LINK, COMPLEMENTARY_ARTICLE_TEXT]
MEASURE_ARTICLE_QUERY_FIELDS = [FIELD_MEASURE_LINK, MEASURE_ARTICLE_TEXT]

RAW_COLUMNS: dict[str, Any] = {
    "a_file_id": pl.Int64,
    **{name: field.dtype for name, field in REGULATION_FIELDS.items()},
    "a_articles": pl.List(pl.Utf8),
    "m_file_id": pl.Int64,
    "m_type": pl.Utf8,
    "m_paragraph": pl.Utf8,
    "m_notes": pl.Utf8,
    "m_article": pl.Utf8,
    "m_opening": pl.Utf8,
    "m_option_formulas": pl.List(pl.Utf8),
    "m_params": pl.List(pl.List(pl.Utf8)),
    # Whether the measure's locations were read (`Expansion.located`).
    "m_located": pl.Boolean,
    "l_file_id": pl.Int64,
    **{name: field.dtype for name, field in LOCATION_FIELDS.items()},
}


def is_read(measure: dict[str, Any]) -> bool:
    """False for a measure of `UNREAD_MEASURE_LABELS`: neither fetched nor read."""
    return displayed_value(measure, MEASURE_NAME.descid) not in UNREAD_MEASURE_LABELS


def count_unreadable_measures(tables: dict[str, list[dict[str, Any]]]) -> int:
    """Measures whose in-scope regulation the API account has no right to read.

    Rights are per record: 13 % of the in-scope measures on 2026-09-08, 23 % of the active
    temporary ones.
    """
    known = {row["FileId"] for row in tables.get(str(TABLE_REGULATIONS), [])}
    return sum(
        1
        for row in tables.get(str(TABLE_MEASURES), [])
        if row.get(f"{FIELD_REGULATION_LINK}_fid") not in known
    )


def _read(row: dict[str, Any], fields: dict[str, Field]) -> dict[str, Any]:
    return {name: field.read(row, field.descid) for name, field in fields.items()}


def _regulation_fields(row: dict[str, Any], articles: list[str]) -> dict[str, Any]:
    return {"a_file_id": row["FileId"], **_read(row, REGULATION_FIELDS), "a_articles": articles}


def _texts(rows: list[dict[str, Any]], field: int) -> list[str]:
    texts = (displayed_value(row, field) for row in rows)
    return [text for text in texts if text]


def _measure_fields(row: dict[str, Any] | None, articles: list[str]) -> dict[str, Any]:
    if row is None:
        return {
            "m_file_id": None,
            "m_type": None,
            "m_paragraph": None,
            "m_notes": None,
            "m_article": None,
            "m_opening": None,
            "m_option_formulas": None,
            "m_params": None,
        }
    params = []
    for name_field, value_field in MEASURE_PARAMETER_PAIRS:
        name = displayed_value(row, name_field)
        if name:
            params.append([name, displayed_value(row, value_field) or ""])
    return {
        "m_file_id": row["FileId"],
        "m_type": displayed_value(row, MEASURE_NAME.descid),
        "m_paragraph": displayed_value(row, MEASURE_PARAGRAPH.descid),
        "m_notes": displayed_value(row, MEASURE_NOTES.descid),
        # One mail merge per measure on 2026-11-17; two would be read as one text.
        "m_article": "\n".join(articles) or None,
        "m_opening": displayed_value(row, MEASURE_OPENING.descid),
        "m_option_formulas": [
            displayed_value(row, field) or "" for field in MEASURE_OPTION_FORMULAS
        ],
        "m_params": params,
    }


def _location_fields(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        return {"l_file_id": None, **{name: None for name in LOCATION_FIELDS}}
    return {"l_file_id": row["FileId"], **_read(row, LOCATION_FIELDS)}


def _group_by_parent(
    children: list[dict[str, Any]], link: int, parents: set[Any] | None = None
) -> tuple[dict[Any, list[dict[str, Any]]], int]:
    """Children by parent `FileId`, and the number of those whose parent is not in `parents`."""
    grouped: dict[Any, list[dict[str, Any]]] = {}
    orphans = 0
    for child in children:
        parent = child.get(f"{link}_fid")
        if parents is not None and parent not in parents:
            orphans += 1
            continue
        grouped.setdefault(parent, []).append(child)
    return grouped, orphans


@dataclass(frozen=True)
class Expansion:
    """The measures whose children are read: locations (`2700`), and articles (`3200`)."""

    located: frozenset[int]
    articled: frozenset[int]


def build_raw_dataframe(
    tables: dict[str, list[dict[str, Any]]], expansion: Expansion, log: bool = True
) -> pl.DataFrame:
    """One row per location, carrying its measure and its regulation.

    Childless parents are kept with null children so that the funnel counts them. The
    measures of `UNREAD_MEASURE_LABELS` get no row at all, with or without locations in the
    tables, and only the measures of `expansion` get their locations and articles, so a frozen
    dump and the online extraction give the same rows.
    """
    regulations = tables.get(str(TABLE_REGULATIONS), [])
    measures = tables.get(str(TABLE_MEASURES), [])
    locations = tables.get(str(TABLE_LOCATIONS), [])
    articles_by_regulation, _ = _group_by_parent(
        tables.get(str(TABLE_COMPLEMENTARY_ARTICLES), []), FIELD_REGULATION_LINK
    )
    articles_by_measure, _ = _group_by_parent(
        tables.get(str(TABLE_MEASURE_ARTICLES), []), FIELD_MEASURE_LINK
    )

    unread = [row for row in measures if not is_read(row)]
    if unread and log:
        kinds = Counter(displayed_value(row, MEASURE_NAME.descid) for row in unread)
        logger.info(
            f"Leaving aside {len(unread)} measures that change nothing else in their act: "
            f"{dict(kinds)}"
        )
    measures = [row for row in measures if is_read(row)]
    measures_by_regulation, _ = _group_by_parent(measures, FIELD_REGULATION_LINK)
    locations_by_measure, orphan_locations = _group_by_parent(
        locations, FIELD_MEASURE_LINK, {row["FileId"] for row in measures}
    )
    if orphan_locations and log:
        logger.warning(f"Ignoring {orphan_locations} locations whose measure is out of scope")

    rows: list[dict[str, Any]] = []
    regulations_without_measure = 0
    measures_without_location = 0
    for regulation in regulations:
        regulation_fields = _regulation_fields(
            regulation,
            _texts(
                articles_by_regulation.get(regulation["FileId"], []), COMPLEMENTARY_ARTICLE_TEXT
            ),
        )
        own_measures = measures_by_regulation.get(regulation["FileId"], [])
        if not own_measures:
            regulations_without_measure += 1
            rows.append(
                {
                    **regulation_fields,
                    **_measure_fields(None, []),
                    "m_located": False,
                    **_location_fields(None),
                }
            )
        for measure in own_measures:
            file_id = measure["FileId"]
            articled = file_id in expansion.articled
            located = file_id in expansion.located
            measure_fields = _measure_fields(
                measure,
                _texts(
                    articles_by_measure.get(file_id, []) if articled else [], MEASURE_ARTICLE_TEXT
                ),
            )
            own_locations = locations_by_measure.get(file_id, []) if located else []
            measures_without_location += located and not own_locations
            for location in own_locations or [None]:
                rows.append(
                    {
                        **regulation_fields,
                        **measure_fields,
                        "m_located": located,
                        **_location_fields(location),
                    }
                )

    if log:
        logger.info(
            f"Built {len(rows)} raw rows from {len(regulations)} regulations, "
            f"{len(measures)} measures and {len(locations)} locations"
        )
    if regulations_without_measure and log:
        logger.warning(f"{regulations_without_measure} regulations have no measure")
    if measures_without_location and log:
        logger.warning(f"{measures_without_location} measures have no location")

    # Eudonet pages in no stated order; the snapshot digest keeps measures and locations
    # in row order, so the rows are put in a fixed one.
    return pl.DataFrame(rows, schema=RAW_COLUMNS).sort(
        "a_file_id", "m_file_id", "l_file_id", nulls_last=True
    )
