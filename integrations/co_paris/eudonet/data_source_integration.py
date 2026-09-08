"""Paris / Eudonet: extraction of the in-scope regulations, measures and locations.

Eudonet holds three nested tables:

    1100 Arrêtés  →  1200 Mesures  →  2700 Localisation

`fetch_raw_data` returns **one row per location**, joined to its measure and to its
regulation (columns prefixed `a_`, `m_`, `l_`). A regulation without any measure, and a
measure without any location, still produce a row with null children: the funnel has to be
able to count those losses rather than silently miss them.

Nothing is written back: the Eudonet account is read-only.
"""

import html
import json
import os
import re
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import polars as pl
from loguru import logger

from api.dia_log_client.models import PeriodRecurrenceTypeEnum, PostApiRegulationsAddBodySubject
from integrations.base_data_source_integration import BaseDataSourceIntegration
from integrations.local_time import end_of_local_day, start_of_local_day

from .client import EQUALS, GREATER_OR_EQUAL, EudonetClient, EudonetError, all_of, any_of, criterion
from .locations import compute_location_fields
from .schema import EudonetRawDataSchema
from .vocabulary import (
    ACCEPTED_STATE_LABELS,
    ALL_VEHICLES_LABELS,
    DIRECTIONAL_LOCATION_LABELS,
    EXEMPTED_OTHER,
    EXEMPTION_BY_LABEL,
    EXEMPTION_PARAMETERS,
    IGNORED_PARAMETERS,
    IMPLIED_MAX_SPEED_BY_LABEL,
    LABEL_CATEGORY_LIMIT,
    LABEL_DIMENSION_LIMIT,
    LABEL_ONE_WAY,
    LABEL_SPEED_LIMIT,
    MEASURE_TYPE_BY_LABEL,
    MULTI_VALUE_SEPARATOR,
    PERMANENT_SUBJECT_TEXT,
    REGULATION_CATEGORY_BY_LABEL,
    REGULATION_SUBJECT_BY_REASON,
    RESTRICTED_OTHER,
    RESTRICTION_BY_LABEL,
    RESTRICTION_PARAMETERS,
    TIME_SLOT_PARAMETERS,
    TYPE_PERMANENT_LABEL,
    TYPE_TEMPORARY_LABEL,
    UNKNOWN_REASON_TEXT,
)

PARIS = ZoneInfo("Europe/Paris")

CREDENTIALS_ENV_VAR = "EUDONET_PARIS_CREDENTIALS"
FIXTURE_ENV_VAR = "EUDONET_PARIS_FIXTURE"

TABLE_REGULATIONS = 1100
TABLE_MEASURES = 1200
TABLE_LOCATIONS = 2700

# Link fields: `1101` carries the regulation of a measure, `1202` the measure of a location.
FIELD_REGULATION_LINK = 1101
FIELD_MEASURE_LINK = 1202

# Perimeter fields on table 1100.
FIELD_TYPE = 1108
FIELD_STATE = 1107
FIELD_END_DATE = 1110

# Catalog `DBValue`s, taken from the `catalogs.json` of the exploration dump.
TYPE_PERMANENT = "7"
TYPE_TEMPORARY = "8"
STATE_PERMANENT_IN_FORCE = "96"
STATES_TEMPORARY_ACTIVE = ("8640", "13789", "8642")  # En vigueur, Publié, Signé

# Measured at 2.2 % on the 2026-09-08 dump; well above means the locations went missing.
MAX_UNLOCATED_MEASURES_SHARE = 0.25

REGULATION_COLUMNS = [1101, 1102, 1105, 1107, 1108, 1109, 1110, 1111, 1114, 1196]
MEASURE_COLUMNS = [
    1101,
    1202,
    1218,
    1219,
    1223,
    1224,
    1226,
    1227,
    1221,
    1222,
    1234,
    1235,
    1237,
    1238,
    1240,
    1241,
    1243,
    1244,
    1246,
    1247,
    1296,
]
LOCATION_COLUMNS = [
    1202,
    2705,
    2710,
    2708,
    2712,
    2711,
    2768,
    2717,
    2720,
    2730,
    2736,
    2737,
    2740,
    2751,
    2755,
    2754,
    2721,
    2731,
    2749,
    2796,
]

# The nine (parameter name, parameter value) pairs of a measure, flattened into `m_params`.
MEASURE_PARAMETER_PAIRS = [
    ("1218", "1219"),
    ("1223", "1224"),
    ("1226", "1227"),
    ("1221", "1222"),
    ("1234", "1235"),
    ("1237", "1238"),
    ("1240", "1241"),
    ("1243", "1244"),
    ("1246", "1247"),
]

# The column contract of the raw dataframe, shared with `schema.py` and with the
# transformations built on top of it.
RAW_COLUMNS: dict[str, Any] = {
    "a_file_id": pl.Int64,
    "a_identifier": pl.Utf8,
    "a_title_html": pl.Utf8,
    "a_type": pl.Utf8,
    "a_state": pl.Utf8,
    "a_start_date": pl.Date,
    "a_end_date": pl.Date,
    "a_signed_at": pl.Date,
    "a_service": pl.Utf8,
    "a_reason": pl.Utf8,
    "a_modified_at": pl.Datetime("us"),
    "m_file_id": pl.Int64,
    "m_type": pl.Utf8,
    "m_params": pl.List(pl.List(pl.Utf8)),
    "m_modified_at": pl.Datetime("us"),
    "l_file_id": pl.Int64,
    "l_scope": pl.Utf8,
    "l_road_name": pl.Utf8,
    "l_district": pl.Utf8,
    "l_side": pl.Utf8,
    "l_direction": pl.Utf8,
    "l_status": pl.Utf8,
    "l_from_house_number": pl.Utf8,
    "l_from_road_name": pl.Utf8,
    "l_from_address_label": pl.Utf8,
    "l_from_complement": pl.Utf8,
    "l_to_house_number": pl.Utf8,
    "l_to_road_name": pl.Utf8,
    "l_to_address_label": pl.Utf8,
    "l_to_complement": pl.Utf8,
    "l_point_house_number": pl.Utf8,
    "l_point_address_label": pl.Utf8,
    "l_point_complement": pl.Utf8,
    "l_axis_name": pl.Utf8,
    "l_modified_at": pl.Datetime("us"),
}

# `DbValue` of a date field, e.g. "2026/09/08 00:00:00".
DB_DATE_FORMATS = ("%Y/%m/%d %H:%M:%S", "%Y/%m/%d")

# Synchronisation prefix (plan §8, journal of 2026-09-08). See `compute_regulation_fields`.
IDENTIFIER_PREFIX = "PARIS-EUDO-"

TITLE_MAX_LENGTH = 255
TITLE_ELLIPSIS = "..."
OTHER_CATEGORY_TEXT_MAX_LENGTH = 100
# `otherRestrictedTypeText` / `otherExemptedTypeText` have no documented limit; capped on
# the longest string the API is known to accept (`title`) rather than left unbounded.
OTHER_TYPE_TEXT_MAX_LENGTH = 255
FREE_TEXT_SEPARATOR = " ; "

SUBJECT_OTHER = PostApiRegulationsAddBodySubject.OTHER.value
SPEED_PARAMETER = "valeur de la vitesse"

# Working column: the start date a permanent regulation ends up using (R-39).
PERIOD_START_SOURCE = "_period_start_source"


def today_in_paris() -> date:
    """The current day on the French calendar, which is the one Eudonet dates are on."""
    return datetime.now(PARIS).date()


def perimeter_criteria(today: date) -> dict[str, Any]:
    """The regulations DiaLog broadcasts: permanent in force, or temporary still running.

    `(1108 = Permanent AND 1107 = En vigueur)`
    `OR (1108 = Temporaire AND 1110 >= today AND 1107 IN (En vigueur, Publié, Signé))`

    Catalog fields compare on their `DBValue`; dates go in as `YYYY/MM/DD`.
    """
    return any_of(
        all_of(
            criterion(FIELD_TYPE, EQUALS, TYPE_PERMANENT),
            criterion(FIELD_STATE, EQUALS, STATE_PERMANENT_IN_FORCE),
        ),
        all_of(
            criterion(FIELD_TYPE, EQUALS, TYPE_TEMPORARY),
            criterion(FIELD_END_DATE, GREATER_OR_EQUAL, today.strftime("%Y/%m/%d")),
            any_of(*(criterion(FIELD_STATE, EQUALS, state) for state in STATES_TEMPORARY_ACTIVE)),
        ),
    )


def extract_tables(client: EudonetClient, today: date) -> dict[str, list[dict[str, Any]]]:
    """The three tables of the perimeter, as flat rows keyed by table number.

    Measures are filtered on their parent's fields, which the API accepts one level up but
    not two: locations are therefore fetched by batches of measure identifiers.
    """
    where = perimeter_criteria(today)

    regulations = client.search_all(TABLE_REGULATIONS, REGULATION_COLUMNS, where, "regulations")
    measures = client.search_all(TABLE_MEASURES, MEASURE_COLUMNS, where, "measures")
    measure_ids = [row["FileId"] for row in measures]
    locations = client.search_by_ids(
        TABLE_LOCATIONS, LOCATION_COLUMNS, FIELD_MEASURE_LINK, measure_ids, label="locations"
    )

    check_extraction_completeness(measures, locations)

    return {
        str(TABLE_REGULATIONS): regulations,
        str(TABLE_MEASURES): measures,
        str(TABLE_LOCATIONS): locations,
    }


def check_extraction_completeness(
    measures: list[dict[str, Any]], locations: list[dict[str, Any]]
) -> None:
    """Refuse an extraction that lost its locations on the way.

    On the 2026-09-08 dump, 2.2 % of the in-scope measures have no location. The API was
    seen answering an empty page once (see `EudonetClient.search_by_ids`); a truncated
    extraction must fail here rather than be mistaken for thousands of withdrawn measures
    once synchronisation deletes what the source no longer carries.
    """
    if not measures:
        raise EudonetError("Eudonet returned no measure at all for the perimeter")
    located = {row.get(f"{FIELD_MEASURE_LINK}_fid") for row in locations}
    unlocated = sum(1 for row in measures if row["FileId"] not in located)
    share = unlocated / len(measures)
    logger.info(f"{unlocated} measures out of {len(measures)} have no location ({share:.1%})")
    if share > MAX_UNLOCATED_MEASURES_SHARE:
        raise EudonetError(
            f"{share:.0%} of the measures have no location "
            f"(limit {MAX_UNLOCATED_MEASURES_SHARE:.0%}): the extraction looks truncated"
        )


def load_fixture(path: Path) -> dict[str, list[dict[str, Any]]]:
    """The frozen extract used offline: same row shape as the API, same code downstream."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload["tables"]


def build_raw_dataframe(tables: dict[str, list[dict[str, Any]]]) -> pl.DataFrame:
    """One row per location, carrying its measure and its regulation.

    Childless parents are kept with null children so that the funnel counts them:
    a regulation with no measure, a measure with no location.
    """
    regulations = tables.get(str(TABLE_REGULATIONS), [])
    measures = tables.get(str(TABLE_MEASURES), [])
    locations = tables.get(str(TABLE_LOCATIONS), [])

    known_regulations = {row["FileId"] for row in regulations}
    measures_by_regulation: dict[Any, list[dict[str, Any]]] = {}
    orphan_measures = 0
    for measure in measures:
        parent = measure.get(f"{FIELD_REGULATION_LINK}_fid")
        if parent not in known_regulations:
            orphan_measures += 1
            continue
        measures_by_regulation.setdefault(parent, []).append(measure)
    if orphan_measures:
        logger.warning(f"Ignoring {orphan_measures} measures whose regulation is out of scope")

    known_measures = {row["FileId"] for row in measures}
    locations_by_measure: dict[Any, list[dict[str, Any]]] = {}
    orphan_locations = 0
    for location in locations:
        parent = location.get(f"{FIELD_MEASURE_LINK}_fid")
        if parent not in known_measures:
            orphan_locations += 1
            continue
        locations_by_measure.setdefault(parent, []).append(location)
    if orphan_locations:
        logger.warning(f"Ignoring {orphan_locations} locations whose measure is out of scope")

    rows: list[dict[str, Any]] = []
    regulations_without_measure = 0
    measures_without_location = 0

    for regulation in regulations:
        regulation_fields = _regulation_fields(regulation)
        own_measures = measures_by_regulation.get(regulation["FileId"], [])
        if not own_measures:
            regulations_without_measure += 1
            rows.append({**regulation_fields, **_measure_fields(None), **_location_fields(None)})
            continue

        for measure in own_measures:
            measure_fields = _measure_fields(measure)
            own_locations = locations_by_measure.get(measure["FileId"], [])
            if not own_locations:
                measures_without_location += 1
                rows.append({**regulation_fields, **measure_fields, **_location_fields(None)})
                continue
            for location in own_locations:
                rows.append({**regulation_fields, **measure_fields, **_location_fields(location)})

    logger.info(
        f"Built {len(rows)} raw rows from {len(regulations)} regulations, "
        f"{len(measures)} measures and {len(locations)} locations"
    )
    if regulations_without_measure:
        logger.warning(f"{regulations_without_measure} regulations have no measure")
    if measures_without_location:
        logger.warning(f"{measures_without_location} measures have no location")

    return pl.DataFrame(rows, schema=RAW_COLUMNS)


class DataSourceIntegration(BaseDataSourceIntegration):
    name = "eudonet"
    raw_data_schema = EudonetRawDataSchema
    metrics: dict[str, int]

    def fetch_raw_data(self) -> pl.DataFrame:
        """Read the perimeter from Eudonet, or from the fixture when one is pointed at."""
        fixture = os.environ.get(FIXTURE_ENV_VAR)
        if fixture:
            logger.warning(
                f"{FIXTURE_ENV_VAR} is set: reading the frozen extract {fixture} "
                "instead of calling the Eudonet API"
            )
            tables = load_fixture(Path(fixture))
        else:
            client = EudonetClient(self._credentials())
            tables = extract_tables(client, today_in_paris())
        # Source volumes for the run report and the Tchap message (read by
        # `BaseIntegration._source_metrics`). Keys are the French labels shown as-is.
        regulations = tables.get(str(TABLE_REGULATIONS), [])
        measures = tables.get(str(TABLE_MEASURES), [])
        known = {row["FileId"] for row in regulations}
        # The API account cannot read every regulation (per-record rights): on the
        # 2026-09-08 dump, 13 % of the in-scope measures (23 % of the active temporary
        # ones) hang from a regulation that `Search/1100` never returns. Counted here so
        # the daily report shows the loss; nothing downstream can recover it.
        unreadable = sum(
            1 for row in measures if row.get(f"{FIELD_REGULATION_LINK}_fid") not in known
        )
        self.metrics = {
            "arrêtés du périmètre": len(regulations),
            "mesures": len(measures),
            "mesures d'arrêtés illisibles pour le compte": unreadable,
            "localisations": len(tables.get(str(TABLE_LOCATIONS), [])),
        }
        return build_raw_dataframe(tables)

    def compute_clean_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        """The five transformations, piped in the order their filters depend on.

        The perimeter filter comes first (it removes whole regulations), then the measure
        type (it decides what a row even is), then the vehicles it restricts, then the
        dates, then the location. `compute_location_fields` lives in `locations.py`: it
        owns the `l_*` columns, the `2768` status and the shapes of the borders.
        """
        return (
            raw_data.pipe(compute_regulation_fields)
            .pipe(compute_measure_fields)
            .pipe(compute_vehicle_fields)
            .pipe(compute_period_fields)
            .pipe(compute_location_fields)
        )

    def _credentials(self) -> dict[str, Any]:
        raw = getattr(self.organization_settings, "eudonet_paris_credentials", None)
        # Offline tools build settings from a duck-typed stand-in that carries no source
        # credential: fall back on the process environment.
        raw = raw or os.environ.get(CREDENTIALS_ENV_VAR)
        if not raw:
            raise EudonetError(
                f"{CREDENTIALS_ENV_VAR} is not set: either provide the Eudonet credentials "
                f"or point {FIXTURE_ENV_VAR} at a frozen extract"
            )
        return json.loads(raw)


def _text(row: dict[str, Any], field: int | str) -> str | None:
    """The displayed value of a field. Eudonet's empty string means "not filled in"."""
    value = row.get(str(field))
    if value is None:
        return None
    value = str(value)
    return value or None


def _db_text(row: dict[str, Any], field: int | str) -> str | None:
    value = row.get(f"{field}_db")
    if value is None:
        return None
    value = str(value)
    return value or None


def _db_datetime(row: dict[str, Any], field: int | str) -> datetime | None:
    """A `DbValue` date, e.g. "2026/09/08 00:00:00". Out-of-range years are kept as-is."""
    value = _db_text(row, field)
    if value is None:
        return None
    for pattern in DB_DATE_FORMATS:
        try:
            return datetime.strptime(value, pattern)
        except ValueError:
            continue
    logger.warning(f"Unreadable Eudonet date {value!r} on field {field}")
    return None


def _db_date(row: dict[str, Any], field: int | str) -> date | None:
    parsed = _db_datetime(row, field)
    return parsed.date() if parsed else None


def _regulation_fields(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "a_file_id": row["FileId"],
        "a_identifier": _text(row, 1101),
        "a_title_html": _text(row, 1102),
        "a_type": _text(row, 1108),
        "a_state": _text(row, 1107),
        "a_start_date": _db_date(row, 1109),
        "a_end_date": _db_date(row, 1110),
        "a_signed_at": _db_date(row, 1111),
        "a_service": _text(row, 1105),
        "a_reason": _text(row, 1114),
        "a_modified_at": _db_datetime(row, 1196),
    }


def _measure_parameters(row: dict[str, Any]) -> list[list[str]]:
    """The named parameters of a measure, in Eudonet's own order.

    A pair whose name is empty is not a parameter; a named parameter with no value keeps
    an empty value rather than disappearing.
    """
    pairs = []
    for name_field, value_field in MEASURE_PARAMETER_PAIRS:
        name = _text(row, name_field)
        if name:
            pairs.append([name, _text(row, value_field) or ""])
    return pairs


def _measure_fields(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        return {"m_file_id": None, "m_type": None, "m_params": None, "m_modified_at": None}
    return {
        "m_file_id": row["FileId"],
        "m_type": _text(row, 1202),
        "m_params": _measure_parameters(row),
        "m_modified_at": _db_datetime(row, 1296),
    }


def _location_fields(row: dict[str, Any] | None) -> dict[str, Any]:
    if row is None:
        return {name: None for name in RAW_COLUMNS if name.startswith("l_")}
    return {
        "l_file_id": row["FileId"],
        "l_scope": _text(row, 2705),
        "l_road_name": _text(row, 2710),
        "l_district": _text(row, 2708),
        "l_side": _text(row, 2712),
        "l_direction": _text(row, 2711),
        "l_status": _text(row, 2768),
        "l_from_house_number": _text(row, 2720),
        "l_from_road_name": _text(row, 2730),
        "l_from_address_label": _text(row, 2736),
        "l_from_complement": _text(row, 2721),
        "l_to_house_number": _text(row, 2737),
        "l_to_road_name": _text(row, 2740),
        "l_to_address_label": _text(row, 2751),
        "l_to_complement": _text(row, 2731),
        "l_point_house_number": _text(row, 2755),
        "l_point_address_label": _text(row, 2754),
        "l_point_complement": _text(row, 2749),
        "l_axis_name": _text(row, 2717),
        "l_modified_at": _db_datetime(row, 2796),
    }


# ---------------------------------------------------------------------------
# Transformations (plan §3, §4)
#
# Free functions piped in order by `compute_clean_data`. Each one owns a slice of the
# pivot, documents the rows it drops, and logs every drop with its volume: what we do not
# publish has to be countable (R-03).
# ---------------------------------------------------------------------------


class EudonetVocabularyError(Exception):
    """A catalog label the closed tables of `vocabulary.py` do not know.

    Raised rather than mapped to a default: Paris can add a value to a catalog at any time,
    and guessing what it means would put an invented restriction on a GPS (R-02, Q-05).
    """


def _clean_html(value: str | None) -> str:
    """Eudonet's `1102` is an HTML memo: strip the markup, decode the entities, unwrap.

    Some records hold a whole Word paste, comments included.
    """
    if not value:
        return ""
    without_comments = re.sub(r"<!--.*?-->", " ", value, flags=re.DOTALL)
    without_tags = re.sub(r"<[^>]+>", " ", without_comments)
    return re.sub(r"\s+", " ", html.unescape(without_tags)).strip()


def _truncate(value: str, limit: int, ellipsis: str = TITLE_ELLIPSIS) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - len(ellipsis)] + ellipsis


def _count_by(df: pl.DataFrame, column: str) -> dict[str, int]:
    """Occurrences of a column's values, for a log line that names what was dropped."""
    if not df.height:
        return {}
    counts = df.get_column(column).fill_null("(empty)").value_counts(sort=True)
    return {str(k): int(v) for k, v in counts.iter_rows()}


def compute_regulation_fields(df: pl.DataFrame, today: date | None = None) -> pl.DataFrame:
    """The regulation half of the pivot, and the perimeter filter that goes with it.

    Produces `regulation_identifier`, `regulation_title`, `regulation_category`,
    `regulation_subject`, `regulation_other_category_text`, `regulation_document_url`.

    Drops (each counted in a log line):

    * regulations whose `1107` state is not one of `En vigueur`, `Publié`, `Signé` — an act
      that is not signed is not enforceable, and `Périmé` / `Abrogé` have left the perimeter;
    * temporary regulations whose `1110` end date is before today, or missing. Online the
      Eudonet query already filters them out; offline (fixture, frozen dump) it does not, and
      the two paths have to produce the same thing.

    **Identifier (R-23).** `regulation_identifier = "PARIS-EUDO-" + 1101`.

    `1101` « N° d'arrêté » is Eudonet's own business key: unique across the base, stable
    across the life of the act (the nightly state recomputation never touches it), and it is
    what the PDF, the BOVP and the Paris services all quote. It follows the pattern
    `AAAA[T|P|E|C]NNNNN` — year, one letter for the nature of the act (T temporary,
    P permanent, E, C), then a sequence number: `2021T113851`, `2023P111563`. Acts taken over
    from the pre-2017 tool keep their old number instead, `AAAA-NNNNN` or `AA-NNNNN`:
    `1996-10345`, `05-00133`. Both shapes are unique, so `1101` is used **as is**, with no
    normalisation that could make two distinct acts collide.

    The `PARIS-EUDO-` prefix keeps this channel apart from the two that already write into
    the same DiaLog organizations: the historical PHP channel posts the bare `1101`
    (`2023T15201`), and the police prefecture posts `Paris_2024P12345`. A hyphen rather than
    a slash, because a slash in an identifier has already bitten the pipeline (review I-7).
    The result stays well inside the 60 characters the API allows.

    **Title (R-24).** `"Arrêté n° {1101} — {1102 cleaned}"`, HTML stripped, entities decoded,
    whitespace collapsed, truncated to 255 characters. An empty `1102` gives
    `"Arrêté n° {1101}"`: a title is never a reason to drop a regulation, it is built.

    **Subject (R-27).** `Travaux` → `roadMaintenance`; `Evènement`, `Manifestation`,
    `Cinéma` → `event`; anything else, empty included, → `other` plus the reason itself as
    `otherCategoryText` (≤ 100 characters). Permanent regulations carry no reason at all in
    Eudonet and get `other` + "Réglementation permanente" (plan §3).
    """
    today = today or today_in_paris()

    kept_state = pl.col("a_state").is_in(ACCEPTED_STATE_LABELS)
    dropped = df.filter(~kept_state)
    if dropped.height:
        logger.warning(
            f"Dropping {dropped.height} rows whose regulation state is out of scope: "
            f"{_count_by(dropped, 'a_state')}"
        )
    df = df.filter(kept_state)

    is_temporary = pl.col("a_type") == TYPE_TEMPORARY_LABEL
    expired = df.filter(is_temporary & pl.col("a_end_date").is_not_null())
    expired = expired.filter(pl.col("a_end_date") < today)
    if expired.height:
        logger.warning(
            f"Dropping {expired.height} rows of {expired['a_identifier'].n_unique()} temporary "
            f"regulations that ended before {today}"
        )
    undated = df.filter(is_temporary & pl.col("a_end_date").is_null())
    if undated.height:
        logger.warning(
            f"Dropping {undated.height} rows of {undated['a_identifier'].n_unique()} temporary "
            "regulations without an end date: nothing would ever close them (R-37)"
        )
    df = df.filter(~is_temporary | (pl.col("a_end_date") >= today))

    unknown_types = sorted(
        repr(label)
        for label in set(df.get_column("a_type").unique().to_list())
        - set(REGULATION_CATEGORY_BY_LABEL)
    )
    if unknown_types:
        raise EudonetVocabularyError(
            f"Unknown Eudonet regulation type(s) in catalog 1108: {unknown_types}. "
            "The category decides how the periods are built: it cannot be guessed."
        )

    title = pl.Series(
        "regulation_title",
        [
            _truncate(
                f"Arrêté n° {identifier} — {cleaned}" if cleaned else f"Arrêté n° {identifier}",
                TITLE_MAX_LENGTH,
            )
            for identifier, cleaned in zip(
                df.get_column("a_identifier"),
                (_clean_html(raw) for raw in df.get_column("a_title_html")),
            )
        ],
        dtype=pl.Utf8,
    )

    is_permanent = pl.col("a_type") == TYPE_PERMANENT_LABEL
    reason = pl.col("a_reason").fill_null("").str.strip_chars()
    mapped_subject = reason.replace_strict(
        REGULATION_SUBJECT_BY_REASON, default=None, return_dtype=pl.Utf8
    )

    return df.with_columns(
        [
            (pl.lit(IDENTIFIER_PREFIX) + pl.col("a_identifier")).alias("regulation_identifier"),
            title,
            pl.col("a_type")
            .replace_strict(REGULATION_CATEGORY_BY_LABEL, return_dtype=pl.Utf8)
            .alias("regulation_category"),
            pl.when(is_permanent)
            .then(pl.lit(SUBJECT_OTHER))
            .otherwise(mapped_subject.fill_null(SUBJECT_OTHER))
            .alias("regulation_subject"),
            pl.when(is_permanent)
            .then(pl.lit(PERMANENT_SUBJECT_TEXT))
            .when(reason == "")
            .then(pl.lit(UNKNOWN_REASON_TEXT))
            .otherwise(reason.str.slice(0, OTHER_CATEGORY_TEXT_MAX_LENGTH))
            .alias("regulation_other_category_text"),
            # No durable URL for the signed PDF: the Eudonet attachment link is a one-shot
            # token, and the BOVP has no per-act permalink we could rebuild (plan §6).
            pl.lit(None, dtype=pl.Utf8).alias("regulation_document_url"),
        ]
    )


def _measure_parameter_values(params: list[list[str]] | None, wanted: str) -> list[str]:
    """The non-empty values of a named parameter, already split on the "; " separator."""
    if not params:
        return []
    values: list[str] = []
    for pair in params:
        if len(pair) != 2 or pair[0].strip() != wanted:
            continue
        for part in pair[1].split(MULTI_VALUE_SEPARATOR):
            stripped = part.strip()
            if stripped:
                values.append(stripped)
    return values


def compute_measure_fields(df: pl.DataFrame) -> pl.DataFrame:
    """`measure_type_` and `measure_max_speed`, from the closed table of catalog `1202`.

    Drops (each counted in a log line):

    * rows carrying no measure at all (`m_type` null): a regulation whose measures are all
      out of the perimeter still reaches here, so the funnel can see it;
    * measures whose type has no DiaLog equivalent, counted **by label** (R-30);
    * `limitation de vitesse` without a readable "valeur de la vitesse" parameter: a speed
      limitation with no speed says nothing, and 30 or 50 is not ours to pick (R-02, R-35);
    * `sens interdit (ou sens unique)` whose location does not name a direction along the
      segment (R-32). `2711` is filled on 0 of the 1 187 one-way locations of the
      2026-09-08 perimeter, so this drops all of them today — that is the measurement, not
      an accident, and it is what makes question 2 to Paris worth asking.

    Raises `EudonetVocabularyError` on a measure label absent from `MEASURE_TYPE_BY_LABEL`:
    Paris can extend catalog `1202`, and a new value has to be qualified by a human.

    `zone 30` carries its limit in its own name (30 km/h); every other speed comes from the
    parameter, read as the first integer before `km/h` ("à 30 km/h", "30 km/h").
    """
    no_measure = df.filter(pl.col("m_type").is_null())
    if no_measure.height:
        logger.warning(
            f"Dropping {no_measure.height} rows without any measure "
            f"({no_measure['a_identifier'].n_unique()} regulations)"
        )
    df = df.filter(pl.col("m_type").is_not_null())

    unknown = sorted(
        repr(label)
        for label in set(df.get_column("m_type").unique().to_list()) - set(MEASURE_TYPE_BY_LABEL)
    )
    if unknown:
        raise EudonetVocabularyError(
            f"Unknown Eudonet measure label(s) in catalog 1202: {unknown}. "
            "Qualify them in vocabulary.MEASURE_TYPE_BY_LABEL before integrating again."
        )

    df = df.with_columns(
        pl.col("m_type")
        .replace_strict(MEASURE_TYPE_BY_LABEL, return_dtype=pl.Utf8)
        .alias("measure_type_")
    )

    out_of_model = df.filter(pl.col("measure_type_").is_null())
    if out_of_model.height:
        logger.warning(
            f"Dropping {out_of_model.height} measures without a DiaLog equivalent: "
            f"{_count_by(out_of_model, 'm_type')}"
        )
    df = df.filter(pl.col("measure_type_").is_not_null())

    speeds: list[int | None] = []
    for measure_label, params in zip(df.get_column("m_type"), df.get_column("m_params").to_list()):
        implied = IMPLIED_MAX_SPEED_BY_LABEL.get(measure_label)
        if implied is not None:
            speeds.append(implied)
            continue
        if measure_label != LABEL_SPEED_LIMIT:
            speeds.append(None)
            continue
        found = None
        for value in _measure_parameter_values(params, SPEED_PARAMETER):
            match = re.search(r"(\d+)\s*km/h", value)
            if match:
                found = int(match.group(1))
                break
        speeds.append(found)
    df = df.with_columns(pl.Series("measure_max_speed", speeds, dtype=pl.Int32))

    speedless = (pl.col("m_type") == LABEL_SPEED_LIMIT) & pl.col("measure_max_speed").is_null()
    dropped = df.filter(speedless)
    if dropped.height:
        logger.warning(
            f"Dropping {dropped.height} '{LABEL_SPEED_LIMIT}' measures without a readable "
            f"'{SPEED_PARAMETER}' parameter (R-02: no default speed)"
        )
    df = df.filter(~speedless)

    has_direction = pl.col("l_direction").is_in(DIRECTIONAL_LOCATION_LABELS).fill_null(False)
    is_one_way = pl.col("m_type") == LABEL_ONE_WAY
    dropped = df.filter(is_one_way & ~has_direction)
    if dropped.height:
        logger.warning(
            f"Dropping {dropped.height} '{LABEL_ONE_WAY}' measures whose location carries no "
            "usable direction (2711): a one-way without a direction is a coin toss (R-32)"
        )
    return df.filter(~is_one_way | has_direction)


def compute_vehicle_fields(df: pl.DataFrame) -> pl.DataFrame:
    """`vehicleSet`, read from the named parameters of the measure (plan §4.2).

    Produces `vehicle_all_vehicles`, `vehicle_restricted_types`, `vehicle_exempted_types`,
    `vehicle_heavyweight_max_weight`, `vehicle_max_length` / `_height` / `_width`,
    `vehicle_other_restricted_type_text`, `vehicle_other_exempted_type_text`.
    Drops nothing.

    `véhicule concerné (1..3)` and `valeur de la limite` say who is restricted;
    `dérogation pour véhicule`, `dérogation pour usager` and `Dérogations véhicules` say who
    is exempted. Values are multi-valued, joined by "; ", and are split before mapping.
    `caractère aggravant` is skipped (it qualifies the offence, not the vehicle);
    `Jours et Horaires` is skipped too but counted, because the pivot has no time slot and
    dropping the slot silently widens the measure to the whole day (plan §4.3).

    `allVehicles = true` only when nothing restricts (R-34) — **except** on
    `limitation dimensionnelle` and `limitation catégorielle`, where an empty vehicle set
    would turn a gauge limit into a road closed to everyone (R-30, review B-2). Those get
    `other` + the measure label as free text instead. A label the tables do not know also
    becomes `other` + that label: readable, and no threshold invented (R-35).
    """
    restricted_types: list[list[str] | None] = []
    exempted_types: list[list[str] | None] = []
    all_vehicles: list[bool] = []
    weights: list[float | None] = []
    lengths: list[float | None] = []
    heights: list[float | None] = []
    widths: list[float | None] = []
    other_restricted: list[str | None] = []
    other_exempted: list[str | None] = []

    unknown_restrictions: Counter[str] = Counter()
    unknown_exemptions: Counter[str] = Counter()
    time_slots_dropped = 0
    forced_other = 0

    for measure_label, params in zip(df.get_column("m_type"), df.get_column("m_params").to_list()):
        restricted: list[str] = []
        exempted: list[str] = []
        restricted_texts: list[str] = []
        exempted_texts: list[str] = []
        weight = length = height = width = None

        for pair in params or []:
            if len(pair) != 2:
                continue
            name = pair[0].strip()
            if name in IGNORED_PARAMETERS:
                continue
            if name in TIME_SLOT_PARAMETERS:
                if pair[1].strip():
                    time_slots_dropped += 1
                continue
            if name not in RESTRICTION_PARAMETERS and name not in EXEMPTION_PARAMETERS:
                continue
            for part in pair[1].split(MULTI_VALUE_SEPARATOR):
                label = part.strip()
                if not label:
                    continue
                if name in RESTRICTION_PARAMETERS:
                    if label in ALL_VEHICLES_LABELS:
                        continue
                    known = RESTRICTION_BY_LABEL.get(label)
                    if known is None:
                        unknown_restrictions[label] += 1
                        _append_unique(restricted, RESTRICTED_OTHER)
                        _append_unique(restricted_texts, label)
                        continue
                    _append_unique(restricted, known.restricted_type)
                    weight = _keep_stricter(weight, known.heavyweight_max_weight)
                    length = _keep_stricter(length, known.max_length)
                    height = _keep_stricter(height, known.max_height)
                    width = _keep_stricter(width, known.max_width)
                else:
                    known_exemption = EXEMPTION_BY_LABEL.get(label)
                    if known_exemption is None:
                        unknown_exemptions[label] += 1
                        _append_unique(exempted, EXEMPTED_OTHER)
                        _append_unique(exempted_texts, label)
                        continue
                    _append_unique(exempted, known_exemption)

        if not restricted and measure_label in (LABEL_DIMENSION_LIMIT, LABEL_CATEGORY_LIMIT):
            # Never a closure of the whole road for a gauge or a category we could not read.
            forced_other += 1
            restricted = [RESTRICTED_OTHER]
            restricted_texts = [measure_label]

        restricted_types.append(restricted or None)
        exempted_types.append(exempted or None)
        all_vehicles.append(not restricted)
        weights.append(weight)
        lengths.append(length)
        heights.append(height)
        widths.append(width)
        other_restricted.append(
            _truncate(FREE_TEXT_SEPARATOR.join(restricted_texts), OTHER_TYPE_TEXT_MAX_LENGTH)
            if RESTRICTED_OTHER in restricted
            else None
        )
        other_exempted.append(
            _truncate(FREE_TEXT_SEPARATOR.join(exempted_texts), OTHER_TYPE_TEXT_MAX_LENGTH)
            if EXEMPTED_OTHER in exempted
            else None
        )

    if unknown_restrictions:
        logger.warning(
            f"{sum(unknown_restrictions.values())} restriction values outside the DiaLog "
            f"enumeration, kept as 'other' + free text: {dict(unknown_restrictions)}"
        )
    if unknown_exemptions:
        logger.warning(
            f"{sum(unknown_exemptions.values())} exemption values outside the DiaLog "
            f"enumeration, kept as 'other' + free text: {dict(unknown_exemptions)}"
        )
    if time_slots_dropped:
        logger.warning(
            f"Ignoring the 'Jours et Horaires' parameter on {time_slots_dropped} measures: "
            "the pivot has no time slot, so they are broadcast for the whole day (phase 2)"
        )
    if forced_other:
        logger.info(
            f"{forced_other} gauge or category measures had no readable vehicle value: "
            "restricted to 'other' + the measure label rather than to every vehicle"
        )

    return df.with_columns(
        [
            pl.Series("vehicle_all_vehicles", all_vehicles, dtype=pl.Boolean),
            pl.Series("vehicle_restricted_types", restricted_types, dtype=pl.List(pl.Utf8)),
            pl.Series("vehicle_exempted_types", exempted_types, dtype=pl.List(pl.Utf8)),
            pl.Series("vehicle_heavyweight_max_weight", weights, dtype=pl.Float64),
            pl.Series("vehicle_max_length", lengths, dtype=pl.Float64),
            pl.Series("vehicle_max_height", heights, dtype=pl.Float64),
            pl.Series("vehicle_max_width", widths, dtype=pl.Float64),
            pl.Series("vehicle_other_restricted_type_text", other_restricted, dtype=pl.Utf8),
            pl.Series("vehicle_other_exempted_type_text", other_exempted, dtype=pl.Utf8),
        ]
    )


def _append_unique(values: list[str], value: str) -> None:
    if value not in values:
        values.append(value)


def _keep_stricter(current: float | None, candidate: float | None) -> float | None:
    """Two thresholds on one measure: keep the lower one, which forbids the most."""
    if candidate is None:
        return current
    if current is None:
        return candidate
    return min(current, candidate)


def compute_period_fields(df: pl.DataFrame, today: date | None = None) -> pl.DataFrame:
    """`period_*`, from the dates of the regulation — measures carry none of their own.

    Produces `period_start_date`, `period_end_date`, `period_recurrence_type`,
    `period_is_permanent`. Dates leave as ISO 8601 in French local time with the offset of
    the day, which is the only shape the API reads correctly.

    Temporary: `1109` at the first instant of its day, `1110` at the last second of its day,
    `is_permanent = False`. Permanent: no end date, `is_permanent = True`, and a start date
    taken from `1109`, else the signature date `1111`, else the day of `1196` "Modifié le" —
    never a constant (R-39). Each fallback is counted.

    Drops (each counted in a log line):

    * **the whole regulation**, every row of it, when a date control of R-38 fails —
      `début > fin`, `année(début) > année(today) + 1`, `année(fin) > année(today) + 5`.
      A typo on the year leaves the act "En vigueur" in Eudonet until that year comes
      (`2019T15796` runs to 2109), so the state alone cannot be trusted;
    * permanent regulations with no date at all, start, signature and modification alike.

    Temporary regulations lasting more than a year are **kept** and counted: on the
    2026-09-08 perimeter they are long works sites (tramway, RATP), not mistakes.
    """
    today = today or today_in_paris()
    max_start_year = today.year + 1
    max_end_year = today.year + 5

    invalid = (
        (pl.col("a_start_date") > pl.col("a_end_date"))
        | (pl.col("a_start_date").dt.year() > max_start_year)
        | (pl.col("a_end_date").dt.year() > max_end_year)
    ).fill_null(False)
    rejected = df.filter(invalid).get_column("a_identifier").unique().sort().to_list()
    if rejected:
        dropped = df.filter(pl.col("a_identifier").is_in(rejected))
        logger.warning(
            f"Dropping {dropped.height} rows of {len(rejected)} regulations failing the R-38 "
            f"date controls (start > end, start after {max_start_year}, end after "
            f"{max_end_year}): {rejected[:20]}"
        )
        df = df.filter(~pl.col("a_identifier").is_in(rejected))

    is_permanent = pl.col("a_type") == TYPE_PERMANENT_LABEL
    long_running = df.filter(
        ~is_permanent
        & ((pl.col("a_end_date") - pl.col("a_start_date")).dt.total_days() > 365).fill_null(False)
    )
    if long_running.height:
        logger.warning(
            f"{long_running['a_identifier'].n_unique()} temporary regulations last more than a "
            f"year ({long_running.height} rows): kept, but they widen the broadcast window"
        )

    df = df.with_columns(
        pl.when(~is_permanent)
        .then(pl.col("a_start_date"))
        .otherwise(
            pl.coalesce(
                pl.col("a_start_date"),
                pl.col("a_signed_at"),
                pl.col("a_modified_at").dt.date(),
            )
        )
        .alias(PERIOD_START_SOURCE)
    )

    fallbacks = df.filter(is_permanent & pl.col("a_start_date").is_null())
    if fallbacks.height:
        on_signature = fallbacks.filter(pl.col("a_signed_at").is_not_null()).height
        logger.warning(
            f"{fallbacks.height} permanent rows have no start date: {on_signature} fall back on "
            f"the signature date, {fallbacks.height - on_signature} on the modification date "
            "(R-39)"
        )

    undated = df.filter(pl.col(PERIOD_START_SOURCE).is_null())
    if undated.height:
        logger.warning(
            f"Dropping {undated.height} rows of {undated['a_identifier'].n_unique()} regulations "
            "with no usable start date at all (start, signature and modification all empty)"
        )
    df = df.filter(pl.col(PERIOD_START_SOURCE).is_not_null())

    if not df.height:
        return df.drop(PERIOD_START_SOURCE).with_columns(
            [
                pl.lit(None, dtype=pl.Utf8).alias("period_start_date"),
                pl.lit(None, dtype=pl.Utf8).alias("period_end_date"),
                pl.lit(None, dtype=pl.Utf8).alias("period_recurrence_type"),
                pl.lit(None, dtype=pl.Boolean).alias("period_is_permanent"),
            ]
        )

    return df.with_columns(
        [
            start_of_local_day(df, PERIOD_START_SOURCE).alias("period_start_date"),
            pl.when(is_permanent)
            .then(pl.lit(None, dtype=pl.Utf8))
            .otherwise(end_of_local_day(df, "a_end_date"))
            .alias("period_end_date"),
            pl.lit(PeriodRecurrenceTypeEnum.EVERYDAY.value).alias("period_recurrence_type"),
            is_permanent.alias("period_is_permanent"),
        ]
    ).drop(PERIOD_START_SOURCE)
