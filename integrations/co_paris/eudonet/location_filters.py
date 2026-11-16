import json
import re
from collections import Counter
from pathlib import Path

import polars as pl
from loguru import logger

from api.dia_log_client.models import MeasureTypeEnum

from .counts import count_by, emptied_regulations
from .locations import BOTH_DIRECTIONS_LABEL, ONE_SIDE_LABELS, SCOPE_POINT
from .text import plain_text
from .vocabulary import (
    COMPLEMENT_CONNECTORS,
    FACING_NUMBER_COMPLEMENT,
    LABEL_ONE_WAY,
    LABEL_PEDESTRIAN_AREA,
    NEUTRAL_FROM_NUMBER_COMPLEMENTS,
    NEUTRAL_POINT_NUMBER_COMPLEMENTS,
    NEUTRAL_TO_NUMBER_COMPLEMENTS,
    TOWARDS_NUMBER_COMPLEMENT,
    WHOLE_LOCATION_COMPLEMENTS,
)

# DiaLog refuses a whole regulation over one location it cannot geocode, without saying which
# (13 % of the regulations on staging, 2026-09-23); the list is kept until probed again.
REFUSED_LOCATIONS_PATH = Path(__file__).with_name("refused_locations.json")
# A temporary filter, to switch off the day the back drops the location it cannot
# geocode instead of refusing the whole regulation.
DROP_REFUSED_LOCATIONS = True

LOCATION_COMPLEMENT_COLUMNS = (
    "l_from_location_complement",
    "l_to_location_complement",
    "l_point_location_complement",
)
BOUND_QUALIFIER_COLUMNS = (
    "l_from_number_complement",
    "l_to_number_complement",
    "l_point_number_complement",
    "l_from_side",
    "l_to_side",
    "l_from_direction",
    "l_to_direction",
)
# Where the numbers of the bounds are written: a number in a complement must be one of them.
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
_SEPARATORS_BUT_SLASH = r"\s,.;:()&\-–"
_COMPLEMENT_WORD = re.compile(rf"[^{_SEPARATORS_BUT_SLASH}]+")
_HOUSE_NUMBER_WORD = re.compile(r"(?:n°?|no)?(\d+)(?:bis|ter|quater|(?!h)[a-z])?")
_DIGITS = re.compile(r"\d+")


def _complement_says_nothing(text: str | None, bound_numbers: frozenset[str]) -> bool:
    """True for an empty complement, a "whole street" formula, or connectors around numbers
    the bounds already carry ("n° 21 et du n° 47" on point 21 says more: two places)."""
    if not text:
        return True
    plain = re.sub(r"[\s,.;:()]+$|^[\s,.;:()]+", "", plain_text(text)).lower()
    if plain in WHOLE_LOCATION_COMPLEMENTS:
        return True
    for word in _COMPLEMENT_WORD.findall(plain):
        if word in COMPLEMENT_CONNECTORS:
            continue
        number = _HOUSE_NUMBER_WORD.fullmatch(word)
        if number is None or str(int(number.group(1))) not in bound_numbers:
            return False
    return True


def _bound_numbers(row: dict) -> frozenset[str]:
    return frozenset(
        str(int(digits))
        for column in BOUND_NUMBER_COLUMNS
        for digits in _DIGITS.findall(row[column] or "")
    )


def discard_location_complements(df: pl.DataFrame) -> pl.DataFrame:
    """Drop the locations whose free-text complement narrows or qualifies the measure:
    published as is, the measure would cover the whole section, every day."""
    rows = df.select(*LOCATION_COMPLEMENT_COLUMNS, *BOUND_NUMBER_COLUMNS).iter_rows(named=True)
    says_more_flags = []
    heads: Counter[str] = Counter()
    for row in rows:
        numbers = _bound_numbers(row)
        text = next(
            (
                row[column]
                for column in LOCATION_COMPLEMENT_COLUMNS
                if not _complement_says_nothing(row[column], numbers)
            ),
            None,
        )
        says_more_flags.append(text is not None)
        if text is not None:
            heads[" ".join(plain_text(text).split()[:3]).lower()] += 1
    says_more = pl.Series(says_more_flags, dtype=pl.Boolean)
    dropped = df.filter(says_more)
    if dropped.height:
        logger.warning(
            f"Dropping {dropped.height} locations whose complement narrows the measure: "
            f"{dict(heads.most_common(10))}; {dropped['a_identifier'].n_unique()} regulations "
            f"published without them, {emptied_regulations(df, pl.lit(says_more))} left with "
            "no location"
        )
    return df.filter(~says_more)


def _bound_qualifier(row: dict) -> str | None:
    """Why a bound's number complement, side or direction keeps the location out, if it does."""
    parking = row["measure_type_"] == MeasureTypeEnum.PARKINGPROHIBITED.value
    by_measure = set() if parking else {FACING_NUMBER_COMPLEMENT}
    if parking or row["m_type"] in (LABEL_PEDESTRIAN_AREA, LABEL_ONE_WAY):
        # On a one-way street "vers" is the direction itself, read by `orient_one_way_streets`.
        by_measure.add(TOWARDS_NUMBER_COMPLEMENT)
    for column, neutral in (
        ("l_from_number_complement", NEUTRAL_FROM_NUMBER_COMPLEMENTS | by_measure),
        ("l_to_number_complement", NEUTRAL_TO_NUMBER_COMPLEMENTS | by_measure),
        ("l_point_number_complement", NEUTRAL_POINT_NUMBER_COMPLEMENTS | by_measure),
    ):
        if row[column] and row[column] not in neutral:
            return f"number complement {row[column]!r}"
    for column in ("l_from_side", "l_to_side"):
        if row[column] in ONE_SIDE_LABELS:
            return "one side of the street at a bound"
    for column in ("l_from_direction", "l_to_direction"):
        if row[column] and row[column] != BOTH_DIRECTIONS_LABEL:
            return "one direction at a bound"
    return None


def discard_bound_qualifiers(df: pl.DataFrame) -> pl.DataFrame:
    """Drop the locations whose bound says more than where it is: the opposite side of a
    parking ban ("en vis à vis du"), a direction of a closure or a speed limit ("vers"), one
    side, one direction, or a catalog value never read."""
    reasons = [
        _bound_qualifier(row)
        for row in df.select(*BOUND_QUALIFIER_COLUMNS, "measure_type_", "m_type").iter_rows(
            named=True
        )
    ]
    qualified = pl.Series([reason is not None for reason in reasons], dtype=pl.Boolean)
    dropped = df.filter(qualified)
    if dropped.height:
        counts = Counter(reason for reason in reasons if reason is not None)
        logger.warning(
            f"Dropping {dropped.height} locations whose bound qualifies the measure: "
            f"{dict(counts.most_common())}; {dropped['a_identifier'].n_unique()} regulations "
            f"published without them, {emptied_regulations(df, pl.lit(qualified))} left with "
            "no location"
        )
    return df.filter(~qualified)


def discard_points_beyond_parking(df: pl.DataFrame) -> pl.DataFrame:
    """Drop a single-point location that carries anything but a parking ban.

    On one house number DiaLog draws a point a GPS cannot follow (decided 2026-10-26).
    """
    beyond = (pl.col("l_scope") == SCOPE_POINT) & (
        pl.col("measure_type_") != MeasureTypeEnum.PARKINGPROHIBITED.value
    )
    dropped = df.filter(beyond)
    if dropped.height:
        logger.warning(
            f"Dropping {dropped.height} single-point locations of measures other than a "
            f"parking ban: {count_by(dropped, 'measure_type_')}; "
            f"{emptied_regulations(df, beyond)} regulations left with no location"
        )
    return df.filter(~beyond)


def load_refused_locations(path: Path = REFUSED_LOCATIONS_PATH) -> dict[int, str]:
    """Eudonet location `FileId` → the motive DiaLog gave when it refused it alone."""
    entries = json.loads(path.read_text(encoding="utf-8"))["locations"]
    return {entry["l_file_id"]: entry["motive"] for entry in entries}


def discard_refused_locations(df: pl.DataFrame) -> pl.DataFrame:
    """Drop the locations DiaLog refuses to geocode, so that their regulation goes through.

    Counted by motive. A regulation whose every location is listed goes away with them.
    """
    if not DROP_REFUSED_LOCATIONS:
        return df
    refused = load_refused_locations()
    if not refused:
        return df
    listed = pl.col("l_file_id").is_in(list(refused))
    dropped = df.filter(listed)
    if dropped.height:
        motives = Counter(refused[file_id] for file_id in dropped["l_file_id"])
        logger.warning(
            f"Dropping {dropped.height} locations DiaLog refuses to geocode (blacklist): "
            f"{dict(motives.most_common())}; {dropped['a_identifier'].n_unique()} regulations "
            f"published without them, {emptied_regulations(df, listed)} left with no location"
        )
    return df.filter(~listed)
