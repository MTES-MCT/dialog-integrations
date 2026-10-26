import json
import re
from collections import Counter
from pathlib import Path

import polars as pl
from loguru import logger

from api.dia_log_client.models import MeasureTypeEnum

from .counts import count_by, emptied_regulations
from .locations import SCOPE_POINT
from .text import plain_text
from .vocabulary import COMPLEMENT_CONNECTORS, WHOLE_LOCATION_COMPLEMENTS

# DiaLog refuses a whole regulation when it cannot geocode one of its locations (13 % of
# the regulations sent to the staging on 2026-09-23), and its answer does not say which. A
# location fixed in Eudonet keeps its `FileId` and stays dropped until the list is probed
# again.
REFUSED_LOCATIONS_PATH = Path(__file__).with_name("refused_locations.json")
# R-05: a temporary filter, to switch off the day the back drops the location it cannot
# geocode instead of refusing the whole regulation.
DROP_REFUSED_LOCATIONS = True

LOCATION_COMPLEMENT_COLUMNS = (
    "l_from_location_complement",
    "l_to_location_complement",
    "l_point_location_complement",
)
_SEPARATORS_BUT_SLASH = r"\s,.;:()&\-–"
_COMPLEMENT_WORD = re.compile(rf"[^{_SEPARATORS_BUT_SLASH}]+")
_HOUSE_NUMBER_WORD = re.compile(r"(?:n°?|no)?\d+(?:bis|ter|quater|(?!h)[a-z])?")


def _complement_says_nothing(text: str | None) -> bool:
    if not text:
        return True
    plain = re.sub(r"[\s,.;:()]+$|^[\s,.;:()]+", "", plain_text(text)).lower()
    if plain in WHOLE_LOCATION_COMPLEMENTS:
        return True
    return all(
        word in COMPLEMENT_CONNECTORS or _HOUSE_NUMBER_WORD.fullmatch(word)
        for word in _COMPLEMENT_WORD.findall(plain)
    )


def discard_location_complements(df: pl.DataFrame) -> pl.DataFrame:
    """Drop the locations whose free-text complement narrows or qualifies the measure (R-81):
    published as is, the measure would cover the whole section, every day."""
    says_more = ~pl.all_horizontal(
        pl.col(column)
        .map_elements(_complement_says_nothing, return_dtype=pl.Boolean)
        .fill_null(True)
        for column in LOCATION_COMPLEMENT_COLUMNS
    )
    dropped = df.filter(says_more)
    if dropped.height:
        heads: Counter[str] = Counter()
        for row in dropped.select(LOCATION_COMPLEMENT_COLUMNS).iter_rows():
            text = next(t for t in row if not _complement_says_nothing(t))
            heads[" ".join(plain_text(text).split()[:3]).lower()] += 1
        logger.warning(
            f"Dropping {dropped.height} locations whose complement narrows the measure (R-81): "
            f"{dict(heads.most_common(10))}; {dropped['a_identifier'].n_unique()} regulations "
            f"published without them, {emptied_regulations(df, says_more)} left with no "
            "location"
        )
    return df.filter(~says_more)


def discard_points_beyond_parking(df: pl.DataFrame) -> pl.DataFrame:
    """Drop a single-point location that carries anything but a parking ban (R-74).

    A ban, a speed or a gauge stands on a stretch of road; on one house number DiaLog draws
    a point a GPS cannot follow (decision of 2026-10-26, Q-56).
    """
    beyond = (pl.col("l_scope") == SCOPE_POINT) & (
        pl.col("measure_type_") != MeasureTypeEnum.PARKINGPROHIBITED.value
    )
    dropped = df.filter(beyond)
    if dropped.height:
        logger.warning(
            f"Dropping {dropped.height} single-point locations of measures other than a "
            f"parking ban (R-74): {count_by(dropped, 'measure_type_')}; "
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
