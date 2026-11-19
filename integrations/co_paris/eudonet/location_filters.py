import json
import re
import unicodedata
from collections import Counter
from functools import lru_cache
from pathlib import Path

import polars as pl
from loguru import logger

from api.dia_log_client.models import MeasureTypeEnum

from .counts import count_by, emptied_regulations
from .locations import BOTH_DIRECTIONS_LABEL, ONE_SIDE_LABELS, SCOPE_POINT, WITHDRAWN_STATUSES
from .text import plain_text
from .vocabulary import (
    FACING_NUMBER_COMPLEMENT,
    LABEL_ONE_WAY,
    LABEL_PEDESTRIAN_AREA,
    NEUTRAL_FROM_NUMBER_COMPLEMENTS,
    NEUTRAL_POINT_NUMBER_COMPLEMENTS,
    NEUTRAL_TO_NUMBER_COMPLEMENTS,
    TOWARDS_NUMBER_COMPLEMENT,
    TYPE_TEMPORARY_LABEL,
)

# `2768` "Statut Localisation" of a location entered for the act; one copied with an older
# act's record keeps "Maintenue" (or "Modifié", or nothing).
ADDED_STATUS = "Ajoutée"


def _words(text: str) -> set[str]:
    """Lowercase words without accents, punctuation or plural: "Pillet-Will" = "pillet will",
    "Staël" = "stael", "Ier" = "1er" = "1"."""
    ascii_text = unicodedata.normalize("NFKD", text.lower()).encode("ascii", "ignore").decode()
    return {_bare_word(word) for word in re.sub(r"[^a-z0-9]+", " ", ascii_text).split()}


def _bare_word(word: str) -> str:
    if word in ("1er", "ier"):
        return "1"
    return word[:-1] if len(word) > 3 and word[-1] in "sx" else word


# Words too common in street names to tell one street from another in a title.
_GENERIC_STREET_WORDS = _words(
    "rue avenue boulevard bd bld av place quai impasse passage allée square cité villa chemin "
    "route porte pont cours sentier voie galerie rond point carrefour parvis promenade "
    "esplanade jardin port hameau cour ruelle sente terrasse "
    "le la les l de du des d à au aux et en sur "
    "saint sainte st ste général maréchal docteur professeur président "
    "jean pierre paul louis jacques charles françois henri marie joseph georges andré victor"
)


@lru_cache(maxsize=None)
def _title_words(title_html: str) -> frozenset[str]:
    return frozenset(_words(plain_text(title_html)))


def _title_names_street(title_html: str | None, road_name: str) -> bool:
    """Whether the title holds a word that singles the street out. A name made of common
    words only cannot be judged, and is said named."""
    distinctive = {w for w in _words(road_name) - _GENERIC_STREET_WORDS if len(w) > 1}
    return not distinctive or bool(distinctive & _title_words(title_html or ""))


def discard_measures_with_copied_locations(df: pl.DataFrame) -> pl.DataFrame:
    """Drop a temporary act's measure holding a location copied from an older act and never
    replaced: its status is not "Ajoutée" and the title does not name its street.

    That street is the older act's, often a kilometre away or more, and the same location
    travels from act to act (one point in 141 acts of a year, 2026-11-19). The title is the
    one text written for the new act that the API reads: a street the signed PDF alone names
    goes too (1 of the 37 locations it drops out of 2 983, 2026-11-19).
    """
    rows = df.select("a_type", "a_title_html", "l_status", "l_road_name").iter_rows()
    copied = pl.Series(
        [
            a_type == TYPE_TEMPORARY_LABEL
            and status != ADDED_STATUS
            and status not in WITHDRAWN_STATUSES
            and road is not None
            and not _title_names_street(title, road)
            for a_type, title, status, road in rows
        ],
        dtype=pl.Boolean,
    )
    measures = df.filter(copied)["m_file_id"].unique().to_list()
    in_measure = pl.col("m_file_id").is_in(measures).fill_null(False)
    if measures:
        logger.warning(
            f"Dropping {len(measures)} measures of temporary acts holding a location copied "
            f"from an older act, its street absent from the title ({int(copied.sum())} such "
            f"locations, {df.filter(in_measure).height} rows); "
            f"{emptied_regulations(df, in_measure)} regulations left with no measure"
        )
    return df.filter(~in_measure)


# DiaLog refuses a whole regulation over one location it cannot geocode, without saying which
# (13 % of the regulations on staging, 2026-09-23); the list is kept until probed again.
REFUSED_LOCATIONS_PATH = Path(__file__).with_name("refused_locations.json")
# A temporary filter, to switch off the day the back drops the location it cannot
# geocode instead of refusing the whole regulation.
DROP_REFUSED_LOCATIONS = True

BOUND_QUALIFIER_COLUMNS = (
    "l_from_number_complement",
    "l_to_number_complement",
    "l_point_number_complement",
    "l_from_side",
    "l_to_side",
    "l_from_direction",
    "l_to_direction",
)


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
