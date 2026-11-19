"""A one-way street is published with every exception its act gives it; one that cannot be
read, and it is not published (decided 2026-11-19).

Other measures of the same act, on the same street, may open the way back: a bus or cycle lane
against the flow, a meeting zone (the highway code lets cycles ride both ways in one, unless a
measure says otherwise). DiaLog publishes none of them: they are read here, before being
dropped. A measure that covers the one-way section adds its users as exemptions; one on another
section of the street, or whose way or users cannot be read, drops the one-way section, since
how much of it the measure opens is not known. A restored two-way traffic on the street
contradicts it and drops it too.
"""

from collections import Counter, defaultdict
from dataclasses import dataclass

import polars as pl
from loguru import logger

from .counts import emptied_regulations
from .locations import SCOPE_WHOLE_STREET, street_key
from .vocabulary import (
    CYCLES_EXEMPTION_LABEL,
    EXEMPTION_BY_LABEL,
    EXEMPTION_PARAMETERS,
    LABEL_BOTH_WAYS_RESTORED,
    LABEL_CYCLE_LANE,
    LABEL_GREENWAY,
    LABEL_MEETING_ZONE,
    LABEL_NO_CONTRAFLOW_CYCLES,
    LABEL_ONE_WAY,
    LANE_BOTH_WAYS,
    LANE_DIRECTION_PARAMETER,
    LANE_LABELS,
    LANE_OTHER_WAY,
    LANE_SAME_WAY,
    LANE_TWO_WAY_VALUE,
    LANE_USERS_BY_LABEL,
    LANE_USERS_PARAMETER,
    MEASURE_TYPE_BY_LABEL,
    MULTI_VALUE_SEPARATOR,
    ONE_WAY_FROM_COMPLEMENTS,
    ONE_WAY_TO_COMPLEMENTS,
    PARAGRAPH_EXEMPTION_PARAMETER,
)

_PUBLISHED = pl.col("m_type").is_in(
    [label for label, kind in MEASURE_TYPE_BY_LABEL.items() if kind is not None]
)
RELATED_LABELS = LANE_LABELS | {
    LABEL_MEETING_ZONE,
    LABEL_NO_CONTRAFLOW_CYCLES,
    LABEL_BOTH_WAYS_RESTORED,
}
_COLUMNS = [
    "a_file_id",
    "m_file_id",
    "m_type",
    "m_params",
    "l_file_id",
    "l_scope",
    "l_road_name",
    "l_direction",
    "l_from_number_complement",
    "l_from_road_name",
    "l_from_house_number",
    "l_to_number_complement",
    "l_to_road_name",
    "l_to_house_number",
]


@dataclass(frozen=True)
class Section:
    street: str | None
    start: str | None
    end: str | None
    whole: bool
    # « depuis A vers B »: the bounds say which way.
    oriented: bool

    def covers(self, other: "Section") -> bool:
        if self.whole:
            return True
        return (
            self.start is not None
            and self.end is not None
            and {self.start, self.end} == {other.start, other.end}
        )


def _bound(road: str | None, number: str | None) -> str | None:
    if road and road.strip():
        return street_key(road)
    if number and number.strip():
        return f"n° {number.strip()}"
    return None


def _section(row: dict) -> Section:
    return Section(
        street=street_key(row["l_road_name"]) if row["l_road_name"] else None,
        start=_bound(row["l_from_road_name"], row["l_from_house_number"]),
        end=_bound(row["l_to_road_name"], row["l_to_house_number"]),
        whole=row["l_scope"] == SCOPE_WHOLE_STREET,
        oriented=row["l_from_number_complement"] in ONE_WAY_FROM_COMPLEMENTS
        and row["l_to_number_complement"] in ONE_WAY_TO_COMPLEMENTS,
    )


def _values(params: list[list[str]] | None, names: frozenset[str] | set[str]) -> list[str]:
    return [
        " ".join(part.split())
        for name, value in (pair for pair in params or [] if len(pair) == 2)
        if name.strip() in names
        for part in value.split(MULTI_VALUE_SEPARATOR)
        if part.strip()
    ]


def lane_users(label: str, params: list[list[str]] | None) -> list[str] | None:
    """Who may use the lane, as exemption labels; `None` if one of them is not known."""
    users: list[str] = []
    if label in (LABEL_CYCLE_LANE, LABEL_GREENWAY):
        users.append(CYCLES_EXEMPTION_LABEL)
    for value in _values(params, {LANE_USERS_PARAMETER}):
        known = LANE_USERS_BY_LABEL.get(value)
        if known is None:
            return None
        users += known
    users += _values(params, EXEMPTION_PARAMETERS)
    return list(dict.fromkeys(users)) or None


def _lane_way(lane: dict, section: Section, one_way: Section) -> str | None:
    """`same`, `other` or `both`, against the one-way street it covers; `None` if unknown."""
    if lane["l_direction"] == LANE_SAME_WAY:
        return "same"
    if lane["l_direction"] == LANE_OTHER_WAY:
        return "other"
    if lane["l_direction"] == LANE_BOTH_WAYS or LANE_TWO_WAY_VALUE in _values(
        lane["m_params"], {LANE_DIRECTION_PARAMETER}
    ):
        return "both"
    if section.whole or not (section.oriented and one_way.oriented):
        return None
    if (section.start, section.end) == (one_way.end, one_way.start):
        return "other"
    if (section.start, section.end) == (one_way.start, one_way.end):
        return "same"
    return None


def _has_cycles(params: list[list[str]] | None) -> bool:
    return any(
        EXEMPTION_BY_LABEL.get(value) == EXEMPTION_BY_LABEL[CYCLES_EXEMPTION_LABEL]
        for value in _values(params, EXEMPTION_PARAMETERS)
    )


def _judge(one_way: dict, related: list[dict]) -> tuple[list[str], str | None]:
    """The exemptions the act's other measures give the one-way section, or why it is dropped."""
    section = _section(one_way)
    added: list[str] = []
    meeting_zone = False
    no_contraflow_cycles = False
    for row in related:
        other = _section(row) if row["l_file_id"] is not None else None
        if other is None or other.street is None:
            return [], f"{row['m_type']} of the act without a street"
        if other.street != section.street:
            continue
        label = row["m_type"]
        if label == LABEL_BOTH_WAYS_RESTORED:
            return [], "two-way traffic restored on the street"
        covers = other.covers(section)
        if label == LABEL_NO_CONTRAFLOW_CYCLES:
            if not covers:
                return [], "cycles kept from the contraflow on another section"
            no_contraflow_cycles = True
            continue
        if not covers:
            return [], f"{label} on another section of the street"
        if label == LABEL_MEETING_ZONE:
            meeting_zone = True
            continue
        way = _lane_way(row, other, section)
        if way is None:
            return [], f"{label} whose way is not known"
        if way == "same":
            continue
        users = lane_users(label, row["m_params"])
        if users is None:
            return [], f"{label} whose users are not known"
        added += users
    if no_contraflow_cycles:
        if _has_cycles(one_way["m_params"]) or CYCLES_EXEMPTION_LABEL in added:
            return [], "cycles both allowed and kept from the contraflow"
    elif meeting_zone:
        added.append(CYCLES_EXEMPTION_LABEL)
    return list(dict.fromkeys(added)), None


def arbitrate_one_way_streets(df: pl.DataFrame) -> pl.DataFrame:
    """Give each one-way section the exemptions its act's lanes and meeting zones give it, or
    drop it; a measure whose sections end up with different exemptions is split, one DiaLog
    measure each."""
    rows = df.select(_COLUMNS).iter_rows(named=True)
    related_by_act: dict[int, list[dict]] = defaultdict(list)
    one_ways: list[dict] = []
    for row in rows:
        if row["m_type"] in RELATED_LABELS:
            related_by_act[row["a_file_id"]].append(row)
        elif row["m_type"] == LABEL_ONE_WAY and row["l_file_id"] is not None:
            one_ways.append(row)

    added: dict[int, list[str]] = {}
    reasons: dict[int, str] = {}
    for one_way in one_ways:
        related = related_by_act.get(one_way["a_file_id"])
        if not related:
            continue
        exemptions, reason = _judge(one_way, related)
        if reason is not None:
            reasons[one_way["l_file_id"]] = reason
        elif exemptions:
            added[one_way["l_file_id"]] = exemptions

    is_dropped = pl.col("l_file_id").is_in(list(reasons)) & (pl.col("m_type") == LABEL_ONE_WAY)
    dropped = df.filter(is_dropped)
    if dropped.height:
        logger.warning(
            f"Dropping {dropped.height} one-way street locations of "
            f"{dropped['a_file_id'].n_unique()} regulations that another measure of the act "
            f"may open the other way: {dict(Counter(reasons.values()).most_common())}; "
            f"{emptied_regulations(df.filter(_PUBLISHED), is_dropped)} regulations left with "
            "nothing published"
        )
    df = df.filter(~is_dropped.fill_null(False))
    if not added:
        return df

    one_way = pl.col("m_type") == LABEL_ONE_WAY
    params = [
        [*(measure_params or []), *([PARAGRAPH_EXEMPTION_PARAMETER, x] for x in added[location])]
        if is_one_way and location in added
        else measure_params
        for location, measure_params, is_one_way in df.select(
            "l_file_id", "m_params", one_way.alias("_one_way")
        ).iter_rows()
    ]
    exemption_set = pl.Series(
        [
            "|".join(added.get(location, [])) if is_one_way else ""
            for location, is_one_way in df.select("l_file_id", one_way).iter_rows()
        ]
    )
    logger.info(
        f"{len(added)} one-way street locations of "
        f"{df.filter(pl.col('l_file_id').is_in(list(added)))['a_file_id'].n_unique()} "
        "regulations exempt the users of a lane or meeting zone of the act going the other way"
    )
    # Keyed by the exemptions themselves: the same from one night to the next.
    return (
        df.with_columns(
            pl.Series("m_params", params, dtype=df.schema["m_params"]), _set=exemption_set
        )
        .with_columns(
            pl.when(pl.col("_set").n_unique().over("m_file_id") > 1)
            .then(pl.format("{}-{}", pl.col("measure_group_key"), pl.col("_set")))
            .otherwise(pl.col("measure_group_key"))
            .alias("measure_group_key")
        )
        .drop("_set")
    )
