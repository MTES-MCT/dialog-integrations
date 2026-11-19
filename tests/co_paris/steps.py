"""Test helpers: each step of the chain judged, then decided at once on its own checks, as the
step used to drop. The run decides once, at the end (`decision.decide`). The steps that read
the act's other records see every row alive."""

from datetime import date

import polars as pl

from integrations.co_paris.eudonet.decision import decide
from integrations.co_paris.eudonet.free_text_filters import (
    judge_location_details,
    judge_superseded_regulations,
    judge_unread_texts,
)
from integrations.co_paris.eudonet.location_filters import (
    judge_bound_qualifiers,
    judge_copied_locations,
    judge_points_beyond_parking,
    judge_refused_locations,
)
from integrations.co_paris.eudonet.locations import (
    judge_location_fields,
    judge_one_way_orientation,
)
from integrations.co_paris.eudonet.measures import judge_measure_fields, judge_out_of_model
from integrations.co_paris.eudonet.one_way_exceptions import (
    judge_one_way_lanes,
    split_one_way_measures,
)
from integrations.co_paris.eudonet.published_elsewhere import judge_published_elsewhere
from integrations.co_paris.eudonet.regulations import judge_period_fields, judge_regulation_fields
from integrations.co_paris.eudonet.report import report
from integrations.co_paris.eudonet.titles import judge_unread_titles
from integrations.co_paris.eudonet.vehicles import judge_vehicle_fields
from integrations.co_paris.eudonet.verdicts import (
    BOUND_QUALIFIER,
    CITED_BY_ANOTHER_ACT,
    COPIED_LOCATION,
    IMPLAUSIBLE_DATES,
    LANE_PARAMETER,
    LOCATION_DETAIL,
    LOCATION_REJECTED,
    NO_MEASURE,
    NO_NUMBER,
    ONE_WAY_LANE,
    ONE_WAY_UNREADABLE,
    OUT_OF_MODEL,
    OUT_OF_STATE,
    POINT_BEYOND_PARKING,
    PUBLISHED_ELSEWHERE,
    REFUSED_LOCATION,
    SOME_DAYS_ONLY,
    SPEED_UNREADABLE,
    SUPERSEDED_BY_LINK,
    TIME_SLOT,
    TITLE_CONDITION,
    TITLE_SAYS_REPEALED,
    UNEXPECTED_OPENING,
    UNKNOWN_MEASURE_LABEL,
    UNKNOWN_OPTION_FORMULA,
    UNKNOWN_PARAMETER,
    UNKNOWN_REGULATION_TYPE,
    UNREAD_ACT_TEXT,
    UNREAD_LOCATION_TEXT,
    UNREAD_MEASURE_TEXT,
    UNREAD_TITLE,
    VEHICLE_REJECTION,
    Check,
    Status,
)

MEASURE_CHECKS = (
    NO_MEASURE,
    UNKNOWN_MEASURE_LABEL,
    UNEXPECTED_OPENING,
    UNKNOWN_OPTION_FORMULA,
    SPEED_UNREADABLE,
    TIME_SLOT,
    LANE_PARAMETER,
    UNKNOWN_PARAMETER,
)
SUPERSEDED = (
    CITED_BY_ANOTHER_ACT,
    SUPERSEDED_BY_LINK,
    SOME_DAYS_ONLY,
    TITLE_SAYS_REPEALED,
    TITLE_CONDITION,
)
UNREAD_TEXTS = (UNREAD_ACT_TEXT, UNREAD_MEASURE_TEXT, UNREAD_LOCATION_TEXT)


def everyone(df: pl.DataFrame) -> pl.Series:
    return pl.Series([True] * df.height, dtype=pl.Boolean)


def drop_refused(rows: pl.DataFrame, *checks: Check) -> pl.DataFrame:
    """Decide on these checks alone and drop at once, logging as the run does."""
    decision = decide(rows, checks)
    report(decision)
    return decision.kept


def keep_unrefused(df: pl.DataFrame, check: Check) -> pl.DataFrame:
    """For a check that refuses the location alone: its rows, minus those it refuses."""
    return df.filter(pl.col(check.column).struct.field("status") != Status.REFUSED.value)


def details(df: pl.DataFrame, check: Check) -> list[str | None]:
    return [verdict and verdict["detail"] for verdict in df[check.column].to_list()]


def compute_regulation_fields(df: pl.DataFrame) -> pl.DataFrame:
    return drop_refused(
        judge_regulation_fields(df), NO_NUMBER, UNKNOWN_REGULATION_TYPE, OUT_OF_STATE
    )


def discard_superseded_regulations(df: pl.DataFrame) -> pl.DataFrame:
    return drop_refused(judge_superseded_regulations(df, everyone(df)), *SUPERSEDED)


def compute_measure_fields(df: pl.DataFrame) -> pl.DataFrame:
    return drop_refused(judge_measure_fields(df), *MEASURE_CHECKS)


def discard_unread_texts(df: pl.DataFrame, alerts: list[str] | None = None) -> pl.DataFrame:
    return drop_refused(judge_unread_texts(df, everyone(df), alerts), *UNREAD_TEXTS)


def discard_unread_titles(df: pl.DataFrame) -> pl.DataFrame:
    return drop_refused(judge_unread_titles(df, everyone(df)), UNREAD_TITLE)


def discard_measures_out_of_model(df: pl.DataFrame) -> pl.DataFrame:
    return drop_refused(judge_out_of_model(df), OUT_OF_MODEL)


def compute_vehicle_fields(df: pl.DataFrame) -> pl.DataFrame:
    return drop_refused(judge_vehicle_fields(df, everyone(df)), VEHICLE_REJECTION)


def compute_period_fields(df: pl.DataFrame, today: date) -> pl.DataFrame:
    return drop_refused(judge_period_fields(df, everyone(df), today), IMPLAUSIBLE_DATES)


def discard_bound_qualifiers(df: pl.DataFrame) -> pl.DataFrame:
    return drop_refused(judge_bound_qualifiers(df), BOUND_QUALIFIER)


def discard_location_details(df: pl.DataFrame) -> pl.DataFrame:
    return drop_refused(judge_location_details(df), LOCATION_DETAIL)


def compute_location_fields(df: pl.DataFrame) -> pl.DataFrame:
    return keep_unrefused(judge_location_fields(df), LOCATION_REJECTED)


def orient_one_way_streets(df: pl.DataFrame) -> pl.DataFrame:
    return drop_refused(judge_one_way_orientation(df), ONE_WAY_UNREADABLE)


def arbitrate_one_way_streets(df: pl.DataFrame) -> pl.DataFrame:
    kept = drop_refused(judge_one_way_lanes(df, everyone(df)), ONE_WAY_LANE)
    return split_one_way_measures(kept)


def discard_points_beyond_parking(df: pl.DataFrame) -> pl.DataFrame:
    return drop_refused(judge_points_beyond_parking(df), POINT_BEYOND_PARKING)


def discard_refused_locations(df: pl.DataFrame) -> pl.DataFrame:
    return drop_refused(judge_refused_locations(df), REFUSED_LOCATION)


def discard_measures_with_copied_locations(df: pl.DataFrame) -> pl.DataFrame:
    return drop_refused(judge_copied_locations(df, everyone(df)), COPIED_LOCATION)


def discard_published_elsewhere(df: pl.DataFrame, numbers: dict[str, str] | None):
    return drop_refused(judge_published_elsewhere(df, numbers), PUBLISHED_ELSEWHERE)
