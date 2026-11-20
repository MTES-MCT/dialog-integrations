"""What the decision dropped, logged by check: the count of each check's primary reasons, and
of the records it refuses too behind an earlier one (for the GitHub summary; Tchap keeps its
totals)."""

from collections import Counter
from typing import TYPE_CHECKING

import polars as pl
from loguru import logger

from .verdicts import Check, Level, Status

if TYPE_CHECKING:
    from .decision import Decision


def report(decision: "Decision") -> None:
    for check in decision.checks:
        if check.reported:
            _report_check(decision, check)
    outcomes = dict(decision.outcomes["outcome"].cast(pl.String).value_counts().iter_rows())
    logger.info(
        f"Decision on {decision.outcomes.height} regulations: {outcomes.get('complete', 0)} "
        f"published whole, {outcomes.get('partial', 0)} in part, "
        f"{outcomes.get('refused', 0)} refused"
    )


def _report_check(decision: "Decision", check: Check) -> None:
    removed = decision.removed.filter(pl.col("primary") == check.code)
    behind = _behind(decision, check)
    if not removed.height:
        if behind:
            logger.info(f"Nothing dropped first for {check.code}{behind}")
        return
    reasons = decision.reasons.filter((pl.col("code") == check.code) & pl.col("primary"))
    details = Counter(d for d in reasons["detail"] if d is not None)
    said = f": {dict(details.most_common())}" if details else ""
    emptied = decision.outcomes.filter(pl.col("emptied_by") == check.code).height
    if check.refuses is Level.ACT:
        logger.warning(f"Dropping {removed['a_file_id'].n_unique()} {check.logged}{said}{behind}")
        return
    if check.refuses is Level.MEASURE:
        # A row without a measure is a record of its own.
        measures = removed["m_file_id"].drop_nulls().n_unique() + removed["m_file_id"].null_count()
        logger.warning(
            f"Dropping {measures} {check.logged} "
            f"({reasons.height} such {check.judged_on.value}s, {removed.height} rows){said}; "
            f"{emptied} regulations left with no measure{behind}"
        )
        return
    touched = removed["a_file_id"].unique().to_list()
    published = decision.outcomes.filter(
        pl.col("a_file_id").is_in(touched) & (pl.col("outcome") != "refused")
    ).height
    logger.warning(
        f"Dropping {removed.height} {check.logged}{said}; {published} regulations published "
        f"without them, {emptied} left with no location{behind}"
    )


def _behind(decision: "Decision", check: Check) -> str:
    """The records `check` refuses too, an earlier check having refused them first."""
    also = decision.reasons.filter(
        (pl.col("code") == check.code)
        & (pl.col("status") == Status.REFUSED.value)
        & ~pl.col("primary")
    ).height
    return (
        f"; {also} more {check.judged_on.value}s refused behind an earlier reason" if also else ""
    )
