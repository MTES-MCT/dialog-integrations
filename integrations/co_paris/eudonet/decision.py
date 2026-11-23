from collections.abc import Sequence
from dataclasses import dataclass

import polars as pl

from .verdicts import LEVEL_KEY, STATUS, Check, Level, Status, check_of

OUTCOME = pl.Enum(["complete", "partial", "refused"])
ID_COLUMNS = ["a_file_id", "a_identifier", "m_file_id", "l_file_id"]
LOCATED = "m_located"


@dataclass(frozen=True)
class Decision:
    kept: pl.DataFrame
    removed: pl.DataFrame
    reasons: pl.DataFrame
    outcomes: pl.DataFrame
    checks: tuple[Check, ...]


def judged(
    rows: pl.DataFrame, checks: Sequence[Check] | None = None, named: Sequence[Check] = ()
) -> tuple[Check, ...]:
    """The checks whose verdict the rows carry, in judging order (the order of their columns)."""
    if checks is not None:
        given = {check.column: check for check in checks}
        return tuple(given[column] for column in rows.columns if column in given)
    names = {check.column: check for check in named}
    found = (names.get(column) or check_of(column) for column in rows.columns)
    return tuple(check for check in found if check is not None)


def refused_by(check: Check) -> pl.Expr:
    """Rows a refusal of `check` takes away: the refused record and all it holds."""
    hit = (pl.col(check.column).struct.field("status") == Status.REFUSED.value).fill_null(False)
    if check.refuses is Level.LOCATION:
        return hit
    key = LEVEL_KEY[check.refuses]
    return pl.when(pl.col(key).is_null()).then(hit).otherwise(hit.any().over(key))


def alive_now(rows: pl.DataFrame) -> pl.Series:
    """Rows no verdict judged so far refuses."""
    refusals = [refused_by(check) for check in judged(rows)]
    if not refusals:
        return pl.Series("alive", [True] * rows.height, dtype=pl.Boolean)
    return rows.select(~pl.any_horizontal(refusals)).to_series().alias("alive")


def decide(
    rows: pl.DataFrame, checks: Sequence[Check] | None = None, named: Sequence[Check] = ()
) -> Decision:
    """Drop the rows a verdict refuses, with every reason."""
    applied = judged(rows, checks, named)
    rows = _unjudged_where_not_located(rows, applied)
    order = {check.code: index for index, check in enumerate(applied)}
    marked = rows.with_columns(
        [refused_by(check).alias(f"_refused_{i}") for i, check in enumerate(applied)]
    ).with_columns(
        _primary=pl.coalesce(
            [
                pl.when(pl.col(f"_refused_{i}")).then(pl.lit(check.code))
                for i, check in enumerate(applied)
            ]
        )
        if applied
        else pl.lit(None, dtype=pl.String)
    )
    helpers = [f"_refused_{i}" for i in range(len(applied))] + ["_primary"]
    kept = marked.filter(pl.col("_primary").is_null()).drop(helpers)
    removed = marked.filter(pl.col("_primary").is_not_null()).select(
        *ID_COLUMNS, pl.col("_primary").alias("primary")
    )
    return Decision(
        kept=kept,
        removed=removed,
        reasons=_reasons(marked, applied),
        outcomes=_outcomes(marked, order),
        checks=applied,
    )


def _unjudged_where_not_located(rows: pl.DataFrame, applied: tuple[Check, ...]) -> pl.DataFrame:
    location_checks = [check for check in applied if check.judged_on is Level.LOCATION]
    if LOCATED not in rows.columns or not location_checks:
        return rows
    unjudged = pl.struct(
        status=pl.lit(Status.UNJUDGED.value).cast(STATUS), detail=pl.lit(None, dtype=pl.String)
    )
    unloaded = pl.col(LOCATED).not_().fill_null(False)
    return rows.with_columns(
        pl.when(unloaded & pl.col(check.column).is_not_null())
        .then(unjudged)
        .otherwise(pl.col(check.column))
        .alias(check.column)
        for check in location_checks
    )


REASON_SCHEMA = {
    "a_file_id": pl.Int64,
    "a_identifier": pl.String,
    "level": pl.String,
    "record": pl.Int64,
    "code": pl.String,
    "label": pl.String,
    "nature": pl.String,
    "status": pl.String,
    "detail": pl.String,
    "primary": pl.Boolean,
}


def _reasons(marked: pl.DataFrame, applied: tuple[Check, ...]) -> pl.DataFrame:
    frames = []
    for check in applied:
        status = pl.col(check.column).struct.field("status")
        hits = marked.filter(status.is_in([Status.REFUSED.value, Status.FLAGGED.value]))
        frame = hits.select(
            "a_file_id",
            "a_identifier",
            pl.lit(check.judged_on.value).alias("level"),
            pl.col(LEVEL_KEY[check.judged_on]).alias("record"),
            pl.lit(check.code).alias("code"),
            pl.lit(check.label).alias("label"),
            pl.lit(check.nature.value).alias("nature"),
            status.cast(pl.String).alias("status"),
            pl.col(check.column).struct.field("detail").alias("detail"),
            (pl.col("_primary") == check.code).fill_null(False).alias("primary"),
        )
        if check.judged_on is not Level.LOCATION:
            frame = frame.group_by(
                "a_file_id", "a_identifier", "level", "record", "code", maintain_order=True
            ).agg(pl.exclude("primary").first(), pl.col("primary").any())
        frames.append(frame.select(list(REASON_SCHEMA)).cast(REASON_SCHEMA))  # type: ignore[arg-type]
    return pl.concat(frames) if frames else pl.DataFrame(schema=REASON_SCHEMA)


def _outcomes(marked: pl.DataFrame, order: dict[str, int]) -> pl.DataFrame:
    rank = pl.col("_primary").replace_strict(order, default=None, return_dtype=pl.Int64)
    codes = dict(enumerate(order))
    return (
        marked.with_columns(_rank=rank)
        .group_by("a_file_id", maintain_order=True)
        .agg(
            pl.col("a_identifier").first(),
            pl.col("_primary").is_null().sum().alias("_kept"),
            pl.len().alias("_rows"),
            pl.col("_rank").max().alias("_last"),
        )
        .select(
            "a_file_id",
            "a_identifier",
            pl.when(pl.col("_kept") == pl.col("_rows"))
            .then(pl.lit("complete"))
            .when(pl.col("_kept") > 0)
            .then(pl.lit("partial"))
            .otherwise(pl.lit("refused"))
            .cast(OUTCOME)
            .alias("outcome"),
            pl.when(pl.col("_kept") == 0)
            .then(pl.col("_last").replace_strict(codes, default=None, return_dtype=pl.String))
            .alias("emptied_by"),
        )
    )
