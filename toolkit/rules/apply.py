"""Apply rules to a source's rows, all at once, on a canonical view of its frame.

The view is the frame plus the source's columns read as canonical ones (`rule_inputs`),
typed as the canonical model types them. It keeps the frame's rows in order, so each
verdict applies to the frame directly, at any step of the source's own pipeline.
"""

from dataclasses import dataclass, field
from types import UnionType
from typing import Literal, Union, get_args, get_origin, get_type_hints

import polars as pl
from loguru import logger

from canonical.regulation_measure import DEFECT_COLUMN, RegulationMeasure
from toolkit.rules.rule import Nature, Rule
from toolkit.transforms.local_time import PARIS, parse_instant

INSTANT = pl.Datetime("us", PARIS)
INSTANT_COLUMNS = {"period_start_date", "period_end_date"}
OUTCOMES = {Nature.DECISION: "left out", Nature.DEFECT: "marked", Nature.SIGNAL: "flagged"}


@dataclass
class RulesResult:
    frame: pl.DataFrame
    # By rule code: rows it applied to, and rows it could not read.
    applied: dict[str, int] = field(default_factory=dict)
    unreadable: dict[str, int] = field(default_factory=dict)
    # Rules reading a column the view lacks.
    unevaluated: list[str] = field(default_factory=list)


def apply_rules(
    df: pl.DataFrame,
    rules: list[Rule],
    rule_inputs: dict[str, pl.Expr] | None = None,
    source: str = "",
) -> RulesResult:
    """Leave out the rows a decision applies to, mark those a defect applies to."""
    if not rules or df.is_empty():
        return RulesResult(df)
    view = canonical_view(df, rule_inputs or {})
    evaluable = [rule for rule in rules if rule.reads <= set(view.columns)]
    result = RulesResult(df, unevaluated=[r.code for r in rules if r not in evaluable])
    if not evaluable:
        return result

    verdicts = view.select(rule.applies.alias(rule.code) for rule in evaluable)
    left_out = pl.Series([False] * df.height)
    defect = pl.Series([None] * df.height, dtype=pl.String)
    for rule in evaluable:
        verdict = verdicts[rule.code]
        if unreadable := verdict.null_count():
            result.unreadable[rule.code] = unreadable
            logger.warning(f"{source}: {unreadable} rows could not be read by {rule.code}")
        applies = verdict.fill_null(False)
        if not (applied := int(applies.sum())):
            continue
        result.applied[rule.code] = applied
        _log(source, rule, applied, view.filter(applies))
        if rule.nature is Nature.DECISION:
            left_out = left_out | applies
        elif rule.nature is Nature.DEFECT:
            defect = defect.zip_with(
                defect.is_not_null() | ~applies, pl.repeat(rule.label, df.height, eager=True)
            )

    frame = df
    if defect.is_not_null().any():
        marked = pl.Series(DEFECT_COLUMN, defect)
        if DEFECT_COLUMN in frame.columns:
            marked = frame[DEFECT_COLUMN].cast(pl.String).fill_null(marked)
        frame = frame.with_columns(marked.alias(DEFECT_COLUMN))
    result.frame = frame.filter(~left_out)
    return result


def canonical_view(df: pl.DataFrame, rule_inputs: dict[str, pl.Expr]) -> pl.DataFrame:
    """The frame with `rule_inputs` added, canonical columns cast to their canonical types.

    A value that does not cast becomes null, so the rule reading it reports it unreadable.
    """
    view = df.with_columns(**rule_inputs) if rule_inputs else df
    return view.with_columns(
        _typed(view, name, dtype) for name, dtype in CANONICAL_TYPES.items() if name in view.columns
    )


def _typed(view: pl.DataFrame, name: str, dtype: pl.DataType) -> pl.Expr:
    value = pl.col(name)
    current = view.schema[name]
    if dtype != INSTANT or current == INSTANT:
        return value.cast(dtype, strict=False)
    if isinstance(current, pl.Datetime) and current.time_zone:
        return value.dt.convert_time_zone(PARIS)
    if isinstance(current, (pl.Datetime, pl.Date)):
        return value.cast(pl.Datetime("us")).dt.replace_time_zone(PARIS)
    return value.map_elements(parse_instant, return_dtype=INSTANT, skip_nulls=True)


def _canonical_types() -> dict[str, pl.DataType]:
    simple = {str: pl.String(), int: pl.Int64(), float: pl.Float64(), bool: pl.Boolean()}
    types: dict[str, pl.DataType] = {}
    for name, hint in get_type_hints(RegulationMeasure).items():
        if name in INSTANT_COLUMNS:
            types[name] = INSTANT
            continue
        if get_origin(hint) in (Union, UnionType):
            hint = next(arg for arg in get_args(hint) if arg is not type(None))
        if get_origin(hint) is Literal:
            types[name] = pl.String()
        elif get_origin(hint) is list and get_args(hint) == (str,):
            types[name] = pl.List(pl.String)
        elif hint in simple:
            types[name] = simple[hint]
    return types


# Time slots (a list of dicts) are left as the source wrote them.
CANONICAL_TYPES = _canonical_types()


def _log(source: str, rule: Rule, applied: int, rows: pl.DataFrame) -> None:
    reference = f", {rule.reference}" if rule.reference else ""
    sample = ""
    if "regulation_identifier" in rows.columns:
        identifiers = rows["regulation_identifier"].cast(pl.String).unique().sort().to_list()
        sample = f": {identifiers[:5]}"
    logger.warning(
        f"{source}: {applied} rows {OUTCOMES[rule.nature]}, {rule.label} ({rule.code}{reference})"
        f"{sample}"
    )
