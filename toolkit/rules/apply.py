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

from canonical.regulation_measure import RegulationMeasure
from toolkit.rules.rule import Outcome, Rule
from toolkit.transforms.local_time import PARIS, parse_instant

INSTANT = pl.Datetime("us", PARIS)
INSTANT_COLUMNS = {"period_start_date", "period_end_date"}
LOGGED_AS = {Outcome.DROP: "left out", Outcome.FLAG: "flagged"}


@dataclass
class RulesResult:
    frame: pl.DataFrame
    # By rule code: rows that failed it.
    applied: dict[str, int] = field(default_factory=dict)
    # Rules reading a column the view lacks.
    unevaluated: list[str] = field(default_factory=list)


@dataclass
class RuleVerdicts:
    # By rule code: the rule (the first of that code) and the rows that fail it.
    fails: dict[str, tuple[Rule, pl.Series]] = field(default_factory=dict)
    # Rules reading a column the view lacks.
    unevaluated: list[str] = field(default_factory=list)
    # The view the rules were read on, for the log.
    view: pl.DataFrame = field(default_factory=pl.DataFrame)


def rule_verdicts(
    df: pl.DataFrame,
    rules: list[Rule],
    rule_inputs: dict[str, pl.Expr] | None = None,
    canonical: bool = False,
) -> RuleVerdicts:
    """Which rows fail each rule, every row of `df` judged, none left out.

    On `canonical` rows, a canonical column the source does not produce is read as null: it
    is judged like any missing value, not skipped.
    """
    if not rules or df.is_empty():
        return RuleVerdicts()
    view = canonical_view(df, rule_inputs or {})
    if canonical:
        view = view.with_columns(
            pl.lit(None, dtype=dtype).alias(name)
            for name, dtype in CANONICAL_TYPES.items()
            if name not in view.columns
        )
    evaluable = [rule for rule in rules if rule.reads <= set(view.columns)]
    evaluated = {rule.code for rule in evaluable}
    result = RuleVerdicts(unevaluated=[r.code for r in rules if r.code not in evaluated], view=view)
    if not evaluable:
        return result

    verdicts = view.select(rule.expression().alias(str(i)) for i, rule in enumerate(evaluable))
    # Rules sharing a code are one rule read from several columns (the common one on the
    # canonical, a source's own on its columns): a row fails it once.
    for i, rule in enumerate(evaluable):
        # A rule validates: what it cannot say is valid, a missing or unreadable value
        # included, fails it. A rule fed the wrong column fails every row, and shows.
        fails = ~verdicts[str(i)].fill_null(False)
        if rule.code in result.fails:
            first, earlier = result.fails[rule.code]
            result.fails[rule.code] = (first, earlier | fails)
        else:
            result.fails[rule.code] = (rule, fails)
    return result


def apply_rules(
    df: pl.DataFrame,
    rules: list[Rule],
    rule_inputs: dict[str, pl.Expr] | None = None,
    source: str = "",
    canonical: bool = False,
) -> RulesResult:
    """Leave out the rows a dropping rule cannot validate, log those a flagging one cannot."""
    verdicts = rule_verdicts(df, rules, rule_inputs, canonical)
    result = RulesResult(df, unevaluated=verdicts.unevaluated)
    left_out = pl.Series([False] * df.height)
    for code, (rule, fails) in verdicts.fails.items():
        if not (failed := int(fails.sum())):
            continue
        result.applied[code] = failed
        log_rule(source, rule, failed, verdicts.view.filter(fails))
        if rule.outcome is Outcome.DROP:
            left_out = left_out | fails
    result.frame = df.filter(~left_out)
    return result


def canonical_view(df: pl.DataFrame, rule_inputs: dict[str, pl.Expr]) -> pl.DataFrame:
    """The frame with `rule_inputs` added, canonical columns cast to their canonical types.

    A value that does not cast becomes null, like a missing one.
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


def log_rule(source: str, rule: Rule, failed: int, rows: pl.DataFrame) -> None:
    sample = ""
    if "regulation_identifier" in rows.columns:
        identifiers = rows["regulation_identifier"].cast(pl.String).unique().sort().to_list()
        sample = f": {identifiers[:5]}"
    logger.warning(
        f"{source}: {failed} rows {LOGGED_AS[rule.outcome]}, {rule.label} ({rule.code}){sample}"
    )
