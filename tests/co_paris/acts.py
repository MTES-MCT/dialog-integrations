"""Test helpers: rows of an act written by hand, and readable questions on a decision."""

from datetime import date, datetime
from typing import Any

import polars as pl

from integrations.co_paris.eudonet.decision import Decision
from integrations.co_paris.eudonet.raw_rows import RAW_COLUMNS, TABLE_MEASURES, Expansion
from integrations.co_paris.eudonet.verdicts import STATUS, VERDICT, Check, Status
from tests.co_paris.steps import (
    compute_measure_fields,
    compute_period_fields,
    compute_regulation_fields,
    compute_vehicle_fields,
    discard_measures_out_of_model,
)

FIXTURE_EXTRACTED_ON = date(2026, 9, 8)


def make_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = dict.fromkeys(RAW_COLUMNS)
    row.update(
        a_file_id=1,
        a_identifier="2024P10001",
        a_title_html="Réglementant le stationnement rue de Test",
        a_type="Permanent",
        a_state="En vigueur",
        a_start_date=date(2020, 1, 1),
        a_signed_at=date(2019, 12, 20),
        a_modified_at=datetime(2021, 3, 4, 0, 30, 0),
        a_service="Mairie de Paris",
        m_file_id=10,
        m_type="interdiction de stationnement",
        m_params=[["véhicule concerné (3)", "à tous les véhicules"]],
        l_file_id=100,
        l_scope="Un point",
        l_road_name="Rue de Test",
        l_district="11ème arrondissement",
        l_point_house_number="12",
    )
    row.update(overrides)
    return row


def make_frame(*rows: dict[str, Any]) -> pl.DataFrame:
    return pl.DataFrame(list(rows), schema=RAW_COLUMNS)


def pipeline(*rows: dict[str, Any], today: date = FIXTURE_EXTRACTED_ON) -> pl.DataFrame:
    return (
        make_frame(*rows)
        .pipe(compute_regulation_fields)
        .pipe(compute_measure_fields)
        .pipe(discard_measures_out_of_model)
        .pipe(compute_vehicle_fields)
        .pipe(compute_period_fields, today)
    )


def everything(tables: dict[str, list[dict[str, Any]]]) -> Expansion:
    """Every measure of `tables` read whole: no prefilter."""
    measures = frozenset(row["FileId"] for row in tables.get(str(TABLE_MEASURES), []))
    return Expansion(measures, measures)


def act_rows(
    identifier: str, file_id: int, measures: dict[int, list[int | None]]
) -> list[dict[str, Any]]:
    """One row per location; `{10: [100, 101], 11: [None]}` is two measures, the second
    without location. `{}` is an act without measure."""
    base = {"a_file_id": file_id, "a_identifier": identifier}
    if not measures:
        return [{**base, "m_file_id": None, "l_file_id": None}]
    return [
        {**base, "m_file_id": measure, "l_file_id": location}
        for measure, locations in measures.items()
        for location in locations
    ]


def frame(*acts: list[dict[str, Any]]) -> pl.DataFrame:
    return pl.DataFrame(
        [row for act in acts for row in act],
        schema={
            "a_file_id": pl.Int64,
            "a_identifier": pl.String,
            "m_file_id": pl.Int64,
            "l_file_id": pl.Int64,
        },
    )


def with_verdict(
    rows: pl.DataFrame,
    check: Check,
    where: pl.Expr,
    status: Status = Status.REFUSED,
    detail: str | None = None,
) -> pl.DataFrame:
    """Add `check`'s verdict: `status` on the rows `where` holds, ok elsewhere."""
    hit = where.fill_null(False)
    return rows.with_columns(
        pl.struct(
            status=pl.when(hit)
            .then(pl.lit(status.value))
            .otherwise(pl.lit(Status.OK.value))
            .cast(STATUS),
            detail=pl.when(hit).then(pl.lit(detail)).otherwise(None).cast(pl.String),
        )
        .cast(VERDICT)
        .alias(check.column)
    )


def reasons_of(decision: Decision, **record: Any) -> list[str]:
    """Codes judged on the record, `location=100`, `measure=10` or `act="2024T1"`."""
    ((level, value),) = record.items()
    reasons = decision.reasons
    if level == "act":
        reasons = reasons.filter((pl.col("level") == "act") & (pl.col("a_identifier") == value))
    else:
        reasons = reasons.filter((pl.col("level") == level) & (pl.col("record") == value))
    return reasons["code"].to_list()


def outcome_of(decision: Decision, identifier: str) -> str:
    return decision.outcomes.filter(pl.col("a_identifier") == identifier)["outcome"].item()
