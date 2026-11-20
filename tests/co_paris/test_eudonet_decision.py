import polars as pl
import pytest

from integrations.co_paris.eudonet.decision import LOCATED, alive_now, decide, judged
from integrations.co_paris.eudonet.verdicts import (
    CHECKS,
    REGISTRY,
    Check,
    Level,
    Stage,
    Status,
    among,
    refusal,
    refusals,
)
from tests.co_paris.acts import (
    act_rows,
    everything,
    frame,
    outcome_of,
    reasons_of,
    with_verdict,
)
from tests.co_paris.steps import drop_refused
from toolkit.rules.rule import Nature


def check(code: str, judged_on: Level, refuses: Level, stage: Stage = Stage.LOCATIONS) -> Check:
    return Check(code, code.lower(), Nature.DATA_DEFECT, stage, judged_on, refuses, f"rows {code}")


ON_LOCATION = check("ON_LOCATION", Level.LOCATION, Level.LOCATION)
LOCATION_TO_MEASURE = check("LOCATION_TO_MEASURE", Level.LOCATION, Level.MEASURE, Stage.COHERENCE)
MEASURE_TO_ACT = check("MEASURE_TO_ACT", Level.MEASURE, Level.ACT, Stage.FIELDS)
ON_ACT = check("ON_ACT", Level.ACT, Level.ACT, Stage.PREFILTER)
ORDER = (ON_ACT, MEASURE_TO_ACT, ON_LOCATION, LOCATION_TO_MEASURE)


@pytest.fixture
def rows():
    # A: two measures of two locations; B: one measure of one location.
    return frame(act_rows("A", 1, {10: [100, 101], 11: [110, 111]}), act_rows("B", 2, {20: [200]}))


def test_without_verdict_every_row_is_kept_and_every_act_complete(rows):
    decision = decide(rows, ORDER)
    assert decision.kept.equals(rows)
    assert decision.reasons.is_empty() and decision.removed.is_empty()
    assert set(decision.outcomes["outcome"]) == {"complete"}


def test_a_location_refusal_drops_its_row_only(rows):
    decision = decide(with_verdict(rows, ON_LOCATION, pl.col("l_file_id") == 100), ORDER)
    assert decision.kept["l_file_id"].to_list() == [101, 110, 111, 200]
    assert outcome_of(decision, "A") == "partial"
    assert outcome_of(decision, "B") == "complete"


def test_a_location_can_refuse_its_whole_measure(rows):
    decision = decide(with_verdict(rows, LOCATION_TO_MEASURE, pl.col("l_file_id") == 100), ORDER)
    assert decision.kept["l_file_id"].to_list() == [110, 111, 200]
    assert reasons_of(decision, location=100) == ["LOCATION_TO_MEASURE"]
    assert reasons_of(decision, location=101) == []


def test_a_measure_can_refuse_its_whole_act(rows):
    decision = decide(with_verdict(rows, MEASURE_TO_ACT, pl.col("m_file_id") == 11), ORDER)
    assert decision.kept["a_identifier"].to_list() == ["B"]
    assert outcome_of(decision, "A") == "refused"
    # Judged on the measure: one reason, not one per row.
    assert reasons_of(decision, measure=11) == ["MEASURE_TO_ACT"]


def test_an_act_refusal_leaves_the_other_acts(rows):
    decision = decide(with_verdict(rows, ON_ACT, pl.col("a_identifier") == "B"), ORDER)
    assert set(decision.kept["a_identifier"]) == {"A"}
    assert reasons_of(decision, act="B") == ["ON_ACT"]


def test_acts_without_measure_are_not_one_measure():
    rows = frame(act_rows("A", 1, {}), act_rows("B", 2, {}))
    decision = decide(with_verdict(rows, LOCATION_TO_MEASURE, pl.col("a_identifier") == "A"), ORDER)
    assert decision.kept["a_identifier"].to_list() == ["B"]


def test_a_measure_whose_every_location_is_refused_empties_its_act():
    rows = frame(act_rows("A", 1, {10: [100, 101]}))
    decision = decide(with_verdict(rows, ON_LOCATION, pl.lit(True)), ORDER)
    assert decision.kept.is_empty()
    assert outcome_of(decision, "A") == "refused"
    assert decision.outcomes["emptied_by"].to_list() == ["ON_LOCATION"]


def test_the_primary_reason_is_the_first_verdict_judged(rows):
    marked = with_verdict(rows, ON_LOCATION, pl.col("l_file_id") == 100).pipe(
        with_verdict, LOCATION_TO_MEASURE, pl.col("l_file_id") == 101
    )
    decision = decide(marked, ORDER)
    primary = dict(decision.removed.select("l_file_id", "primary").iter_rows())
    # Judged first, its column first: ON_LOCATION, though the other refuses 100 too.
    assert primary == {100: "ON_LOCATION", 101: "LOCATION_TO_MEASURE"}
    assert decision.reasons.filter(pl.col("record") == 100)["primary"].to_list() == [True]


def test_a_reason_behind_an_earlier_one_is_kept_but_not_primary(rows):
    marked = with_verdict(rows, ON_ACT, pl.col("a_identifier") == "A").pipe(
        with_verdict, ON_LOCATION, pl.col("l_file_id") == 100
    )
    decision = decide(marked, ORDER)
    reasons = decision.reasons.filter(pl.col("code") == "ON_LOCATION")
    assert reasons["primary"].to_list() == [False]
    assert reasons_of(decision, act="A") == ["ON_ACT"]


def test_an_act_empties_at_the_latest_of_its_rows_reasons():
    rows = frame(act_rows("A", 1, {10: [100, 101]}))
    marked = with_verdict(rows, ON_LOCATION, pl.col("l_file_id") == 100).pipe(
        with_verdict, LOCATION_TO_MEASURE, pl.col("l_file_id") == 101
    )
    decision = decide(marked, ORDER)
    assert decision.outcomes["emptied_by"].to_list() == ["LOCATION_TO_MEASURE"]


def test_the_primary_reasons_add_up_to_the_rows_removed(rows):
    marked = with_verdict(rows, ON_ACT, pl.col("a_identifier") == "B").pipe(
        with_verdict, ON_LOCATION, pl.col("l_file_id").is_in([100, 200])
    )
    decision = decide(marked, ORDER)
    assert decision.removed.height + decision.kept.height == rows.height
    assert decision.removed["primary"].value_counts().sort("primary").rows() == [
        ("ON_ACT", 1),
        ("ON_LOCATION", 1),
    ]


def test_a_flag_drops_nothing_and_is_a_reason(rows):
    decision = decide(
        with_verdict(rows, ON_LOCATION, pl.col("l_file_id") == 100, Status.FLAGGED, "à voir"), ORDER
    )
    assert decision.kept.height == rows.height
    assert decision.reasons.select("record", "status", "detail", "primary").rows() == [
        (100, "flagged", "à voir", False)
    ]


def test_unjudged_is_neither_dropped_nor_a_reason(rows):
    decision = decide(
        with_verdict(rows, ON_LOCATION, pl.col("l_file_id") == 100, Status.UNJUDGED), ORDER
    )
    assert decision.kept.height == rows.height
    assert decision.reasons.is_empty()


def test_the_detail_goes_with_the_reason(rows):
    decision = decide(
        with_verdict(rows, ON_LOCATION, pl.col("l_file_id") == 100, detail="un seul côté"), ORDER
    )
    assert decision.reasons["detail"].to_list() == ["un seul côté"]


def test_kept_rows_keep_their_order_and_their_verdicts(rows):
    shuffled = rows.reverse().pipe(with_verdict, ON_LOCATION, pl.col("l_file_id") == 110)
    decision = decide(shuffled, ORDER)
    assert decision.kept["l_file_id"].to_list() == [200, 111, 101, 100]
    assert decision.kept.columns == shuffled.columns


def test_alive_now_leaves_out_every_record_a_verdict_refuses(rows, monkeypatch):
    for test_check in ORDER:
        monkeypatch.setitem(REGISTRY, test_check.column, test_check)
    marked = with_verdict(rows, MEASURE_TO_ACT, pl.col("l_file_id") == 100).pipe(
        with_verdict, ON_LOCATION, pl.col("l_file_id") == 200
    )
    assert alive_now(marked).to_list() == [False, False, False, False, False]
    assert alive_now(rows).to_list() == [True] * 5


def test_judged_keeps_the_checks_the_rows_carry_in_the_order_of_their_columns(rows):
    marked = with_verdict(rows, ON_LOCATION, pl.lit(False)).pipe(
        with_verdict, ON_ACT, pl.lit(False)
    )
    assert judged(marked, ORDER) == (ON_LOCATION, ON_ACT)


def test_a_check_cannot_refuse_below_what_it_judges():
    with pytest.raises(ValueError):
        check("WRONG", Level.ACT, Level.LOCATION)


def test_a_refusal_is_a_detail_given():
    out = pl.DataFrame({"why": ["raison", None]}).select(refusal(pl.col("why")).alias("v"))
    assert out["v"].to_list() == [
        {"status": "refused", "detail": "raison"},
        {"status": "ok", "detail": None},
    ]
    assert pl.DataFrame({"x": [1, 2]}).select(refusals(["côté", None]).alias("v"))[
        "v"
    ].to_list() == [{"status": "refused", "detail": "côté"}, {"status": "ok", "detail": None}]


def test_a_verdict_among_the_living_leaves_the_others_unjudged():
    out = pl.DataFrame({"why": ["a", "b"]}).select(
        among(pl.Series([True, False]), refusal(pl.col("why"))).alias("v")
    )
    assert out["v"].to_list() == [{"status": "refused", "detail": "a"}, None]


def test_a_location_check_judges_nothing_where_the_locations_were_not_read(rows):
    unread = rows.with_columns(pl.Series(LOCATED, [True, True, True, True, False]))
    decision = decide(with_verdict(unread, ON_LOCATION, pl.lit(True), detail="vide"), ORDER)
    assert decision.kept["l_file_id"].to_list() == [200]
    assert reasons_of(decision, location=200) == []


def test_the_registry_has_one_column_per_check():
    assert len({c.code for c in CHECKS}) == len({c.column for c in CHECKS}) == len(CHECKS)


def test_drop_refused_is_the_decision_on_one_check_and_logs_it(rows, captured_logs):
    marked = with_verdict(rows, ON_LOCATION, pl.col("l_file_id").is_in([100, 200]), detail="côté")
    kept = drop_refused(marked, ON_LOCATION)
    assert kept.equals(decide(marked, (ON_LOCATION,)).kept)
    assert any(
        "Dropping 2 rows ON_LOCATION: {'côté': 2}; 1 regulations published without them, "
        "1 left with no location" in line
        for line in captured_logs
    )


def test_a_measure_refusal_is_logged_by_measure(rows, captured_logs):
    drop_refused(
        with_verdict(rows, LOCATION_TO_MEASURE, pl.col("l_file_id") == 200), LOCATION_TO_MEASURE
    )
    assert any(
        "Dropping 1 rows LOCATION_TO_MEASURE (1 such locations, 1 rows); "
        "1 regulations left with no measure" in line
        for line in captured_logs
    )


def test_the_run_keeps_every_row_until_its_one_decision(monkeypatch):
    from integrations.co_paris.eudonet import data_source_integration
    from integrations.co_paris.eudonet.raw_rows import build_raw_dataframe
    from settings import OrganizationSettings
    from tests.co_paris.conftest import OfflineSettings
    from tests.co_paris.eudonet_fixture import load_fixture

    settings = OrganizationSettings(OfflineSettings(), "co_paris")  # type: ignore[arg-type]
    source = data_source_integration.DataSourceIntegration(settings, None)  # type: ignore[arg-type]
    source.numbers_published_elsewhere = {}
    tables = load_fixture()
    raw = build_raw_dataframe(tables, everything(tables))
    clean = source.compute_clean_data(raw)
    decision = source.decision
    assert decision is not None
    assert decision.kept.height + decision.removed.height == raw.height
    assert set(decision.outcomes["outcome"]) <= {"complete", "partial", "refused"}
    assert decision.outcomes.height == raw["a_file_id"].n_unique()
    # Every row removed has its reason, and the first of them is the primary one.
    assert decision.removed["primary"].null_count() == 0
    assert clean.height >= decision.kept.height
    # The same reasons, detail by detail, from one run to the next.
    first = decision.reasons
    source.compute_clean_data(raw)
    assert source.decision is not None and source.decision.reasons.equals(first)
