"""Applying business rules to rows, alone and through a source."""

from datetime import date

import polars as pl
import pytest
from loguru import logger

from canonical.regulation_measure import DEFECT_COLUMN
from canonical.source import BaseDataSourceIntegration
from checks import rows as checks
from tests.sync_fixtures import measure_rows
from toolkit.rules.apply import apply_rules
from toolkit.rules.rule import Nature, Rule

ENDS_BEFORE_2026_03 = pl.col("period_end_date") < pl.datetime(2026, 3, 1, time_zone="Europe/Paris")
SPEED_ABOVE_40 = pl.col("measure_max_speed") > 40


def rule(code: str, nature: Nature, applies: pl.Expr) -> Rule:
    return Rule(code, code.lower().replace("_", " "), nature, applies)


def rows(identifiers, **columns) -> pl.DataFrame:
    return measure_rows(identifiers).with_columns(
        **{name: pl.Series(values) for name, values in columns.items()}
    )


def test_a_decision_leaves_its_rows_out_and_keeps_the_others_in_order():
    df = rows(["A", "B", "C"], measure_max_speed=[30, 50, 20])

    result = apply_rules(df, [rule("FAST", Nature.DECISION, SPEED_ABOVE_40)])

    assert result.frame["regulation_identifier"].to_list() == ["A", "C"]
    assert result.frame.columns == df.columns
    assert result.applied == {"FAST": 1}


def test_a_defect_marks_its_rows_and_keeps_them():
    df = rows(["A", "B"], measure_max_speed=[30, 50])

    result = apply_rules(df, [rule("FAST", Nature.DEFECT, SPEED_ABOVE_40)])

    assert result.frame[DEFECT_COLUMN].to_list() == [None, "fast"]
    assert checks.failing_rows(result.frame).to_dicts() == [
        {"regulation_identifier": "B", "reason": "fast"}
    ]


def test_the_first_defect_names_the_row_and_a_mark_set_earlier_is_kept():
    df = rows(["A", "B"], measure_max_speed=[50, 50]).with_columns(
        pl.Series(DEFECT_COLUMN, ["earlier", None])
    )
    rules = [
        rule("FAST", Nature.DEFECT, SPEED_ABOVE_40),
        rule("ANY", Nature.DEFECT, pl.col("measure_max_speed") > 0),
    ]

    assert apply_rules(df, rules).frame[DEFECT_COLUMN].to_list() == ["earlier", "fast"]


def test_a_signal_changes_nothing_but_is_counted():
    df = rows(["A", "B"], measure_max_speed=[30, 50])

    result = apply_rules(df, [rule("FAST", Nature.SIGNAL, SPEED_ABOVE_40)])

    assert result.frame.equals(df)
    assert result.applied == {"FAST": 1}


def test_overlapping_rules_each_count_their_rows():
    df = rows(["A", "B"], measure_max_speed=[30, 50])
    rules = [
        rule("FAST", Nature.DECISION, SPEED_ABOVE_40),
        rule("ANY", Nature.DECISION, pl.col("measure_max_speed") > 0),
    ]

    assert apply_rules(df, rules).applied == {"FAST": 1, "ANY": 2}


def test_a_source_column_is_read_under_its_canonical_name_and_stays_out_of_the_frame():
    df = pl.DataFrame({"vitesse": ["30", "50"], "regulation_identifier": ["A", "B"]})

    result = apply_rules(
        df,
        [rule("FAST", Nature.DECISION, SPEED_ABOVE_40)],
        rule_inputs={"measure_max_speed": pl.col("vitesse")},
    )

    assert result.frame.to_dicts() == [{"vitesse": "30", "regulation_identifier": "A"}]


def test_period_bounds_are_compared_as_instants_whatever_their_shape():
    df = rows(
        ["early", "late", "offset"],
        period_end_date=["2026-02-01", "2026-04-01", "2026-03-01T00:30:00+01:00"],
    )
    by_date = pl.DataFrame({"fin": [date(2026, 2, 1), date(2026, 4, 1)]})

    kept = apply_rules(df, [rule("ENDED", Nature.DECISION, ENDS_BEFORE_2026_03)]).frame
    kept_by_date = apply_rules(
        by_date,
        [rule("ENDED", Nature.DECISION, ENDS_BEFORE_2026_03)],
        rule_inputs={"period_end_date": pl.col("fin")},
    ).frame

    assert kept["regulation_identifier"].to_list() == ["late", "offset"]
    assert kept_by_date["fin"].to_list() == [date(2026, 4, 1)]


def test_a_value_that_cannot_be_read_is_unreadable_and_kept():
    df = rows(["A", "B"], period_end_date=["2026-02-01", "fin des travaux"])

    result = apply_rules(df, [rule("ENDED", Nature.DECISION, ENDS_BEFORE_2026_03)])

    assert result.frame["regulation_identifier"].to_list() == ["B"]
    assert result.unreadable == {"ENDED": 1}


def test_a_rule_can_apply_to_the_whole_regulation():
    df = rows(["A", "A", "B"], period_end_date=["2026-02-01", "2026-04-01", "2026-02-01"])
    every_measure_ended = pl.col("period_end_date").max().over("regulation_identifier") < (
        pl.datetime(2026, 3, 1, time_zone="Europe/Paris")
    )

    kept = apply_rules(df, [rule("ENDED", Nature.DECISION, every_measure_ended)]).frame

    assert kept["regulation_identifier"].to_list() == ["A", "A"]


def test_a_rule_reading_a_missing_column_is_reported_unevaluated():
    df = rows(["A"])

    result = apply_rules(df, [rule("HEAVY", Nature.DECISION, pl.col("vehicle_max_height") > 4)])

    assert result.unevaluated == ["HEAVY"]
    assert result.frame.equals(df)


# --- Through a source -------------------------------------------------------------

FAST = Rule("FAST", "trop rapide", Nature.DECISION, SPEED_ABOVE_40, reference="R-00")
SLOW = Rule("SLOW", "trop lent", Nature.SIGNAL, pl.col("measure_max_speed") < 25)


class _Source(BaseDataSourceIntegration):
    name = "test"
    rules = (FAST, SLOW)
    frame = rows(["A", "B", "C"], measure_max_speed=[30, 50, 20])

    def __init__(self):
        super().__init__(organization_settings=None, client=None)  # type: ignore[arg-type]

    def fetch_raw_data(self):
        return self.frame

    def validate_raw_data(self, raw_data):
        return raw_data

    def compute_clean_data(self, raw_data):
        return raw_data


@pytest.fixture
def errors():
    messages: list[str] = []
    sink = logger.add(lambda message: messages.append(str(message)), level="ERROR")
    yield messages
    logger.remove(sink)


def test_a_source_has_its_rules_applied_to_its_canonical_rows():
    source = _Source()

    assert source.compute_data_regulations()["regulation_identifier"].to_list() == ["A", "C"]
    assert source.rule_counts == {"trop rapide (écarté)": 1, "trop lent (signalé)": 1}


def test_a_source_declaring_no_rule_is_left_as_it_is():
    class Untouched(_Source):
        rules = ()

    source = Untouched()

    assert source.compute_data_regulations().height == 3
    assert source.rule_counts == {}


def test_a_source_applying_the_rules_early_is_not_counted_twice(errors):
    class Early(_Source):
        rule_inputs = {"measure_max_speed": pl.col("vitesse")}
        frame = pl.DataFrame({"regulation_identifier": ["A", "B"], "vitesse": [30, 50]})

        def compute_clean_data(self, raw_data):
            return self.apply_rules(raw_data).rename({"vitesse": "measure_max_speed"})

    source = Early()

    assert source.compute_data_regulations()["regulation_identifier"].to_list() == ["A"]
    assert source.rule_counts == {"trop rapide (écarté)": 1}
    assert errors == []


def test_a_decision_that_still_applies_after_the_source_applied_it_is_an_error(errors):
    class Late(_Source):
        def compute_clean_data(self, raw_data):
            kept = self.apply_rules(raw_data)
            return kept.with_columns(measure_max_speed=pl.lit(90))

    assert Late().compute_data_regulations().is_empty()
    assert len(errors) == 1 and "FAST" in errors[0]


def test_a_source_applies_its_own_rules_with_the_common_ones():
    own = Rule("C_ONLY", "hors liste", Nature.DECISION, pl.col("regulation_identifier") == "C")

    class Own(_Source):
        def compute_clean_data(self, raw_data):
            return self.apply_rules(raw_data, own)

    assert Own().compute_data_regulations()["regulation_identifier"].to_list() == ["A"]
