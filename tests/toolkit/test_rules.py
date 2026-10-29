"""Applying business rules to rows, alone and through a source."""

from datetime import date

import polars as pl
import pytest
from loguru import logger

from canonical.source import BaseDataSourceIntegration
from tests.sync_fixtures import measure_rows
from toolkit.rules.apply import apply_rules
from toolkit.rules.rule import Nature, Outcome, Rule

ENDS_FROM_2026_03 = pl.col("period_end_date") >= pl.datetime(2026, 3, 1, time_zone="Europe/Paris")
SPEED_UP_TO_40 = pl.col("measure_max_speed") <= 40


def rule(code: str, outcome: Outcome, valid: pl.Expr) -> Rule:
    return Rule(code, code.lower().replace("_", " "), Nature.OUT_OF_SCOPE, outcome, valid)


def rows(identifiers, **columns) -> pl.DataFrame:
    return measure_rows(identifiers).with_columns(
        **{name: pl.Series(values) for name, values in columns.items()}
    )


def test_a_decision_leaves_its_rows_out_and_keeps_the_others_in_order():
    df = rows(["A", "B", "C"], measure_max_speed=[30, 50, 20])

    result = apply_rules(df, [rule("FAST", Outcome.DROP, SPEED_UP_TO_40)])

    assert result.frame["regulation_identifier"].to_list() == ["A", "C"]
    assert result.frame.columns == df.columns
    assert result.applied == {"FAST": 1}


def test_a_signal_changes_nothing_but_is_counted():
    df = rows(["A", "B"], measure_max_speed=[30, 50])

    result = apply_rules(df, [rule("FAST", Outcome.FLAG, SPEED_UP_TO_40)])

    assert result.frame.equals(df)
    assert result.applied == {"FAST": 1}


def test_rules_sharing_a_code_count_a_row_once_in_one_call():
    """The common rule on a canonical column and a source's own on its column."""
    df = rows(["A", "B", "C"], measure_max_speed=[30, 50, 90], limit=[20, 20, 90])

    result = apply_rules(
        df,
        [
            rule("FAST", Outcome.DROP, SPEED_UP_TO_40),
            rule("FAST", Outcome.DROP, pl.col("limit") <= 40),
        ],
    )

    assert result.frame["regulation_identifier"].to_list() == ["A"]
    assert result.applied == {"FAST": 2}


def test_overlapping_rules_each_count_their_rows():
    df = rows(["A", "B"], measure_max_speed=[30, 50])
    rules = [
        rule("FAST", Outcome.DROP, SPEED_UP_TO_40),
        rule("ANY", Outcome.DROP, pl.col("measure_max_speed") <= 0),
    ]

    assert apply_rules(df, rules).applied == {"FAST": 1, "ANY": 2}


def test_a_source_column_is_read_under_its_canonical_name_and_stays_out_of_the_frame():
    df = pl.DataFrame({"vitesse": ["30", "50"], "regulation_identifier": ["A", "B"]})

    result = apply_rules(
        df,
        [rule("FAST", Outcome.DROP, SPEED_UP_TO_40)],
        rule_inputs={"measure_max_speed": pl.col("vitesse")},
    )

    assert result.frame.to_dicts() == [{"vitesse": "30", "regulation_identifier": "A"}]


def test_period_bounds_are_compared_as_instants_whatever_their_shape():
    df = rows(
        ["early", "late", "offset"],
        period_end_date=["2026-02-01", "2026-04-01", "2026-03-01T00:30:00+01:00"],
    )
    by_date = pl.DataFrame({"fin": [date(2026, 2, 1), date(2026, 4, 1)]})

    kept = apply_rules(df, [rule("ENDED", Outcome.DROP, ENDS_FROM_2026_03)]).frame
    kept_by_date = apply_rules(
        by_date,
        [rule("ENDED", Outcome.DROP, ENDS_FROM_2026_03)],
        rule_inputs={"period_end_date": pl.col("fin")},
    ).frame

    assert kept["regulation_identifier"].to_list() == ["late", "offset"]
    assert kept_by_date["fin"].to_list() == [date(2026, 4, 1)]


def test_a_value_missing_or_that_cannot_be_read_fails_the_rule():
    df = rows(["A", "B", "C"], period_end_date=["2026-04-01", "fin des travaux", None])

    result = apply_rules(df, [rule("ENDED", Outcome.DROP, ENDS_FROM_2026_03)])

    assert result.frame["regulation_identifier"].to_list() == ["A"]
    assert result.applied == {"ENDED": 2}


def test_on_canonical_rows_a_column_the_source_lacks_fails_the_rule_like_a_missing_value():
    df = rows(["A"])
    tall = rule("TALL", Outcome.DROP, pl.col("vehicle_max_height") <= 4)

    result = apply_rules(df, [tall], canonical=True)

    assert result.unevaluated == []
    assert result.frame.is_empty()


def test_a_rule_can_apply_to_the_whole_regulation():
    df = rows(["A", "A", "B"], period_end_date=["2026-02-01", "2026-04-01", "2026-02-01"])
    a_measure_still_runs = pl.col("period_end_date").max().over("regulation_identifier") >= (
        pl.datetime(2026, 3, 1, time_zone="Europe/Paris")
    )

    kept = apply_rules(df, [rule("ENDED", Outcome.DROP, a_measure_still_runs)]).frame

    assert kept["regulation_identifier"].to_list() == ["A", "A"]


def test_a_rule_reading_a_missing_column_is_reported_unevaluated():
    df = rows(["A"])

    result = apply_rules(df, [rule("HEAVY", Outcome.DROP, pl.col("vehicle_max_height") <= 4)])

    assert result.unevaluated == ["HEAVY"]
    assert result.frame.equals(df)


# --- Through a source -------------------------------------------------------------

FAST = Rule("FAST", "trop rapide", Nature.OUT_OF_SCOPE, Outcome.DROP, SPEED_UP_TO_40)
SLOW = Rule("SLOW", "trop lent", Nature.SUSPICIOUS, Outcome.FLAG, pl.col("measure_max_speed") >= 25)


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
    assert source.rule_counts == {
        "trop rapide (hors périmètre, écarté)": 1,
        "trop lent (à vérifier, signalé)": 1,
    }


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
    assert source.rule_counts == {"trop rapide (hors périmètre, écarté)": 1}
    assert errors == []


def test_a_decision_that_still_applies_after_the_source_applied_it_is_an_error(errors):
    class Late(_Source):
        def compute_clean_data(self, raw_data):
            kept = self.apply_rules(raw_data)
            return kept.with_columns(measure_max_speed=pl.lit(90))

    assert Late().compute_data_regulations().is_empty()
    assert len(errors) == 1 and "FAST" in errors[0]


def test_a_source_applies_its_own_rules_with_the_common_ones():
    own = Rule(
        "C_ONLY",
        "hors liste",
        Nature.OUT_OF_SCOPE,
        Outcome.DROP,
        pl.col("regulation_identifier") != "C",
    )

    class Own(_Source):
        def compute_clean_data(self, raw_data):
            return self.apply_rules(raw_data, own)

    assert Own().compute_data_regulations()["regulation_identifier"].to_list() == ["A"]


def test_the_common_rules_apply_to_a_source_that_declares_nothing():
    class Points(_Source):
        rules = ()
        frame = rows(["A", "B"]).with_columns(
            location_geometry=pl.lit('{"type": "Point", "coordinates": [2.3, 48.8]}'),
            measure_type_=pl.Series(["speedLimitation", "parkingProhibited"]),
        )

    assert Points().compute_data_regulations()["regulation_identifier"].to_list() == ["B"]


def test_a_source_may_disable_a_common_rule_with_its_reason():
    class Points(_Source):
        rules = ()
        disabled_rules = {"POINT_NOT_PARKING": "test"}
        frame = rows(["A"]).with_columns(
            location_geometry=pl.lit('{"type": "Point", "coordinates": [2.3, 48.8]}'),
            measure_type_=pl.lit("speedLimitation"),
        )

    assert Points().compute_data_regulations().height == 1
