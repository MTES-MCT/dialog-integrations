"""The port against the back end's own PHP code, run on the same simulated cases.

`php_oracle.json.gz` is what the back end's Litteralis code (commit and PHP version
recorded inside) produced on `simulated_cases.cases()`: WFS requests, import commands,
report records, fatal errors. The port must reproduce it value for value. The file is
regenerated with PHP whenever the cases or the back end's code change; the day it was
produced on is replayed, since time slots depend on it.
"""

import gzip
import json
from datetime import date
from pathlib import Path

import pytest

from integrations.dialog.litteralis.url_validator import is_valid_https_url
from tests.dialog.litteralis.port_runner import (
    comparable_php_case,
    comparable_port_case,
    first_difference,
    run_case,
)
from tests.dialog.litteralis.simulated_cases import URLS, cases

with gzip.open(Path(__file__).with_name("php_oracle.json.gz"), "rt", encoding="utf-8") as handle:
    ORACLE = json.load(handle)

CASES = {case["name"]: case for case in cases()}
PHP_CASES = {case["name"]: case for case in ORACLE["cases"]}


def test_the_oracle_covers_every_case():
    assert list(PHP_CASES) == list(CASES), "cases changed: regenerate the PHP oracle"


@pytest.mark.parametrize("name", list(CASES))
def test_the_port_reproduces_the_back_end(name):
    expected = comparable_php_case(PHP_CASES[name])
    actual = comparable_port_case(run_case(CASES[name], date.fromisoformat(ORACLE["today"])))

    php_fatal, port_fatal = expected.pop("fatal"), actual.pop("fatal")
    assert (php_fatal or {}).get("class") == (port_fatal or {}).get("class")
    assert first_difference(expected, actual) is None


def test_the_cases_exercise_what_they_are_meant_to():
    """Guards against a generator change that would quietly empty the comparison."""
    standard = PHP_CASES["standard"]
    names = [context[kind] for kind, context in standard["records"]]
    assert sum(1 for r in standard["results"] if r["command"]) > 50
    for record in (
        # `missing_geometry_excluded` is absent on purpose: the extractors already drop
        # the features without geometry, the transformer never sees one.
        "litteralis.polygon_location_excluded",
        "litteralis.unsupported_measure",
        "litteralis.no_measures_found",
        "litteralis.period_unparsable",
        "litteralis.measure_parameter_inconsistent_number",
        "litteralis.measure_parameter_malformed",
        "litteralis.max_speed_value_missing",
        "litteralis.max_speed_value_invalid",
        "litteralis.date_parsing_failed",
        "litteralis.missing_geometry",
    ):
        assert record in names, record
    assert len(PHP_CASES["standard-pagination"]["requests"]) == 2 + 3
    assert {case["fatal"]["class"] for case in ORACLE["cases"] if case["fatal"]} == {
        "Error",
        "ErrorException",
        "TypeError",
    }


def test_the_url_validator_judges_as_symfony_does():
    assert sorted(ORACLE["urls"]) == sorted(URLS)
    assert {url: is_valid_https_url(url) for url in URLS} == ORACLE["urls"]
