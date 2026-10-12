"""The WFS stand-in, and the PHP behaviours the port reproduces (values from PHP 8.4)."""

from datetime import datetime, timezone

import httpx
import pytest

from integrations.dialog.litteralis import php
from integrations.dialog.litteralis.client import TYPENAME_STANDARD, LitteralisClient
from integrations.dialog.litteralis.extractor import build_cql_filter
from integrations.dialog.litteralis.simulation import simulated_transport

LATER_THAN = datetime(2026, 10, 12, 0, 0, tzinfo=timezone.utc)


def feature(mesures: str | None, arretefin: str | None) -> dict:
    return {"geometry": None, "properties": {"mesures": mesures, "arretefin": arretefin}}


def client(features: list[dict]) -> LitteralisClient:
    http = httpx.Client(
        base_url="https://apps.sogelink.fr",
        transport=simulated_transport({TYPENAME_STANDARD: features}),
    )
    return LitteralisClient(http, TYPENAME_STANDARD, "u:p", sleep=lambda _: None)


def test_the_stand_in_evaluates_the_filter_the_extractor_builds():
    features = [
        feature("SOGELINK - Circulation interdite", None),
        feature("Limitation de VITESSE;Déviation", "2026-12-01T00:00:00Z"),
        feature("Interdiction de dépasser", "2026-10-11T23:59:59Z"),  # ended
        feature("Déviation", None),  # no known measure
        feature(None, None),
    ]
    cql_filter = build_cql_filter("mesures", LATER_THAN)

    assert client(features).count(None) == 5
    assert client(features).count(cql_filter) == 2
    assert [
        f["properties"]["mesures"] for f in client(features).fetch_all_paginated(cql_filter)
    ] == [
        "SOGELINK - Circulation interdite",
        "Limitation de VITESSE;Déviation",
    ]


def test_the_stand_in_paginates_as_geoserver():
    features = [feature("Circulation interdite", None) for _ in range(1001)]
    cql_filter = build_cql_filter("mesures", LATER_THAN)
    assert len(client(features).fetch_all_paginated(cql_filter)) == 1001


def test_the_cql_filter_is_the_back_ends():
    assert build_cql_filter("mesures", datetime(2026, 10, 12, 14, 0, tzinfo=timezone.utc)) == (
        "(mesures ILIKE '%circulation interdite%' OR mesures ILIKE '%limitation de vitesse%' "
        "OR mesures ILIKE '%interruption de circulation%' OR mesures ILIKE "
        "'%interdiction de stationnement%' OR mesures ILIKE '%interdiction de dépasser%') "
        "AND (arretefin IS NULL OR arretefin >= '2026-10-12T14:00:00+0000')"
    )


def test_a_retried_status_is_retried_three_times_then_raises():
    answers = iter([503, 503, 503, 503])
    http = httpx.Client(
        base_url="https://apps.sogelink.fr",
        transport=httpx.MockTransport(lambda request: httpx.Response(next(answers))),
    )
    delays = []
    with pytest.raises(httpx.HTTPStatusError):
        LitteralisClient(http, TYPENAME_STANDARD, "u:p", sleep=delays.append).count()
    assert delays == [2.0, 6.0, 18.0]


@pytest.mark.parametrize(
    "value, encoded",
    [
        (1e14, "100000000000000"),
        (1e16, "10000000000000000"),
        (1e17, "1.0e+17"),
        (123456789012345678.0, "1.2345678901234568e+17"),
        (1234567890123456.7, "1234567890123456.8"),
        (0.0001, "0.0001"),
        (0.00001, "1.0e-5"),
        (1.5e-5, "1.5e-5"),
        (-0.0, "-0"),
        (0.0, "0"),
        (100.0, "100"),
        (50.6878190776, "50.6878190776"),
        (5e-324, "5.0e-324"),
        (
            ["a😀b", "\x01\x1f\x7f", '"\\/', "é", " "],
            '["a\\ud83d\\ude00b","\\u0001\\u001f\x7f","\\"\\\\\\/","\\u00e9","\\u2028"]',
        ),
        ({}, "[]"),
        ({"a": []}, '{"a":[]}'),
        ({"1": "x"}, '{"1":"x"}'),
        ({"0": "x", "1": "y"}, '["x","y"]'),
    ],
)
def test_json_encode_writes_what_php_writes(value, encoded):
    assert php.json_encode(value) == encoded


@pytest.mark.parametrize(
    "text, parsed",
    [
        ("2023-10-31T01:00:00Z", "2023-10-31T01:00:00+00:00"),
        ("2023-10-31T01:00:00+0200", "2023-10-31T01:00:00+02:00"),
        ("2023-1-5T1:00:00Z", "2023-01-05T01:00:00+00:00"),
        ("2023-02-30T01:00:00Z", "2023-03-02T01:00:00+00:00"),
        ("2023-13-31T01:00:00Z", "2024-01-31T01:00:00+00:00"),
        ("2023-00-00T01:00:00Z", "2022-11-30T01:00:00+00:00"),
        ("2023-10-31T24:60:60Z", "2023-11-01T01:01:00+00:00"),
        ("2023-10-31T01:00:00Europe/Paris", "2023-10-31T01:00:00+01:00"),
        ("2023-10-31T01:00:00 Z", "2023-10-31T01:00:00+00:00"),
        ("2023-10-31T01:00:00.000Z", None),
        ("2023-10-31T01:0:00Z", None),
        ("2023-10-31", None),
        ("2023-10-31T01:00:00", None),
        ("2023-10-31T01:00:00Z ", None),
        ("+2023-10-31T01:00:00Z", None),
    ],
)
def test_create_from_format_parses_what_php_parses(text, parsed):
    result = php.create_from_iso8601(text)
    assert (result.isoformat() if result else None) == parsed


def test_trim_and_falsy_follow_php():
    assert php.trim(" x\t\0\x0b ") == " x"
    assert php.is_falsy("0") and php.is_falsy("") and not php.is_falsy("00")
    assert php.loose_equals("1e1", "10") and php.loose_equals(" 1", "1 ")
    assert not php.loose_equals("abc", "ABC")
