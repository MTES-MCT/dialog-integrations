"""Run one simulated case through the Python port, in the PHP oracle's output layout.

The PHP oracle runs the back end's own Litteralis code (client, extractor,
transformer, period parser, reporter) on a case, behind a WFS stand-in that serves the
case's features as they are, and writes, per case: the WFS requests, one result per
regulation (the import command, or null), the reporter's records, and the throwable
that aborted the import, if any. `run_case` produces the same from this port, so that
the two can be compared value for value.
"""

import json
from datetime import date
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from integrations.dialog.litteralis.client import (
    TYPENAME_COMMUNICATION,
    TYPENAME_STANDARD,
    LitteralisClient,
)
from integrations.dialog.litteralis.commands import oracle_dump
from integrations.dialog.litteralis.executor import COMMUNICATION, transform_all
from integrations.dialog.litteralis.period_parser import LitteralisPeriodParser
from integrations.dialog.litteralis.php import PhpFatalError, json_encode
from integrations.dialog.litteralis.transformer import LitteralisTransformer

CREDENTIALS = "utilisateur:motdepasse"


def _stand_in(features: list[dict], requests: list[dict]) -> httpx.MockTransport:
    """The oracle's `FakeWfs`: no filtering, counts and pages over the case's features.

    Bodies are written by `json_encode`, as the oracle writes them: what PHP decodes
    then re-encodes is not always what it was given (`-0.0` comes back as `0`).
    """

    def handle(request: httpx.Request) -> httpx.Response:
        query = dict(request.url.params)
        requests.append({"method": request.method, "path": request.url.path, "query": query})
        matched = len(features)
        if int(query["count"]) == 1:
            return _php_json_response(
                {
                    "type": "FeatureCollection",
                    "features": [],
                    "totalFeatures": matched,
                    "numberMatched": matched,
                    "numberReturned": 0,
                }
            )
        start, count = int(query["startIndex"]), int(query["count"])
        page = features[start : start + count]
        return _php_json_response(
            {
                "type": "FeatureCollection",
                "features": page,
                "totalFeatures": matched,
                "numberMatched": matched,
                "numberReturned": len(page),
            }
        )

    return httpx.MockTransport(handle)


def _php_json_response(body: dict) -> httpx.Response:
    return httpx.Response(
        200, content=json_encode(body).encode(), headers={"Content-Type": "application/json"}
    )


def run_case(case: dict, today: date) -> dict:
    """The case through the port, as the oracle writes it (one entry of `cases`)."""
    from datetime import datetime

    requests: list[dict] = []
    flow = case["flow"]
    http = httpx.Client(
        base_url="https://apps.sogelink.fr",
        transport=_stand_in(json.loads(json.dumps(case["features"])), requests),
    )
    typename = TYPENAME_COMMUNICATION if flow == COMMUNICATION else TYPENAME_STANDARD
    client = LitteralisClient(http, typename, CREDENTIALS, sleep=lambda _: None)
    zone = ZoneInfo(case.get("zone") or "Europe/Paris")
    transformer = LitteralisTransformer(LitteralisPeriodParser(zone=zone, today=today))
    later_than = datetime.fromisoformat(case["laterThan"])

    results: list[dict] = []
    fatal = None
    reporter_records: list = []
    try:
        run = transform_all("simulee", flow, client, transformer, later_than)
        reporter_records = run.reporter.records
        results = [
            {
                "identifier": r.identifier,
                "features": len(r.features),
                "command": oracle_dump(r.command),
            }
            for r in run.regulations
        ]
    except PhpFatalError as error:
        fatal = {"class": error.php_class, "message": error.message}

    return json.loads(
        json.dumps(
            {
                "name": case["name"],
                "requests": requests,
                "results": results,
                "records": [list(record) for record in reporter_records],
                "fatal": fatal,
            },
            ensure_ascii=False,
        )
    )


def comparable_php_case(php_case: dict) -> dict:
    """The oracle's case, with query values as the strings that travel on the wire,
    the authentication dropped (checked separately) and, after a fatal error, the
    partial results and records dropped: PHP rolls everything back."""
    php_case = json.loads(json.dumps(php_case))
    for request in php_case["requests"]:
        request["query"] = {k: str(v) for k, v in request["query"].items()}
        request.pop("auth_basic", None)
    if php_case["fatal"]:
        php_case["results"], php_case["records"] = [], []
    return php_case


def comparable_port_case(port_case: dict) -> dict:
    if port_case["fatal"]:
        port_case = {**port_case, "results": [], "records": []}
    return port_case


def first_difference(left: Any, right: Any, path: str = "") -> str | None:
    """Where two JSON values first differ, or None when they are equal.

    An int and a float of the same value are equal: PHP widens the counts to float.
    """
    if isinstance(left, dict) and isinstance(right, dict):
        for key in list(left) + [k for k in right if k not in left]:
            if key not in left or key not in right:
                return f"{path}.{key}: present on one side only"
            found = first_difference(left[key], right[key], f"{path}.{key}")
            if found:
                return found
        return None
    if isinstance(left, list) and isinstance(right, list):
        if len(left) != len(right):
            return f"{path}: {len(left)} items != {len(right)} items"
        for index, (a, b) in enumerate(zip(left, right)):
            found = first_difference(a, b, f"{path}[{index}]")
            if found:
                return found
        return None
    numbers = (int, float)
    same_kind = type(left) is type(right) or (
        isinstance(left, numbers)
        and isinstance(right, numbers)
        and not isinstance(left, bool)
        and not isinstance(right, bool)
    )
    return None if same_kind and left == right else f"{path}: {left!r} != {right!r}"
