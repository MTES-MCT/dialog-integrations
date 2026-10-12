"""A stand-in for Litteralis' WFS, for running the import without Sogelink credentials.

It answers the requests `LitteralisClient` sends, from a JSON file of features, the
way GeoServer does: it evaluates the CQL filter the extractors build (measure name
ILIKE patterns, regulation end date), counts, and paginates with `count` and
`startIndex`. Only that filter's grammar is understood; anything else is refused, so
a change in the filter cannot go unnoticed.

The file holds one list of features per layer:

    {"litteralis:litteralis": [...features...], "litteralis:LIcommunication": [...]}
"""

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import httpx

from integrations.dialog.litteralis.client import WFS_PATH

_FILTER = re.compile(
    r"^\((?P<measures>(?P<field>mesures?) ILIKE '[^']*'(?: OR (?P=field) ILIKE '[^']*')*)\)"
    r" AND \(arretefin IS NULL OR arretefin >= '(?P<later_than>[^']+)'\)$"
)


def load_layers(path: str | Path) -> dict[str, list[dict]]:
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def simulated_transport(layers: dict[str, list[dict]]) -> httpx.MockTransport:
    def handle(request: httpx.Request) -> httpx.Response:
        if request.url.path != WFS_PATH:
            return httpx.Response(404)
        query = dict(request.url.params)
        if query.get("REQUEST") != "GetFeature" or query.get("outputFormat") != "application/json":
            return httpx.Response(400, text="Unsupported WFS request")
        if not request.headers.get("Authorization", "").startswith("Basic "):
            return httpx.Response(401)

        features = layers.get(query.get("TYPENAME", ""), [])
        cql_filter = query.get("cql_filter")
        if cql_filter is not None:
            features = [f for f in features if _matches(f, cql_filter)]

        start = int(query.get("startIndex", 0))
        count = int(query.get("count", len(features)))
        page = features[start : start + count]
        return httpx.Response(
            200,
            json={
                "type": "FeatureCollection",
                "features": page,
                "totalFeatures": len(features),
                "numberMatched": len(features),
                "numberReturned": len(page),
            },
        )

    return httpx.MockTransport(handle)


def _matches(feature: dict, cql_filter: str) -> bool:
    match = _FILTER.match(cql_filter)
    if not match:
        raise ValueError(f"CQL filter not understood by the simulation: {cql_filter}")
    properties = feature.get("properties") or {}

    value = properties.get(match["field"])
    patterns = re.findall(r"ILIKE '([^']*)'", match["measures"])
    if value is None or not any(_ilike(str(value), pattern) for pattern in patterns):
        return False

    end = properties.get("arretefin")
    if end is None:
        return True
    later_than = datetime.strptime(match["later_than"], "%Y-%m-%dT%H:%M:%S%z")
    try:
        end_date = datetime.fromisoformat(str(end))
    except ValueError:
        # GeoServer cannot compare what is not a date: the feature does not match.
        return False
    if end_date.tzinfo is None:
        end_date = end_date.replace(tzinfo=timezone.utc)
    return end_date >= later_than


def _ilike(value: str, pattern: str) -> bool:
    """SQL `ILIKE`: `%` any run of characters, `_` any one, case ignored."""
    regex = "".join(
        ".*" if char == "%" else "." if char == "_" else re.escape(char) for char in pattern
    )
    return re.fullmatch(regex, value, re.IGNORECASE | re.DOTALL) is not None
