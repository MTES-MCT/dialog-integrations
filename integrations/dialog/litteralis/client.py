"""Port of the back end's `LitteralisClient`: Litteralis' WFS (GeoServer), one layer,
paginated 500 features at a time, HTTP Basic authentication.

The HTTP behaviour is the one Symfony's client was configured with: 120 s timeout, and
up to three retries of a GET answered 429, 500, 502 or 503, or not answered at all,
after 2 s, 6 s and 18 s. Any other failure, or a failure after the last retry, raises
and aborts the import of the organization, as in PHP.
"""

import base64
import json
import math
import re
import time
from typing import Any

import httpx
from loguru import logger

from integrations.dialog.litteralis import php

TYPENAME_STANDARD = "litteralis:litteralis"
TYPENAME_COMMUNICATION = "litteralis:LIcommunication"
WFS_PATH = "/maplink/public/wfs"
NUM_PER_PAGE = 500

TIMEOUT = httpx.Timeout(120)
RETRIED_STATUSES = frozenset({429, 500, 502, 503})
MAX_RETRIES = 3
RETRY_DELAY_S = 2.0
RETRY_MULTIPLIER = 3
RETRY_MAX_DELAY_S = 30.0


class LitteralisClient:
    def __init__(
        self,
        http: httpx.Client,
        typename: str,
        credentials: str,
        sleep=time.sleep,
    ):
        if php.is_falsy(credentials):
            raise RuntimeError("Credentials are empty")
        self.http = http
        self.typename = typename
        self.credentials = credentials
        self.sleep = sleep

    def _get(self, query: dict[str, Any]) -> dict | None:
        """One GET on the WFS; the decoded body, or None when it is not JSON.

        PHP's `json_decode` returns null on a body it cannot parse, and every read that
        follows uses `??`: an unreadable page is an empty page, silently.
        """
        # `auth_basic` takes "user:password" as one string and encodes it whole.
        token = base64.b64encode(self.credentials.encode()).decode()
        headers = {"Authorization": f"Basic {token}"}
        params = {name: str(value) for name, value in query.items()}

        response = self._get_with_retries(params, headers)
        # Symfony's getContent() throws on any status from 300 up.
        if response.status_code >= 300:
            raise httpx.HTTPStatusError(
                f"Litteralis WFS answered HTTP {response.status_code}",
                request=response.request,
                response=response,
            )

        try:
            decoded = json.loads(response.content)
        except ValueError:
            return None
        return decoded if isinstance(decoded, dict) else None

    def _get_with_retries(self, params: dict, headers: dict) -> httpx.Response:
        attempt = 0
        while True:
            try:
                response = self.http.get(WFS_PATH, params=params, headers=headers)
            except httpx.TransportError as error:
                if attempt == MAX_RETRIES:
                    raise
                logger.warning(f"Litteralis WFS unreachable ({error}), retrying")
            else:
                if response.status_code not in RETRIED_STATUSES or attempt == MAX_RETRIES:
                    return response
                logger.warning(f"Litteralis WFS answered {response.status_code}, retrying")
            self.sleep(min(RETRY_DELAY_S * RETRY_MULTIPLIER**attempt, RETRY_MAX_DELAY_S))
            attempt += 1

    def fetch_all_paginated(self, cql_filter: str) -> list[dict]:
        features: list[dict] = []
        total_pages: float = math.inf
        page_number = 1

        while page_number <= total_pages:
            page = self._get(
                {
                    "outputFormat": "application/json",
                    "SERVICE": "wfs",
                    "VERSION": "2",
                    "REQUEST": "GetFeature",
                    "TYPENAME": self.typename,
                    "cql_filter": cql_filter,
                    "count": NUM_PER_PAGE,
                    "startIndex": NUM_PER_PAGE * (page_number - 1),
                }
            )
            page = page or {}

            if total_pages == math.inf:
                total = _php_number(page.get("totalFeatures"), "totalFeatures")
                total_pages = max(int(math.ceil(total / NUM_PER_PAGE)), 1)

            features.extend(page.get("features") or [])
            page_number += 1

        return features

    def count(self, cql_filter: str | None = None) -> int:
        query: dict[str, Any] = {
            "outputFormat": "application/json",
            "SERVICE": "wfs",
            "VERSION": "2",
            "REQUEST": "GetFeature",
            "TYPENAME": self.typename,
            "count": 1,
            "startIndex": 0,
        }
        if cql_filter:
            query["cql_filter"] = cql_filter
        page = self._get(query) or {}
        return _php_int_cast(page.get("numberMatched"))


def _php_number(value: Any, name: str) -> float:
    """A WFS count as PHP's arithmetic reads it; null counts as 0.

    GeoServer may answer `"totalFeatures": "unknown"`: PHP 8 refuses arithmetic on a
    non-numeric string with a TypeError, which aborts the import.
    """
    if value is None:
        return 0
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str) and php.NUMERIC_STRING.match(value):
        return php.to_number(value)
    raise php.PhpFatalError("TypeError", f"Unsupported operand types: {name} / int")


def _php_int_cast(value: Any) -> int:
    """PHP `(int) $value`: a cast never fails, a non-numeric string is 0."""
    if value is None or value is False:
        return 0
    if value is True:
        return 1
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        match = re.match(r"^[ \t\n\r\v\f]*[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?", value)
        return int(float(match.group(0))) if match else 0
    return 1
