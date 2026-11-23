"""The frozen Eudonet extract shared by the Paris tests. Not a test module."""

import json
from pathlib import Path
from typing import Any

from integrations.co_paris.eudonet import data_source_integration
from integrations.co_paris.eudonet.data_source_integration import (
    DataSourceIntegration,
)
from integrations.co_paris.eudonet.published_elsewhere import (
    index_by_eudonet_number,
    load_frozen_elsewhere,
)
from integrations.co_paris.eudonet.raw_rows import Expansion

FIXTURE = Path("tests/co_paris/eudonet.json")


def load_fixture(path: Path = FIXTURE) -> dict[str, list[dict[str, Any]]]:
    """The tables of a frozen extract: same row shape as the API, same code downstream."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload["tables"]


def extract_offline(path: Path, client, today, articles_of, locations_of):
    """`extract_tables` on the frozen extract: the same choice of articles and locations."""
    tables = load_fixture(path)
    articled = articles_of(tables)
    return tables, Expansion(locations_of(tables, articled), articled)


def read_eudonet_from_the_fixture(monkeypatch, path: Path = FIXTURE) -> None:
    """Make `fetch_raw_data` read the frozen extract instead of calling Eudonet and DiaLog;
    the numbers published elsewhere come from the frozen list alone."""
    monkeypatch.setattr(DataSourceIntegration, "_credentials", lambda self: {})
    monkeypatch.setattr(
        data_source_integration, "extract_tables", lambda *args: extract_offline(path, *args)
    )
    monkeypatch.setattr(
        DataSourceIntegration,
        "fetch_numbers_published_elsewhere",
        lambda self: index_by_eudonet_number(load_frozen_elsewhere()),
    )
