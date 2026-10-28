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

FIXTURE = Path("tests/co_paris/eudonet.json")


def load_fixture(path: Path = FIXTURE) -> dict[str, list[dict[str, Any]]]:
    """The tables of a frozen extract: same row shape as the API, same code downstream."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload["tables"]


def read_eudonet_from_the_fixture(monkeypatch, path: Path = FIXTURE) -> None:
    """Make `fetch_raw_data` read the frozen extract instead of calling Eudonet and DiaLog;
    the numbers published elsewhere come from the frozen list alone."""
    monkeypatch.setattr(DataSourceIntegration, "_credentials", lambda self: {})
    monkeypatch.setattr(
        data_source_integration, "extract_tables", lambda client, today: load_fixture(path)
    )
    monkeypatch.setattr(
        DataSourceIntegration,
        "fetch_numbers_published_elsewhere",
        lambda self: index_by_eudonet_number(load_frozen_elsewhere()),
    )
