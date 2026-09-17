"""Test-wide safety nets."""

import pytest


@pytest.fixture(autouse=True)
def isolated_state_directory(monkeypatch, tmp_path):
    """Keep synchronization snapshots out of the working tree.

    An integration that opted into update detection writes `state/{org}/{source}.json.gz`
    at the end of a run. Tests must never leave one behind, nor read yesterday's.
    """
    monkeypatch.setenv("DIALOG_STATE_DIR", str(tmp_path / "state"))
