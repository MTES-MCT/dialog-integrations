"""Test-wide safety nets."""

import socket
from datetime import date

import pytest

from toolkit.rules import period


@pytest.fixture(autouse=True)
def isolated_state_directory(monkeypatch, tmp_path):
    """Keep synchronization snapshots out of the working tree: never leave one, nor read
    yesterday's."""
    monkeypatch.setenv("DIALOG_STATE_DIR", str(tmp_path / "state"))


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    """Fail any test that opens a connection: its result would depend on the day's data."""

    def refuse(*args, **kwargs):
        raise RuntimeError("network access in a test")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    # A name lookup is network too, and fails on its own offline: a test whose code
    # swallows that failure would pass by accident.
    monkeypatch.setattr(socket, "getaddrinfo", refuse)


@pytest.fixture(autouse=True)
def rules_on_a_fixed_day(monkeypatch):
    """The period rules read the clock: fixed in January 2026, when the shared fixture rows
    are in force, so that they do not expire as the calendar moves. A test may set its own."""
    monkeypatch.setattr(period, "today_in_paris", lambda: date(2026, 1, 15))
