"""The Tchap bot: configuration and the Matrix API call, whatever the message says."""

from uuid import UUID

import pytest
import requests

from notifications.tchap_bot import TchapBot

TCHAP_ENV_VARS = ("TCHAP_HOMESERVER_URL", "TCHAP_ACCESS_TOKEN", "TCHAP_ROOM_ID")


class _FakeResponse:
    def __init__(self, status_code: int = 200):
        self.status_code = status_code

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return {"event_id": "$event:example.org"}


@pytest.fixture
def bot(monkeypatch):
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    return TchapBot(
        # The trailing slash must be absorbed, otherwise the URL has a double slash.
        homeserver_url="https://matrix.agent.dinum.tchap.gouv.fr/",
        access_token="token",
        room_id="!abc:agent.dinum.tchap.gouv.fr",
    )


@pytest.fixture
def without_configuration(monkeypatch):
    for name in TCHAP_ENV_VARS:
        monkeypatch.delenv(name, raising=False)

    def forbidden(*args, **kwargs):
        raise AssertionError("no network call may happen without configuration")

    monkeypatch.setattr("notifications.tchap_bot.requests.put", forbidden)


def test_quotes_coming_from_env_files_are_stripped():
    # Some loaders export .env contents verbatim, quotes included. Without stripping,
    # the URL ends up holding %22 and requests rejects the scheme.
    configured = TchapBot(
        homeserver_url='"https://matrix.example.org/"',
        access_token='"syt_token"',
        room_id='"!abc:example.org"',
    )

    assert configured.homeserver_url == "https://matrix.example.org"
    assert configured.access_token == "syt_token"
    assert configured.room_id == "!abc:example.org"


def test_empty_value_counts_as_missing(monkeypatch):
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("TCHAP_HOMESERVER_URL", '""')
    monkeypatch.setenv("TCHAP_ACCESS_TOKEN", "token")
    monkeypatch.setenv("TCHAP_ROOM_ID", "!abc:example.org")

    assert TchapBot().missing_configuration() == ["TCHAP_HOMESERVER_URL"]
    with pytest.raises(RuntimeError, match="TCHAP_HOMESERVER_URL"):
        TchapBot().send("hello")


def test_sending_calls_the_matrix_api(bot, monkeypatch):
    captured = {}

    def fake_put(url, headers=None, json=None, timeout=None):
        captured.update(url=url, headers=headers, payload=json, timeout=timeout)
        return _FakeResponse()

    monkeypatch.setattr("notifications.tchap_bot.requests.put", fake_put)
    event_id = bot.send("hello", "<p>hello</p>")

    assert event_id == "$event:example.org"
    prefix, _, transaction_id = captured["url"].rpartition("/")
    assert prefix == (
        "https://matrix.agent.dinum.tchap.gouv.fr"
        "/_matrix/client/v3/rooms/%21abc%3Aagent.dinum.tchap.gouv.fr"
        "/send/m.room.message"
    )
    UUID(transaction_id)  # raises if the transaction id is not a UUID
    assert captured["headers"]["Authorization"] == "Bearer token"
    assert captured["payload"] == {
        "msgtype": "m.text",
        "body": "hello",
        "format": "org.matrix.custom.html",
        "formatted_body": "<p>hello</p>",
    }
    assert captured["timeout"] is not None


def test_a_plain_text_message_carries_no_format(bot, monkeypatch):
    captured = {}

    def fake_put(url, headers=None, json=None, timeout=None):
        captured.update(payload=json)
        return _FakeResponse()

    monkeypatch.setattr("notifications.tchap_bot.requests.put", fake_put)
    bot.send("hello")

    assert captured["payload"] == {"msgtype": "m.text", "body": "hello"}


def test_rejected_token_propagates_the_error(bot, monkeypatch):
    rejection = requests.Response()
    rejection.status_code = 401

    monkeypatch.setattr("notifications.tchap_bot.requests.put", lambda *a, **k: rejection)

    with pytest.raises(requests.HTTPError):
        bot.send("hello")


def test_missing_configuration_fails_in_ci(monkeypatch, without_configuration):
    monkeypatch.setenv("GITHUB_ACTIONS", "true")

    with pytest.raises(RuntimeError, match="TCHAP_HOMESERVER_URL"):
        TchapBot().send("hello")


def test_missing_configuration_is_skipped_locally(monkeypatch, without_configuration):
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)

    assert TchapBot().send("hello") is None
