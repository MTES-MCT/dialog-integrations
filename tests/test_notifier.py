"""Tests for the Tchap notifier: message formatting and the Matrix API call."""

from uuid import UUID

import pytest
import requests

from notifications.notifier import TchapNotifier

# Deliberately out of order: GitHub matrix job outputs have no guaranteed order.
RESULTS = {
    "result_dp_sarthe": '{"success":false}',
    "result_co_brest": '{"success":true}',
}

TCHAP_ENV_VARS = ("TCHAP_HOMESERVER_URL", "TCHAP_ACCESS_TOKEN", "TCHAP_ROOM_ID")


class _FakeResponse:
    def __init__(self, status_code: int = 200):
        self.status_code = status_code

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return {"event_id": "$event:example.org"}


@pytest.fixture
def notifier(monkeypatch):
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    return TchapNotifier(
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

    monkeypatch.setattr("notifications.notifier.requests.put", forbidden)


def test_quotes_coming_from_env_files_are_stripped():
    # Some loaders export .env contents verbatim, quotes included. Without stripping,
    # the URL ends up holding %22 and requests rejects the scheme.
    configured = TchapNotifier(
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

    with pytest.raises(RuntimeError, match="TCHAP_HOMESERVER_URL"):
        TchapNotifier().send_notification(RESULTS)


def test_every_organization_is_listed_in_a_stable_order(notifier):
    body, formatted_body = notifier.format_message(RESULTS)

    assert "✅ co_brest : Importé avec succès" in body
    assert "❌ dp_sarthe : Erreur lors de l'import" in body
    assert body.index("co_brest") < body.index("dp_sarthe")
    assert formatted_body.index("co_brest") < formatted_body.index("dp_sarthe")


def test_keys_other_than_results_are_ignored(notifier):
    body, _ = notifier.format_message({**RESULTS, "run_url": "https://github.com/…"})

    assert "run_url" not in body


def test_unreadable_result_counts_as_a_failure(notifier):
    body, _ = notifier.format_message({"result_co_rennes": "not json"})

    assert "❌ co_rennes : Erreur lors de l'import" in body


def test_empty_report_is_flagged_as_an_anomaly(notifier):
    body, formatted_body = notifier.format_message({})

    assert "Aucun résultat d'intégration reçu." in body
    assert "Aucun résultat d'intégration reçu." in formatted_body


def test_html_is_escaped(notifier):
    _, formatted_body = notifier.format_message({"result_<script>": '{"success":true}'})

    assert "<script>" not in formatted_body
    assert "&lt;script&gt;" in formatted_body


def test_sending_calls_the_matrix_api(notifier, monkeypatch):
    captured = {}

    def fake_put(url, headers=None, json=None, timeout=None):
        captured.update(url=url, headers=headers, payload=json, timeout=timeout)
        return _FakeResponse()

    monkeypatch.setattr("notifications.notifier.requests.put", fake_put)
    notifier.send_notification(RESULTS)

    prefix, _, transaction_id = captured["url"].rpartition("/")
    assert prefix == (
        "https://matrix.agent.dinum.tchap.gouv.fr"
        "/_matrix/client/v3/rooms/%21abc%3Aagent.dinum.tchap.gouv.fr"
        "/send/m.room.message"
    )
    UUID(transaction_id)  # raises if the transaction id is not a UUID
    assert captured["headers"]["Authorization"] == "Bearer token"
    assert captured["payload"]["msgtype"] == "m.text"
    assert captured["payload"]["format"] == "org.matrix.custom.html"
    assert captured["timeout"] is not None


def test_rejected_token_propagates_the_error(notifier, monkeypatch):
    rejection = requests.Response()
    rejection.status_code = 401

    monkeypatch.setattr("notifications.notifier.requests.put", lambda *a, **k: rejection)

    with pytest.raises(requests.HTTPError):
        notifier.send_notification(RESULTS)


def test_missing_configuration_fails_in_ci(monkeypatch, without_configuration):
    monkeypatch.setenv("GITHUB_ACTIONS", "true")

    with pytest.raises(RuntimeError, match="TCHAP_HOMESERVER_URL"):
        TchapNotifier().send_notification(RESULTS)


def test_missing_configuration_is_skipped_locally(monkeypatch, without_configuration):
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)

    TchapNotifier().send_notification(RESULTS)
