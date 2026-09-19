"""The API wrapper turns each call into a boolean, whatever the client does."""

from types import SimpleNamespace

import pytest

from api.dia_log_client.models import PostApiRegulationsAddBody
from integrations import api as api_module
from integrations.api import DialogApi


def regulation() -> PostApiRegulationsAddBody:
    return PostApiRegulationsAddBody.from_dict(
        {"identifier": "X-1", "status": "draft", "title": "t", "measures": []}
    )


def response(status_code: int, content: bytes = b"{}"):
    return SimpleNamespace(status_code=status_code, content=content, parsed=None)


@pytest.fixture
def api():
    return DialogApi(client=None)  # type: ignore[arg-type]


def test_add_is_true_only_on_201(api, monkeypatch):
    monkeypatch.setattr(api_module, "add_regulation", lambda client, body: response(201))
    assert api.add(regulation()) is True

    monkeypatch.setattr(api_module, "add_regulation", lambda client, body: response(400))
    assert api.add(regulation()) is False


def test_a_raising_call_is_false_not_an_exception(api, monkeypatch):
    def boom(client, body):
        raise RuntimeError("network")

    monkeypatch.setattr(api_module, "add_regulation", boom)
    assert api.add(regulation()) is False


def test_a_5xx_answer_is_checked_with_a_get(api, monkeypatch):
    # The staging's router answers 504 after 60 s while the back end commits (D-20).
    monkeypatch.setattr(api_module, "add_regulation", lambda client, body: response(504))
    monkeypatch.setattr(api, "get", lambda identifier: {"identifier": identifier})
    assert api.add(regulation()) is True

    monkeypatch.setattr(api, "get", lambda identifier: None)
    assert api.add(regulation()) is False


def test_a_4xx_answer_is_a_refusal_without_a_get(api, monkeypatch):
    monkeypatch.setattr(api_module, "add_regulation", lambda client, body: response(400))

    def forbidden(identifier):
        raise AssertionError("a refusal must not be second-guessed")

    monkeypatch.setattr(api, "get", forbidden)
    assert api.add(regulation()) is False


def test_delete_is_true_only_on_204(api, monkeypatch):
    monkeypatch.setattr(api_module, "delete_regulation", lambda identifier, client: response(204))
    assert api.delete("X-1") is True

    monkeypatch.setattr(api_module, "delete_regulation", lambda identifier, client: response(404))
    assert api.delete("X-1") is False


def test_identifiers_raise_when_unreadable(api, monkeypatch):
    monkeypatch.setattr(api_module, "get_identifiers", lambda client: response(200))
    with pytest.raises(Exception, match="Failed to fetch identifiers"):
        api.identifiers()

    parsed = SimpleNamespace(identifiers=["X-1", "X-2"])
    monkeypatch.setattr(
        api_module, "get_identifiers", lambda client: SimpleNamespace(parsed=parsed)
    )
    assert api.identifiers() == ["X-1", "X-2"]
