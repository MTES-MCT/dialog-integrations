"""The API wrapper turns each call into a boolean, whatever the client does."""

from types import SimpleNamespace

import pytest

from api.dia_log_client.errors import UnexpectedStatus
from api.dia_log_client.models import PostApiRegulationsAddBody
from integrations import api as api_module
from integrations.api import DialogApi, WriteFailure, api_motive, split_failures


def regulation(identifier: str = "X-1") -> PostApiRegulationsAddBody:
    return PostApiRegulationsAddBody.from_dict(
        {"identifier": identifier, "status": "draft", "title": "t", "measures": []}
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


COMPETENCE = (
    b'{"status": 400, "detail": "L\'organisation \\"\\" ne semble pas avoir les '
    b"comp\xc3\xa9tences pour intervenir sur ce lin\xc3\xa9aire de route. S'il s'agit "
    b"d'une erreur, vous pouvez contacter le support DiaLog.\"}"
)
VIOLATIONS = (
    b'{"status": 422, "detail": "Validation failed", "violations": ['
    b'{"propertyPath": "vehicleSet.restrictedTypes", "title": "Veuillez sp\xc3\xa9cifier '
    b'le gabarit des v\xc3\xa9hicules concern\xc3\xa9s."}]}'
)


def test_the_motive_is_the_first_sentence_of_the_detail_or_the_first_violation():
    assert api_motive(COMPETENCE) == (
        'L\'organisation "" ne semble pas avoir les compétences pour intervenir sur ce '
        "linéaire de route"
    )
    assert api_motive(VIOLATIONS) == "Veuillez spécifier le gabarit des véhicules concernés"
    assert api_motive(b'{"violations": 5, "detail": "Refus. Suite"}') == "Refus"
    assert api_motive(b"<html>502</html>") == "réponse illisible"
    assert api_motive(b"null") == "sans motif"
    assert len(api_motive(b'{"detail": "' + b"x" * 300 + b'"}')) == 100


@pytest.mark.parametrize(
    ("status", "rejection"),
    [(422, True), (401, False), (429, False), (500, False), (None, False)],
)
def test_a_4xx_is_a_refusal_except_credentials_and_limits(status, rejection):
    assert WriteFailure(status, "m").is_rejection is rejection


def test_every_failed_write_is_remembered_with_its_status_and_motive(api, monkeypatch):
    def bad_gateway(identifier, client):
        raise UnexpectedStatus(502, b"<html>Bad gateway</html>")

    def timeout(client, body):
        raise TimeoutError("read timeout")

    monkeypatch.setattr(
        api_module, "add_regulation", lambda client, body: response(422, VIOLATIONS)
    )
    monkeypatch.setattr(api_module, "delete_regulation", bad_gateway)
    monkeypatch.setattr(api_module, "update_regulation", timeout)
    api.add(regulation("X-1"))
    api.delete("X-2")
    api.update(regulation("X-3"))

    assert api.write_failures == {
        "X-1": WriteFailure(422, "Veuillez spécifier le gabarit des véhicules concernés"),
        "X-2": WriteFailure(502, "réponse illisible"),
        "X-3": WriteFailure(None, "TimeoutError"),
    }


def test_failures_split_into_refusals_by_motive_and_outages_by_cause():
    failures = {
        "a": WriteFailure(400, "hors compétence"),
        "b": WriteFailure(400, "hors compétence"),
        "c": WriteFailure(422, "gabarit"),
        "d": WriteFailure(500, "x"),
        "e": WriteFailure(None, "TimeoutError"),
    }

    rejections, outages = split_failures(["a", "b", "c", "d", "e", "unrecorded"], failures)

    assert rejections == {"hors compétence": 2, "gabarit": 1}
    assert outages == {"HTTP 500": 1, "sans réponse": 1, "autre": 1}
