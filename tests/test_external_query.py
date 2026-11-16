import contextlib
from types import SimpleNamespace

import httpx
import pytest
import requests

import external_query
from dialog.calls import DialogApi
from dialog.client import build_client


def _response(status: int) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    return response


def _requests_get_answering(monkeypatch, *answers):
    calls = []

    def get(url, **kwargs):
        calls.append(kwargs)
        answer = answers[len(calls) - 1]
        if isinstance(answer, Exception):
            raise answer
        return answer

    monkeypatch.setattr(external_query.requests, "get", get)
    return calls


@pytest.mark.parametrize(
    "failure",
    [
        requests.ConnectTimeout("connect timed out"),
        requests.exceptions.ChunkedEncodingError("IncompleteRead"),
        _response(500),
        _response(429),
    ],
)
def test_a_transient_failure_is_retried(monkeypatch, failure):
    calls = _requests_get_answering(monkeypatch, failure, _response(200))

    assert external_query.get("https://example.org").status_code == 200
    assert len(calls) == 2
    assert calls[0]["timeout"] == (10, 600)


@pytest.mark.parametrize(
    "failure", [_response(404), requests.exceptions.SSLError("certificate expired")]
)
def test_a_lasting_failure_is_not_retried(monkeypatch, failure):
    calls = _requests_get_answering(monkeypatch, failure, _response(200))

    with pytest.raises(requests.RequestException):
        external_query.get("https://example.org")
    assert len(calls) == 1


def test_three_attempts_then_the_last_error(monkeypatch):
    calls = _requests_get_answering(
        monkeypatch, *(requests.ConnectTimeout(f"try {n}") for n in range(1, 5))
    )

    with pytest.raises(requests.ConnectTimeout, match="try 3"):
        external_query.get("https://example.org")
    assert len(calls) == 3


def test_a_run_that_spent_its_retries_fails_at_once(monkeypatch):
    monkeypatch.setattr(external_query, "_retries_left", 1)
    calls = _requests_get_answering(
        monkeypatch, *(requests.ConnectTimeout("down") for _ in range(4))
    )

    for _ in range(2):
        with pytest.raises(requests.ConnectTimeout):
            external_query.get("https://example.org")
    assert len(calls) == 3


class _CutBody(httpx.SyncByteStream):
    def __iter__(self):
        yield b'{"identif'
        raise httpx.ReadError("connection reset")


def _dialog_client_over_network(monkeypatch, *answers) -> tuple[DialogApi, list]:
    sent = []

    def network(self, request):
        answer = answers[len(sent)]
        sent.append(request.method)
        if isinstance(answer, Exception):
            raise answer
        if answer == "cut":
            return httpx.Response(200, stream=_CutBody(), request=request)
        return httpx.Response(answer, json={"identifiers": ["MGL-1"]}, request=request)

    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", network)
    settings = SimpleNamespace(base_url="https://dialog.example", client_id="id", client_secret="s")
    return DialogApi(build_client(settings)), sent  # type: ignore


@pytest.mark.parametrize("failures", [[500, 502], ["cut"], [httpx.ReadTimeout("silent")]])
def test_a_dialog_read_is_retried(monkeypatch, failures):
    api, sent = _dialog_client_over_network(monkeypatch, *failures, 200)

    assert api.identifiers() == ["MGL-1"]
    assert sent == ["GET"] * (len(failures) + 1)


@pytest.mark.parametrize(
    "first, expected",
    [
        (httpx.ConnectTimeout("no connection"), ["POST", "POST"]),
        (500, ["POST"]),
        (httpx.ReadTimeout("silent"), ["POST"]),
    ],
)
def test_a_dialog_write_is_resent_only_when_it_never_left(monkeypatch, first, expected):
    api, sent = _dialog_client_over_network(monkeypatch, first, 201)

    with contextlib.suppress(httpx.ReadTimeout):
        api.client.get_httpx_client().post("/api/regulations/add")
    assert sent == expected
