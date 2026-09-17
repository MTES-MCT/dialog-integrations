"""Unit tests for the Eudonet client. No network: a fake session answers every call."""

import pytest

from integrations.co_paris.eudonet import client as client_module
from integrations.co_paris.eudonet.client import (
    EQUALS,
    IN_LIST,
    INTER_AND,
    INTER_OR,
    EudonetClient,
    EudonetError,
    all_of,
    any_of,
    criterion,
    flatten_row,
)

CREDENTIALS = {"user": "someone", "password": "not-a-real-secret"}


def token_response(expires="2030-01-01T00:00:00"):
    return {
        "ResultInfos": {"Success": True},
        "ResultData": {"Token": "a-token", "ExpirationDate": expires},
    }


def rows_response(row_ids, total_rows=None, total_pages=None):
    payload = {
        "ResultInfos": {"Success": True},
        "ResultData": {
            "Rows": [
                {"FileId": row_id, "Fields": [{"DescId": 1101, "Value": f"A{row_id}"}]}
                for row_id in row_ids
            ]
        },
    }
    if total_rows is not None:
        payload["ResultMetaData"] = {"TotalRows": total_rows, "TotalPages": total_pages}
    return payload


def error_response(number):
    return {"ResultInfos": {"Success": False, "ErrorNumber": number, "ErrorMessage": "nope"}}


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class FakeSession:
    """Answers each POST with the next queued payload, and records every call."""

    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.calls = []

    def post(self, url, json=None, headers=None, timeout=None):
        self.calls.append({"url": url, "json": json, "headers": headers})
        return FakeResponse(self.payloads.pop(0))

    @property
    def search_bodies(self):
        return [call["json"] for call in self.calls if "/Search/" in call["url"]]

    @property
    def auth_calls(self):
        return [call for call in self.calls if call["url"].endswith("/Authenticate/Token")]


def make_client(payloads, **kwargs) -> EudonetClient:
    session = FakeSession(payloads)
    return EudonetClient(
        CREDENTIALS,
        base_url="https://eudonet.test",
        session=session,  # type: ignore[arg-type]
        **kwargs,
    )


# -- criteria ------------------------------------------------------------------------------


def test_criterion_stringifies_field_and_value():
    assert criterion(1108, EQUALS, 7) == {
        "WhereCustoms": None,
        "Criteria": {"Field": "1108", "Operator": EQUALS, "Value": "7"},
        "InterOperator": 0,
    }


def test_groups_nest_and_only_the_first_member_keeps_operator_zero():
    where = any_of(
        all_of(criterion(1108, EQUALS, "7"), criterion(1107, EQUALS, "96")),
        all_of(criterion(1108, EQUALS, "8"), any_of(criterion(1107, EQUALS, "8640"))),
    )

    assert where["Criteria"] is None
    groups = where["WhereCustoms"]
    assert [group["InterOperator"] for group in groups] == [0, INTER_OR]

    first = groups[0]["WhereCustoms"]
    assert [member["InterOperator"] for member in first] == [0, INTER_AND]

    nested = groups[1]["WhereCustoms"][1]
    assert nested["WhereCustoms"][0]["Criteria"]["Value"] == "8640"


def test_an_empty_group_is_refused():
    with pytest.raises(ValueError):
        all_of()


# -- row shape -----------------------------------------------------------------------------


def test_flatten_row_keeps_value_db_value_and_parent_file_id():
    row = {
        "FileId": 12,
        "Fields": [
            {"DescId": 1101, "Value": "2026T1", "DbValue": "2026T1", "FileId": 99},
            {"DescId": 1110, "Value": "25/06/2026", "DbValue": "2026/06/25 00:00:00"},
        ],
    }

    assert flatten_row(row) == {
        "FileId": 12,
        "1101": "2026T1",
        "1101_db": "2026T1",
        "1101_fid": 99,
        "1110": "25/06/2026",
        "1110_db": "2026/06/25 00:00:00",
    }


# -- paging --------------------------------------------------------------------------------


def test_search_all_reads_every_page_announced_by_the_metadata():
    client = make_client(
        [
            token_response(),
            rows_response([1, 2], total_rows=5, total_pages=3),
            rows_response([3, 4]),
            rows_response([5]),
        ]
    )

    rows = client.search_all(1100, [1101], criterion(1108, EQUALS, "7"))

    assert [row["FileId"] for row in rows] == [1, 2, 3, 4, 5]
    bodies = client.session.search_bodies  # type: ignore[attr-defined]
    assert [body["NumPage"] for body in bodies] == [1, 2, 3]
    # Only the first page pays for the metadata.
    assert [body["ShowMetadata"] for body in bodies] == [True, False, False]


def test_count_asks_for_a_single_row_and_returns_the_total():
    client = make_client([token_response(), rows_response([1], total_rows=4021, total_pages=81)])

    assert client.count(1100, criterion(1108, EQUALS, "7")) == 4021
    body = client.session.search_bodies[0]  # type: ignore[attr-defined]
    assert body["RowsPerPage"] == 1
    assert body["ShowMetadata"] is True


def test_search_by_ids_splits_the_identifiers_into_batches():
    ids = list(range(1, 8))
    client = make_client(
        [
            token_response(),
            rows_response([1], total_rows=1, total_pages=1),
            rows_response([2], total_rows=1, total_pages=1),
            rows_response([3], total_rows=1, total_pages=1),
        ]
    )

    rows = client.search_by_ids(2700, [2705], 1200, ids, chunk=3)

    assert [row["FileId"] for row in rows] == [1, 2, 3]
    values = [
        body["WhereCustom"]["Criteria"]["Value"]
        for body in client.session.search_bodies  # type: ignore[attr-defined]
    ]
    assert values == ["1;2;3", "4;5;6", "7"]
    operators = [
        body["WhereCustom"]["Criteria"]["Operator"]
        for body in client.session.search_bodies  # type: ignore[attr-defined]
    ]
    assert set(operators) == {IN_LIST}


# -- robustness ----------------------------------------------------------------------------


def test_an_expired_token_is_renewed_and_the_call_replayed():
    client = make_client(
        [
            token_response(),
            error_response(101),
            token_response(),
            rows_response([7], total_rows=1, total_pages=1),
        ]
    )

    rows = client.search_all(1100, [1101], criterion(1108, EQUALS, "7"))

    assert [row["FileId"] for row in rows] == [7]
    assert len(client.session.auth_calls) == 2  # type: ignore[attr-defined]


def test_the_quota_error_pauses_then_retries(monkeypatch):
    slept = []
    monkeypatch.setattr(client_module.time, "sleep", slept.append)
    client = make_client(
        [
            token_response(),
            error_response(300),
            rows_response([7], total_rows=1, total_pages=1),
        ]
    )

    rows = client.search_all(1100, [1101], criterion(1108, EQUALS, "7"))

    assert [row["FileId"] for row in rows] == [7]
    assert slept == [client_module.QUOTA_PAUSE_SECONDS]


def test_an_unknown_error_is_raised_immediately():
    client = make_client([token_response(), error_response(204)])

    with pytest.raises(EudonetError, match="204"):
        client.search_all(2700, [2705], criterion(1108, EQUALS, "7"))


def test_a_failed_authentication_never_echoes_the_credentials():
    client = make_client([{"ResultInfos": {"Success": False, "ErrorMessage": "bad login"}}])

    with pytest.raises(EudonetError) as raised:
        client.authenticate()

    assert "bad login" in str(raised.value)
    assert CREDENTIALS["password"] not in str(raised.value)


def test_the_token_is_renewed_before_it_expires(monkeypatch):
    client = make_client([token_response("2000-01-01T00:00:00"), token_response()])

    assert client.token == "a-token"
    # The stored expiration is in the past: the next read must re-authenticate.
    assert client.token == "a-token"
    assert len(client.session.auth_calls) == 2  # type: ignore[attr-defined]
