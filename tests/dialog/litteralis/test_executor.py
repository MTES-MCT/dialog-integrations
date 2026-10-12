"""The PHP executor's order over the API: transform everything, delete, create."""

import json
from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

from integrations.api import WriteFailure
from integrations.dialog import integration as dialog_integration
from integrations.dialog.integration import Integration
from integrations.dialog.litteralis.executor import _to_clean_up, select_cleanup
from tests.dialog.litteralis.simulated_cases import layers

LATER_THAN = datetime(2026, 10, 12, 10, 0, tzinfo=timezone.utc)


def remote(status="published", category="temporaryRegulation", ends=("2026-11-01T00:00:00+00:00",)):
    return {
        "status": status,
        "category": category,
        "measures": [{"periods": [{"endDateTime": end} for end in ends]}],
    }


@pytest.mark.parametrize(
    "regulation, cleaned",
    [
        (remote(), True),
        (remote(ends=("2026-10-12T10:00:00+00:00",)), True),  # `>=`
        (remote(ends=("2026-10-01T00:00:00+00:00", "2026-11-01T00:00:00+00:00")), True),
        (remote(ends=("2026-10-01T00:00:00+00:00",)), False),  # ended: kept as history
        (remote(ends=(None,)), False),  # no end at all: SQL MAX is null
        (remote(category="permanentRegulation", ends=(None,)), True),
        (remote(status="draft"), False),
    ],
)
def test_cleanup_follows_the_php_query(regulation, cleaned):
    assert _to_clean_up(regulation, LATER_THAN) is cleaned


class FakeApi:
    """Records the writes, in order; holds a few regulations of the organization."""

    def __init__(self, existing: dict[str, dict] | None = None, refuse: tuple[str, ...] = ()):
        self.existing = existing or {}
        self.refuse = refuse
        self.calls: list[tuple[str, str]] = []
        self.bodies: dict[str, dict] = {}
        self.write_failures: dict = {}

    def identifiers(self):
        return list(self.existing)

    def get(self, identifier):
        return self.existing.get(identifier)

    def delete(self, identifier, missing_is_gone=False):
        self.calls.append(("delete", identifier))
        if identifier in self.refuse:
            return False
        self.existing.pop(identifier, None)
        return True

    def add(self, body):
        identifier = str(body.identifier)
        self.calls.append(("add", identifier))
        if identifier in self.refuse or identifier in self.existing:
            self.write_failures[identifier] = WriteFailure(422)
            return False
        self.bodies[identifier] = body.to_dict()
        periods = [p for m in self.bodies[identifier]["measures"] for p in m["periods"]]
        self.existing[identifier] = remote(
            category=self.bodies[identifier]["category"],
            ends=[p.get("endDate") for p in periods],
        )
        return True

    def attach_url(self, identifier, url, title):
        self.calls.append(("attach", identifier))
        assert title == "dl.sogelink.fr"
        return True


def test_cleanup_only_reads_what_this_import_owns():
    api = FakeApi(
        {"117374#A-1": remote(), "MANUEL-1": remote(), "117374#A-2": remote(status="draft")}
    )
    assert select_cleanup(api, LATER_THAN, None) == ["117374#A-1"]  # type: ignore[arg-type]


@pytest.fixture
def simulated(tmp_path, monkeypatch):
    path = tmp_path / "litteralis.json"
    path.write_text(json.dumps(layers(), ensure_ascii=False), encoding="utf-8")
    monkeypatch.setenv("DIALOG_LITTERALIS_SIMULATION", str(path))
    monkeypatch.setenv("DIALOG_LITTERALIS_ENABLED_ORGS", '["simulee"]')
    monkeypatch.setenv("DIALOG_LITTERALIS_COMMUNICATION_ENABLED_ORGS", '["simulee2"]')
    return path


def integration(api: FakeApi) -> Integration:
    settings = SimpleNamespace(organization="dialog", extra={}, base_url="https://dialog.test")
    instance = Integration.__new__(Integration)
    instance.organization_settings = settings  # type: ignore[assignment]
    instance.api = api  # type: ignore[assignment]
    return instance


def test_a_simulated_run_deletes_then_creates_and_attaches_documents(simulated, monkeypatch):
    monkeypatch.setenv("DIALOG_LITTERALIS_COMMUNICATION_ENABLED_ORGS", "[]")
    api = FakeApi({"117374#OLD": remote()})

    outcome = integration(api).integrate_regulations()

    assert api.calls[0] == ("delete", "117374#OLD")
    kinds = [kind for kind, _ in api.calls]
    assert "delete" not in kinds[1:], "deletions all come before the first creation"
    assert outcome.deleted == 1
    assert outcome.created == kinds.count("add") > 5  # the refused one is never sent
    assert kinds.count("attach") > 0
    # Every document follows the creation of its own regulation.
    for index, (kind, identifier) in enumerate(api.calls):
        if kind == "attach":
            assert api.calls[index - 1] == ("add", identifier)
    assert all(body["status"] == "published" for body in api.bodies.values())
    assert "Import Litteralis" in outcome.report


def test_a_dry_run_reads_but_writes_nothing(simulated):
    api = FakeApi({"117374#OLD": remote()})

    outcome = integration(api).integrate_regulations(dry_run=True)

    assert api.calls == []
    assert outcome.planned["deletions"] == 2  # once per flow, as PHP cleans up per command
    assert outcome.planned["creations"] > 10


def test_a_failed_deletion_stops_the_organization_before_any_creation(simulated):
    api = FakeApi({"117374#OLD": remote()}, refuse=("117374#OLD",))

    outcome = integration(api).integrate_regulations()

    assert [kind for kind, _ in api.calls] == ["delete", "delete"]  # one per flow, both stop
    assert outcome.created == 0
    assert outcome.error_causes == {"import Litteralis en échec": 2}


def test_a_fatal_error_in_the_data_writes_nothing(tmp_path, monkeypatch):
    data = layers()
    for feature in data["litteralis:litteralis"]:
        feature["properties"]["parametresmesures"] = None  # TypeError in PHP
    path = tmp_path / "litteralis.json"
    path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setenv("DIALOG_LITTERALIS_SIMULATION", str(path))
    monkeypatch.setenv("DIALOG_LITTERALIS_ENABLED_ORGS", '["simulee"]')
    monkeypatch.delenv("DIALOG_LITTERALIS_COMMUNICATION_ENABLED_ORGS", raising=False)
    api = FakeApi({"117374#OLD": remote()})

    outcome = integration(api).integrate_regulations()

    assert api.calls == []
    assert outcome.error_causes == {"import Litteralis en échec": 1}


def test_an_invalid_document_url_refuses_the_regulation_without_sending_it(simulated, monkeypatch):
    data = layers()
    for feature in data["litteralis:litteralis"]:
        feature["properties"]["shorturl"] = "http://dl.sogelink.fr/?insecure"
    simulated.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    monkeypatch.setenv("DIALOG_LITTERALIS_COMMUNICATION_ENABLED_ORGS", "[]")
    api = FakeApi()

    outcome = integration(api).integrate_regulations()

    assert api.calls == []
    assert outcome.refused == outcome.produced > 0


def test_the_time_slots_sent_depend_on_the_day_of_the_run(simulated, monkeypatch):
    """The PHP import's DST behaviour, carried over: summer and winter runs differ."""
    monkeypatch.setenv("DIALOG_LITTERALIS_COMMUNICATION_ENABLED_ORGS", "[]")
    slots = {}
    for day in (date(2026, 7, 1), date(2026, 12, 1)):
        monkeypatch.setattr(
            dialog_integration,
            "LitteralisPeriodParser",
            lambda day=day: __import__(
                "integrations.dialog.litteralis.period_parser", fromlist=["x"]
            ).LitteralisPeriodParser(today=day),
        )
        api = FakeApi()
        integration(api).integrate_regulations()
        slots[day.month] = sorted(
            slot["startTime"]
            for body in api.bodies.values()
            for measure in body["measures"]
            for period in measure["periods"]
            for slot in period["timeSlots"]
        )
    assert slots[7] and slots[12] and slots[7] != slots[12]


def test_two_flows_sharing_one_organization_share_its_cleanup(simulated):
    """As in PHP with two names on one organization: the second cleanup deletes the
    regulations the first import just created."""
    api = FakeApi()

    integration(api).integrate_regulations()

    created_first = [i for kind, i in api.calls if kind == "add"]
    deleted_after = [i for kind, i in api.calls if kind == "delete"]
    assert created_first and set(deleted_after) & set(created_first)


def test_an_organization_with_its_own_key_writes_to_its_own_organization(simulated, monkeypatch):
    shared, own = FakeApi(), FakeApi()
    monkeypatch.setenv("DIALOG_LITTERALIS_ORG_SIMULEE2_CLIENT_ID", "id")
    monkeypatch.setenv("DIALOG_LITTERALIS_ORG_SIMULEE2_CLIENT_SECRET", "secret")
    monkeypatch.setattr(
        dialog_integration.LitteralisConfig,
        "api",
        lambda self, name, default, base_url: own if name in self.api_keys else default,
    )

    integration(shared).integrate_regulations()

    assert not [kind for kind, _ in shared.calls if kind == "delete"]
    assert [kind for kind, _ in own.calls if kind == "add"]


def test_a_regulation_the_api_refuses_is_a_rejection_and_the_run_goes_on(simulated, monkeypatch):
    monkeypatch.setenv("DIALOG_LITTERALIS_COMMUNICATION_ENABLED_ORGS", "[]")
    first = FakeApi()
    integration(first).integrate_regulations(dry_run=False)
    refused = next(identifier for kind, identifier in first.calls if kind == "add")
    api = FakeApi(refuse=(refused,))

    outcome = integration(api).integrate_regulations()

    assert outcome.rejections == 1
    assert outcome.created == len(first.bodies) - 1
    assert "1 en échec" in outcome.report
