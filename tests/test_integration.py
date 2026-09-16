"""Integration tests for all organizations - tests full pipeline with all data sources."""

import json
from types import SimpleNamespace

import polars as pl
import pytest

from integrations.base_integration import BaseIntegration


@pytest.mark.parametrize(
    "organization",
    [
        "co_brest",
    ],
)
def test_full_pipeline_integration(organization, monkeypatch):
    """Test the full pipeline with actual CSV data for all organizations, mocking only API calls."""
    # Create integration instance
    integration = BaseIntegration.from_organization(organization)

    # Get data sources and mock their fetch_raw_data to load actual CSV data
    for data_source_integration in integration.data_sources:

        def mock_fetch_raw_data(_, name=data_source_integration.name):
            return pl.read_csv(f"tests/{organization}/{name}.csv", separator=",")

        monkeypatch.setattr(data_source_integration, "fetch_raw_data", mock_fetch_raw_data)

    # Mock API-related methods
    monkeypatch.setattr(integration, "_integrate_regulations_add", lambda regs: ([], []))
    monkeypatch.setattr(integration, "fetch_regulation_ids", lambda: [])

    # Run the full pipeline
    integration.integrate_regulations()


# --- Synchronization: the three operations, the guard rails and --dry-run -----------

from integrations.base_data_source_integration import BaseDataSourceIntegration  # noqa: E402
from integrations.sync.reconciliation import IdentifierOutsidePrefixError  # noqa: E402
from integrations.sync.state import SnapshotStore, compute_regulation_digest  # noqa: E402
from tests.sync_fixtures import build_regulation, measure_rows  # noqa: E402

PREFIX = "MGL-CHP-"


class _RecordingApi:
    """Stands in for `DialogApi`: remembers every write, answers as configured."""

    def __init__(self, add_ok=True, update_ok=True, delete_ok=True):
        self.posted: list[str] = []
        self.put: list[str] = []
        self.deleted: list[str] = []
        self.add_ok, self.update_ok, self.delete_ok = add_ok, update_ok, delete_ok

    def add(self, regulation):
        self.posted.append(str(regulation.identifier))
        return self.add_ok

    def update(self, regulation):
        self.put.append(str(regulation.identifier))
        return self.update_ok

    def delete(self, identifier, *, missing_is_gone=False):
        self.deleted.append(str(identifier))
        return self.delete_ok

    def get(self, identifier):
        return None

    def publish(self, identifier):
        return True


def _build_integration(monkeypatch, tmp_path, frame, remote, api=None, **attributes):
    monkeypatch.setenv("DIALOG_STATE_DIR", str(tmp_path))

    class _Source(BaseDataSourceIntegration):
        name = "fake"

        def fetch_raw_data(self):
            return frame

        def validate_raw_data(self, raw_data):
            return raw_data

        def compute_clean_data(self, raw_data):
            return raw_data

    class _Integration(BaseIntegration):
        data_sources = [_Source]

    for name, value in attributes.items():
        setattr(_Integration, name, value)

    settings = SimpleNamespace(organization="co_test", env="dev")
    integration = _Integration(settings, client=None)  # type: ignore[arg-type]
    integration.api = api or _RecordingApi()  # type: ignore[assignment]
    monkeypatch.setattr(integration, "fetch_regulation_ids", lambda: list(remote))
    return integration


def _snapshot(tmp_path) -> dict:
    return SnapshotStore("co_test", "fake", base_dir=tmp_path).load() or {}


def test_an_update_goes_through_put_and_never_delete_then_post(monkeypatch, tmp_path):
    # DELETE then POST loses the regulation when the POST fails (D-06).
    store = SnapshotStore("co_test", "fake", base_dir=tmp_path)
    store.save(
        {f"{PREFIX}a": compute_regulation_digest(build_regulation(f"{PREFIX}a", max_speed=50))}
    )
    api = _RecordingApi()
    integration = _build_integration(
        monkeypatch,
        tmp_path,
        measure_rows([f"{PREFIX}a"], max_speed=30),
        [f"{PREFIX}a"],
        api=api,
        identifier_prefix=PREFIX,
        update_changed=True,
    )
    outcome = integration.integrate_regulations()

    assert api.put == [f"{PREFIX}a"]
    assert api.posted == [] and api.deleted == []
    assert outcome.updated == 1


def test_a_deletion_is_bounded_by_the_prefix(monkeypatch, tmp_path):
    api = _RecordingApi()
    integration = _build_integration(
        monkeypatch,
        tmp_path,
        measure_rows([f"{PREFIX}a"]),
        [f"{PREFIX}a", f"{PREFIX}gone", "LYON_2009RP05617"],
        api=api,
        identifier_prefix=PREFIX,
        delete_missing=True,
        update_changed=True,
    )
    outcome = integration.integrate_regulations()

    assert api.deleted == [f"{PREFIX}gone"]
    assert outcome.deleted == 1
    # The deleted regulation leaves the snapshot; the foreign one was never touched.
    assert set(_snapshot(tmp_path)) == {f"{PREFIX}a"}


def test_a_failed_identifier_fetch_stops_the_run(monkeypatch, tmp_path):
    api = _RecordingApi()
    integration = _build_integration(
        monkeypatch, tmp_path, measure_rows([f"{PREFIX}a"]), [], api=api, identifier_prefix=PREFIX
    )

    def boom():
        raise Exception("Failed to fetch identifiers")

    monkeypatch.setattr(integration, "fetch_regulation_ids", boom)

    # Falling back to "this organization holds nothing" would recreate the whole
    # corpus as duplicates.
    with pytest.raises(Exception, match="Failed to fetch identifiers"):
        integration.integrate_regulations()
    assert api.posted == []


def test_an_identifier_outside_the_prefix_stops_before_any_write(monkeypatch, tmp_path):
    api = _RecordingApi()
    integration = _build_integration(
        monkeypatch,
        tmp_path,
        measure_rows([f"{PREFIX}a", "LYON_2009RP05617"]),
        [],
        api=api,
        identifier_prefix=PREFIX,
    )

    with pytest.raises(IdentifierOutsidePrefixError):
        integration.integrate_regulations()
    assert api.posted == []


def test_a_dry_run_writes_nothing_at_all(monkeypatch, tmp_path):
    api = _RecordingApi()
    integration = _build_integration(
        monkeypatch,
        tmp_path,
        measure_rows([f"{PREFIX}a", f"{PREFIX}b"]),
        [f"{PREFIX}b", f"{PREFIX}gone"],
        api=api,
        identifier_prefix=PREFIX,
        delete_missing=True,
        update_changed=True,
    )

    outcome = integration.integrate_regulations(dry_run=True)

    assert (api.posted, api.put, api.deleted) == ([], [], [])
    assert not SnapshotStore("co_test", "fake", base_dir=tmp_path).exists()
    assert outcome.planned == {"create": 1, "update": 0, "delete": 1}
    assert outcome.created == outcome.updated == outcome.deleted == 0
    assert "Rapport de synchronisation" in outcome.report
    assert outcome.to_result()["dry_run"] is True


def test_the_snapshot_only_records_what_was_written(monkeypatch, tmp_path):
    # The API refuses everything: nothing may enter the snapshot, so the next run
    # tries again.
    integration = _build_integration(
        monkeypatch,
        tmp_path,
        measure_rows([f"{PREFIX}a"]),
        [],
        api=_RecordingApi(add_ok=False),
        identifier_prefix=PREFIX,
        update_changed=True,
    )

    outcome = integration.integrate_regulations()

    assert outcome.created == 0
    assert outcome.errors == 1
    assert _snapshot(tmp_path) == {}


def test_a_held_update_keeps_its_previous_fingerprint(monkeypatch, tmp_path):
    old = {
        f"{PREFIX}{i}": compute_regulation_digest(build_regulation(f"{PREFIX}{i}", max_speed=50))
        for i in range(3)
    }
    SnapshotStore("co_test", "fake", base_dir=tmp_path).save(old)
    api = _RecordingApi()
    integration = _build_integration(
        monkeypatch,
        tmp_path,
        measure_rows(list(old), max_speed=30),
        list(old),
        api=api,
        identifier_prefix=PREFIX,
        update_changed=True,
        max_updates_per_run=2,
    )
    outcome = integration.integrate_regulations()

    assert api.put == []
    assert outcome.held == {"update": 3}
    # Same fingerprints as yesterday: the batch is detected again, identically.
    assert _snapshot(tmp_path) == old


def test_the_json_result_carries_the_counters(monkeypatch, tmp_path):
    integration = _build_integration(
        monkeypatch,
        tmp_path,
        measure_rows([f"{PREFIX}a"]),
        [f"{PREFIX}gone"],
        identifier_prefix=PREFIX,
        delete_missing=True,
        update_changed=True,
    )

    result = integration.integrate_regulations().to_result()

    assert result == {
        "success": True,
        "created": 1,
        "updated": 0,
        "deleted": 1,
        "integrated": {"regulations": 1, "measures": 1, "rows": 1},
    }
    assert json.loads(json.dumps(result)) == result


def test_a_pipeline_refusal_is_counted_apart_from_failures(monkeypatch, tmp_path):
    integration = _build_integration(
        monkeypatch, tmp_path, measure_rows([f"{PREFIX}a"]), [], identifier_prefix=PREFIX
    )
    monkeypatch.setattr(integration, "_create_regulation", lambda regulation: "refused")

    outcome = integration.integrate_regulations()

    assert (outcome.created, outcome.refused, outcome.errors) == (0, 1, 0)
    assert outcome.to_result()["refused"] == 1
    # Built, then refused: not in DiaLog, so not counted as integrated.
    assert outcome.to_result()["integrated"] == {"regulations": 0, "measures": 0, "rows": 1}
