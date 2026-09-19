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

    def get(self, identifier) -> dict | None:
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
        "datasets": [
            {
                "label": "temporaire",
                "sources": ["fake"],
                "regulations": 1,
                "measures": 1,
                "restrictions": 1,
                "retained": 1,
            }
        ],
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
    # The dataset still stated one restriction and the pipeline retained it.
    assert outcome.to_result()["datasets"][0] == {
        "label": "temporaire",
        "sources": ["fake"],
        "regulations": 0,
        "measures": 0,
        "restrictions": 1,
        "retained": 1,
    }


# --- Closure: a regulation that left the source keeps its history --------------------

from datetime import datetime  # noqa: E402
from zoneinfo import ZoneInfo  # noqa: E402

from tests.sync.test_closure import READ  # noqa: E402


def _read_of(identifier: str, end: str = "2026-10-20T21:59:00+00:00") -> dict:
    read = json.loads(json.dumps(READ))
    read["identifier"] = identifier
    read["measures"][0]["periods"][0]["endDateTime"] = end
    return read


class _ReadingApi(_RecordingApi):
    """The recording API plus GET, which the closure needs to rebuild the payload."""

    def __init__(self, reads: dict[str, dict], **kwargs):
        super().__init__(**kwargs)
        self.reads = reads
        self.read: list[str] = []
        self.put_bodies: list[dict] = []

    def get(self, identifier) -> dict | None:
        self.read.append(str(identifier))
        return self.reads.get(str(identifier))

    def update(self, regulation):
        self.put_bodies.append(regulation.to_dict())
        return super().update(regulation)


def _closing_org(monkeypatch, tmp_path, produced, remote, api, **attributes):
    return _build_integration(
        monkeypatch,
        tmp_path,
        measure_rows(produced),
        remote,
        api=api,
        identifier_prefix=PREFIX,
        close_missing=True,
        update_changed=True,
        **attributes,
    )


def test_what_left_the_source_is_closed_through_put_never_deleted(monkeypatch, tmp_path):
    api = _ReadingApi({f"{PREFIX}gone": _read_of(f"{PREFIX}gone")})
    integration = _closing_org(
        monkeypatch, tmp_path, [f"{PREFIX}a"], [f"{PREFIX}a", f"{PREFIX}gone", "LYON_X"], api
    )

    outcome = integration.integrate_regulations()

    assert api.deleted == []
    assert api.read == [f"{PREFIX}gone"] and api.put == [f"{PREFIX}gone"]
    period = api.put_bodies[0]["measures"][0]["periods"][0]
    yesterday = datetime.now(tz=ZoneInfo("Europe/Paris")).date().toordinal() - 1
    assert datetime.fromisoformat(period["endDate"]).date().toordinal() == yesterday
    assert period["endDate"][11:] in ("23:59:59+02:00", "23:59:59+01:00")
    assert outcome.closed == 1 and outcome.deleted == 0 and outcome.errors == 0
    assert outcome.to_result()["closed"] == 1
    # The closed regulation stays in the snapshot, with its new end, so tomorrow
    # knows it ended without reading it again.
    snapshot = _snapshot(tmp_path)
    assert set(snapshot) == {f"{PREFIX}a", f"{PREFIX}gone"}
    assert snapshot[f"{PREFIX}gone"]["measures"][0]["periods"][0]["endDate"] == period["endDate"]


def test_a_regulation_that_ended_on_its_own_needs_no_write(monkeypatch, tmp_path):
    api = _ReadingApi({f"{PREFIX}done": _read_of(f"{PREFIX}done", "2026-09-01T21:59:00+00:00")})
    integration = _closing_org(
        monkeypatch, tmp_path, [f"{PREFIX}a"], [f"{PREFIX}a", f"{PREFIX}done"], api
    )

    outcome = integration.integrate_regulations()

    assert api.read == [f"{PREFIX}done"] and api.put == [] and api.deleted == []
    assert outcome.closed == 1 and outcome.errors == 0
    assert set(_snapshot(tmp_path)) == {f"{PREFIX}a", f"{PREFIX}done"}


def test_an_already_closed_regulation_is_skipped_the_next_day(monkeypatch, tmp_path):
    api = _ReadingApi({f"{PREFIX}gone": _read_of(f"{PREFIX}gone")})
    integration = _closing_org(
        monkeypatch, tmp_path, [f"{PREFIX}a"], [f"{PREFIX}a", f"{PREFIX}gone"], api
    )
    integration.integrate_regulations()
    assert api.put == [f"{PREFIX}gone"]

    outcome = integration.integrate_regulations()

    # Second day: nothing read, nothing written, still remembered.
    assert api.read == [f"{PREFIX}gone"] and api.put == [f"{PREFIX}gone"]
    assert outcome.closed == 0
    assert outcome.planned == {"create": 0, "update": 0, "delete": 0, "close": 0}
    assert f"{PREFIX}gone" in _snapshot(tmp_path)


def test_a_failed_closure_is_retried_tomorrow(monkeypatch, tmp_path):
    api = _ReadingApi({f"{PREFIX}gone": _read_of(f"{PREFIX}gone")}, update_ok=False)
    integration = _closing_org(
        monkeypatch, tmp_path, [f"{PREFIX}a"], [f"{PREFIX}a", f"{PREFIX}gone"], api
    )

    outcome = integration.integrate_regulations()

    assert api.put == [f"{PREFIX}gone"] and api.deleted == []
    assert outcome.closed == 0 and outcome.errors == 1
    # Not in the snapshot: unknown tomorrow, so it is read and closed again.
    assert set(_snapshot(tmp_path)) == {f"{PREFIX}a"}


def test_a_reappearing_site_is_reopened_as_an_update(monkeypatch, tmp_path):
    api = _ReadingApi({f"{PREFIX}back": _read_of(f"{PREFIX}back")})
    gone = _closing_org(monkeypatch, tmp_path, [f"{PREFIX}a"], [f"{PREFIX}a", f"{PREFIX}back"], api)
    gone.integrate_regulations()
    assert api.put == [f"{PREFIX}back"]

    back = _closing_org(
        monkeypatch,
        tmp_path,
        [f"{PREFIX}a", f"{PREFIX}back"],
        [f"{PREFIX}a", f"{PREFIX}back"],
        api,
    )
    outcome = back.integrate_regulations()

    assert outcome.planned == {"create": 0, "update": 1, "delete": 0, "close": 0}
    assert api.put == [f"{PREFIX}back", f"{PREFIX}back"]


def test_a_dry_run_reports_closures_without_reading_anything(monkeypatch, tmp_path):
    api = _ReadingApi({})
    integration = _closing_org(
        monkeypatch, tmp_path, [f"{PREFIX}a"], [f"{PREFIX}a", f"{PREFIX}gone"], api
    )

    outcome = integration.integrate_regulations(dry_run=True)

    assert api.read == [] and api.put == []
    assert outcome.planned == {"create": 0, "update": 0, "delete": 0, "close": 1}
    assert "à clore : 1" in outcome.report
    assert f"  - {PREFIX}gone" in outcome.report


# --- Dating: an undated permanent period is dated when DiaLog is written (R-39) --------

from tests.sync.test_dating import _read, _read_measure  # noqa: E402


def _undated_rows(identifiers) -> pl.DataFrame:
    """Permanent rows the source could not date, in the pivot's column names."""
    return measure_rows(identifiers).with_columns(
        pl.lit(None, dtype=pl.String).alias("period_start_date"),
        pl.lit(None, dtype=pl.String).alias("period_end_date"),
        pl.lit(True).alias("period_is_permanent"),
    )


class _DatingApi(_ReadingApi):
    """The reading API plus the bodies of every POST."""

    def __init__(self, reads: dict[str, dict], **kwargs):
        super().__init__(reads, **kwargs)
        self.post_bodies: list[dict] = []

    def add(self, regulation):
        self.post_bodies.append(regulation.to_dict())
        return super().add(regulation)


def _today() -> str:
    return datetime.combine(
        datetime.now(tz=ZoneInfo("Europe/Paris")).date(),
        datetime.min.time(),
        ZoneInfo("Europe/Paris"),
    ).isoformat()


def test_a_created_regulation_starts_the_day_it_is_published(monkeypatch, tmp_path):
    api = _DatingApi({})
    integration = _build_integration(
        monkeypatch, tmp_path, _undated_rows([f"{PREFIX}a"]), [], api=api, identifier_prefix=PREFIX
    )

    integration.integrate_regulations()

    period = api.post_bodies[0]["measures"][0]["periods"][0]
    assert period["startDate"] == period["startTime"] == _today()
    assert api.read == []


def test_an_updated_regulation_keeps_the_date_dialog_holds(monkeypatch, tmp_path):
    api = _DatingApi({f"{PREFIX}a": _read(_read_measure(max_speed=50))})
    integration = _build_integration(
        monkeypatch,
        tmp_path,
        _undated_rows([f"{PREFIX}a"]),
        [f"{PREFIX}a"],
        api=api,
        identifier_prefix=PREFIX,
        update_changed=True,
    )
    # Yesterday's send, with the speed the source has since changed.
    old = {f"{PREFIX}a": compute_regulation_digest(build_regulation(f"{PREFIX}a", max_speed=50))}
    old[f"{PREFIX}a"]["measures"][0]["periods"][0]["startDate"] = None
    SnapshotStore("co_test", "fake", base_dir=tmp_path).save(old)

    outcome = integration.integrate_regulations()

    assert outcome.updated == 1 and api.read == [f"{PREFIX}a"]
    period = api.put_bodies[0]["measures"][0]["periods"][0]
    assert period["startDate"] == period["startTime"] == "2026-09-18T00:00:00+02:00"
    # The snapshot keeps the null: tomorrow's comparison never sees a date.
    assert _snapshot(tmp_path)[f"{PREFIX}a"]["measures"][0]["periods"][0]["startDate"] is None


def test_the_run_day_is_not_a_change(monkeypatch, tmp_path):
    api = _DatingApi({})
    integration = _build_integration(
        monkeypatch,
        tmp_path,
        _undated_rows([f"{PREFIX}a"]),
        [],
        api=api,
        identifier_prefix=PREFIX,
        update_changed=True,
    )
    integration.integrate_regulations()
    assert api.posted == [f"{PREFIX}a"]

    # The next run: same source, DiaLog now holds the regulation.
    monkeypatch.setattr(integration, "fetch_regulation_ids", lambda: [f"{PREFIX}a"])
    outcome = integration.integrate_regulations()

    assert outcome.planned == {"create": 0, "update": 0, "delete": 0}
    assert api.put == [] and api.read == []


def test_an_update_whose_dates_cannot_be_read_back_waits_for_tomorrow(monkeypatch, tmp_path):
    api = _DatingApi({})  # GET answers None: DiaLog could not be read.
    integration = _build_integration(
        monkeypatch,
        tmp_path,
        _undated_rows([f"{PREFIX}a"]),
        [f"{PREFIX}a"],
        api=api,
        identifier_prefix=PREFIX,
        update_changed=True,
    )
    old = {f"{PREFIX}a": compute_regulation_digest(build_regulation(f"{PREFIX}a", max_speed=50))}
    SnapshotStore("co_test", "fake", base_dir=tmp_path).save(old)

    outcome = integration.integrate_regulations()

    # Sending it would have restarted the regulation today.
    assert api.put == [] and outcome.updated == 0 and outcome.errors == 1
    # Previous fingerprint kept: detected again, identically, tomorrow.
    assert _snapshot(tmp_path) == old


def test_a_dated_regulation_is_updated_without_reading_anything(monkeypatch, tmp_path):
    api = _DatingApi({})
    integration = _build_integration(
        monkeypatch,
        tmp_path,
        measure_rows([f"{PREFIX}a"], max_speed=30),
        [f"{PREFIX}a"],
        api=api,
        identifier_prefix=PREFIX,
        update_changed=True,
    )
    old = {f"{PREFIX}a": compute_regulation_digest(build_regulation(f"{PREFIX}a", max_speed=50))}
    SnapshotStore("co_test", "fake", base_dir=tmp_path).save(old)

    outcome = integration.integrate_regulations()

    assert outcome.updated == 1 and api.put == [f"{PREFIX}a"] and api.read == []
