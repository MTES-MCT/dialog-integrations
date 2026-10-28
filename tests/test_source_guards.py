"""A data source that fails, collapses or is paused freezes its own prefix, not the others'."""

from types import SimpleNamespace

import pytest

from integrations.base_data_source_integration import BaseDataSourceIntegration
from integrations.base_integration import BaseIntegration
from integrations.sync.reconciliation import IdentifierOutsidePrefixError, MissingPrefixError
from integrations.sync.state import SnapshotStore
from integrations.sync.totals import Totals, TotalsStore
from tests.sync_fixtures import measure_rows

ORG = "OR-"
A, B = "OR-A-", "OR-B-"


class _Api:
    def __init__(self):
        self.posted: list[str] = []
        self.deleted: list[str] = []

    def add(self, regulation):
        self.posted.append(str(regulation.identifier))
        return True

    def update(self, regulation):
        return True

    def delete(self, identifier, missing_is_gone=False):
        self.deleted.append(identifier)
        return True

    def get(self, identifier):
        return None


def _api(integration: BaseIntegration) -> _Api:
    return integration.api  # type: ignore[return-value]


def _source(name: str, prefix: str | None, rows=None, error: Exception | None = None):
    class _Source(BaseDataSourceIntegration):
        def fetch_raw_data(self):
            if error is not None:
                raise error
            return measure_rows(rows or [])

        def validate_raw_data(self, raw_data):
            return raw_data

        def compute_clean_data(self, raw_data):
            return raw_data

    _Source.name = name
    _Source.identifier_prefix = prefix
    return _Source


def _integration(monkeypatch, tmp_path, sources, remote, **attributes):
    monkeypatch.setenv("DIALOG_STATE_DIR", str(tmp_path))

    class _Integration(BaseIntegration):
        data_sources = sources
        identifier_prefix = ORG
        delete_missing = True

    for name, value in attributes.items():
        setattr(_Integration, name, value)

    settings = SimpleNamespace(organization="co_test", env="dev", base_url="https://test.invalid")
    integration = _Integration(settings, client=None)  # type: ignore[arg-type]
    integration.api = _Api()  # type: ignore[assignment]
    monkeypatch.setattr(integration, "fetch_regulation_ids", lambda: list(remote))
    return integration


REMOTE = [f"{A}kept", f"{A}gone", f"{B}kept", f"{B}gone"]


def test_a_failing_source_freezes_its_prefix_and_the_others_still_write(monkeypatch, tmp_path):
    integration = _integration(
        monkeypatch,
        tmp_path,
        [
            _source("a", A, [f"{A}kept", f"{A}new"]),
            _source("b", B, error=RuntimeError("export cut")),
        ],
        REMOTE,
    )

    outcome = integration.integrate_regulations()

    assert _api(integration).posted == [f"{A}new"]
    assert _api(integration).deleted == [f"{A}gone"]
    assert outcome.frozen_sources == {"b": "failed"}
    assert outcome.failed_sources == ["b"]
    assert outcome.errors == 0
    assert "export cut" in outcome.source_errors["b"]


def test_a_paused_source_is_not_read_and_nothing_in_it_is_removed(monkeypatch, tmp_path):
    read = []

    class _Paused(BaseDataSourceIntegration):
        name = "b"
        identifier_prefix = B

        def fetch_raw_data(self):
            read.append(True)
            return measure_rows([])

    integration = _integration(
        monkeypatch, tmp_path, [_source("a", A, [f"{A}kept"]), _Paused], REMOTE
    )
    integration.paused_sources = frozenset({"b"})

    outcome = integration.integrate_regulations()

    assert read == []
    assert _api(integration).deleted == [f"{A}gone"]
    assert outcome.frozen_sources == {"b": "paused"}
    assert outcome.failed_sources == []


def test_a_source_without_its_own_prefix_freezes_the_whole_organization(monkeypatch, tmp_path):
    integration = _integration(
        monkeypatch,
        tmp_path,
        [_source("a", A, [f"{A}kept"]), _source("b", None, error=RuntimeError("down"))],
        REMOTE,
    )

    integration.integrate_regulations()

    assert _api(integration).deleted == []


def test_a_collapsed_source_is_frozen_until_released(monkeypatch, tmp_path):
    rows = [f"{B}{i}" for i in range(10)]
    remote = [f"{A}kept"] + rows
    sources = [_source("a", A, [f"{A}kept"]), _source("b", B, rows[:2])]

    integration = _integration(monkeypatch, tmp_path, sources, remote)
    store = TotalsStore("co_test", target="test.invalid")
    store.save(Totals(dialog_in_prefix=11, produced=11, sources={"a": 1, "b": 10}))
    outcome = integration.integrate_regulations()

    assert _api(integration).deleted == []
    assert outcome.frozen_sources == {"b": "volume"}
    assert outcome.volume_drops == {"b": [10, 2]}
    # The trusted baseline survives the frozen run: tomorrow is judged against 10 again.
    assert store.load().sources == {"a": 1, "b": 10}  # type: ignore[union-attr]

    released = integration.integrate_regulations(force_deletions=True)

    assert sorted(_api(integration).deleted) == sorted(rows[2:])
    assert released.frozen_sources == {}
    assert store.load().sources == {"a": 1, "b": 2}  # type: ignore[union-attr]


def test_a_source_unknown_yesterday_is_not_judged_on_its_volume(monkeypatch, tmp_path):
    integration = _integration(
        monkeypatch, tmp_path, [_source("a", A, [f"{A}kept"])], [f"{A}kept", f"{A}gone"]
    )
    TotalsStore("co_test", target="test.invalid").save(Totals(2, 2, sources={}))

    outcome = integration.integrate_regulations()

    assert _api(integration).deleted == [f"{A}gone"]
    assert outcome.volume_drops == {}


def test_what_a_frozen_source_left_stays_in_the_snapshot(monkeypatch, tmp_path):
    from integrations.sync.state import compute_regulation_digest
    from tests.sync_fixtures import build_regulation

    digest = compute_regulation_digest(build_regulation(f"{B}kept"))
    monkeypatch.setenv("DIALOG_STATE_DIR", str(tmp_path))
    SnapshotStore("co_test", "b", target="test.invalid").save({f"{B}kept": digest})
    integration = _integration(
        monkeypatch,
        tmp_path,
        [_source("a", A, [f"{A}kept"]), _source("b", B, error=RuntimeError("down"))],
        [f"{A}kept", f"{B}kept"],
        delete_missing=False,
        close_missing=True,
        update_changed=True,
    )

    integration.integrate_regulations()

    assert SnapshotStore("co_test", "b", target="test.invalid").load() == {f"{B}kept": digest}


def test_an_organization_without_prefix_must_say_it_only_adds(monkeypatch, tmp_path):
    sources = [_source("a", None, ["X1"])]

    with pytest.raises(MissingPrefixError):
        _integration(
            monkeypatch, tmp_path, sources, [], identifier_prefix=None, delete_missing=False
        ).integrate_regulations()

    legacy = _integration(
        monkeypatch,
        tmp_path,
        sources,
        [],
        identifier_prefix=None,
        delete_missing=False,
        legacy_additive=True,
    )
    assert legacy.integrate_regulations().created == 1


def test_a_source_writing_outside_its_own_prefix_is_refused(monkeypatch, tmp_path):
    integration = _integration(
        monkeypatch,
        tmp_path,
        [_source("a", A, [f"{A}ok"]), _source("b", B, [f"{A}stray"])],
        [],
    )

    with pytest.raises(IdentifierOutsidePrefixError):
        integration.integrate_regulations()


def test_state_is_kept_per_target_and_the_older_path_is_still_read(tmp_path):
    old = SnapshotStore("co_test", "a", base_dir=tmp_path)
    old.save({"X": {"title": "t"}})

    store = SnapshotStore("co_test", "a", base_dir=tmp_path, target="dialog.beta.gouv.fr")
    assert store.path == tmp_path / "dialog.beta.gouv.fr" / "co_test" / "a.json.gz"
    assert store.load() == {"X": {"title": "t"}}

    store.save({"Y": {"title": "u"}})
    assert store.load() == {"Y": {"title": "u"}}
    assert SnapshotStore("co_test", "a", base_dir=tmp_path, target="other.host").load() == {
        "X": {"title": "t"}
    }


def test_totals_written_before_sources_were_counted_still_load(tmp_path):
    path = tmp_path / "co_test" / "totals.json"
    path.parent.mkdir(parents=True)
    path.write_text('{"dialog_in_prefix": 3, "produced": 2}')

    assert TotalsStore("co_test", base_dir=tmp_path, target="h").load() == Totals(3, 2)


def test_a_regulation_failing_a_check_is_left_whole_and_not_taken_for_gone(monkeypatch, tmp_path):
    import polars as pl

    class _Source(BaseDataSourceIntegration):
        name = "a"
        identifier_prefix = A

        def fetch_raw_data(self):
            rows = measure_rows([f"{A}bad", f"{A}bad", f"{A}new"])
            return rows.with_columns(measure_max_speed=pl.Series([30, None, 30], dtype=pl.Int64))

        def validate_raw_data(self, raw_data):
            return raw_data

        def compute_clean_data(self, raw_data):
            return raw_data

    integration = _integration(monkeypatch, tmp_path, [_Source], [f"{A}bad"])

    outcome = integration.integrate_regulations()

    assert _api(integration).posted == [f"{A}new"]
    assert _api(integration).deleted == []
    assert (outcome.refused, outcome.errors) == (1, 0)
