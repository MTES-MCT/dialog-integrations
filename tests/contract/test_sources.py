"""What every organization and data source must satisfy, whoever wrote it.

Parametrized over the folders of `integrations/` and over `integrations.toml`: a new
source is covered the day it is declared, without a line here.
"""

import importlib
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

import registry
from canonical.regulation_measure import unknown_canonical_columns
from checks import rows as checks
from run.organization import BaseIntegration
from settings import Organization
from sync.digest import compute_regulation_digest

ORGANIZATIONS = [o.name for o in Organization]  # type: ignore[attr-defined]


def _integration_class(organization: str) -> type[BaseIntegration]:
    return importlib.import_module(f"integrations.{organization}.integration").Integration


SOURCES = [
    (organization, source)
    for organization in ORGANIZATIONS
    for source in _integration_class(organization).data_sources
]


def _id(param) -> str:
    return param if isinstance(param, str) else param.name


@pytest.mark.parametrize("organization", ORGANIZATIONS)
def test_an_organization_has_a_prefix_or_declares_it_only_adds(organization):
    integration = _integration_class(organization)

    assert integration.identifier_prefix or integration.legacy_additive


@pytest.mark.parametrize("organization", ORGANIZATIONS)
def test_the_sources_of_an_organization_that_removes_have_their_own_prefixes(organization):
    """So one source failing, collapsing or paused freezes itself, not its neighbours."""
    integration = _integration_class(organization)
    names = [source.name for source in integration.data_sources]
    assert all(names) and len(set(names)) == len(names)

    if not (integration.delete_missing or integration.close_missing):
        return
    organization_prefix = integration.identifier_prefix or ""
    if len(integration.data_sources) == 1:
        prefix = integration.data_sources[0].identifier_prefix
        assert prefix is None or prefix.startswith(organization_prefix)
        return
    prefixes = [source.identifier_prefix or "" for source in integration.data_sources]
    assert all(prefixes), f"every source of {organization} needs its own identifier_prefix"
    for prefix in prefixes:
        assert prefix.startswith(organization_prefix)
        assert not any(o != prefix and o.startswith(prefix) for o in prefixes)


def test_the_registry_names_existing_organizations_and_sources():
    for entry in registry.load():
        assert entry.organization in ORGANIZATIONS
        names = {source.name for source in _integration_class(entry.organization).data_sources}
        assert set(entry.paused) <= names, f"{entry.organization}: unknown paused source"


# --- On the frozen fixture of each source -----------------------------------------
# `tests/{organization}/{source}.csv`. A source without one is not run here: its own
# tests are all there is (listed when pytest runs with -rs).

WITH_FIXTURE = [
    (organization, source)
    for organization, source in SOURCES
    if Path(f"tests/{organization}/{source.name}.csv").is_file()
]


@pytest.fixture(autouse=True)
def _no_reference_downloads(monkeypatch):
    """A territory's outline is replaced by one that keeps everything: the fixture is the
    subject here."""
    from shapely.geometry import box

    from toolkit.transforms.perimeter import Perimeter

    whole_world = Perimeter.build("epci", "0", [box(-180, -90, 180, 90)])
    monkeypatch.setattr(Perimeter, "fetch", classmethod(lambda cls, *a, **k: whole_world))


def _clean_data(organization, source_class) -> pl.DataFrame:
    settings = SimpleNamespace(organization=organization, env="dev", base_url=None)
    source = source_class(settings, client=None)
    raw = pl.read_csv(f"tests/{organization}/{source_class.name}.csv", separator=",")
    return source.compute_clean_data(source.validate_raw_data(raw))


@pytest.mark.parametrize("organization, source", WITH_FIXTURE, ids=_id)
def test_a_source_names_no_column_the_canonical_model_lacks(organization, source):
    clean = _clean_data(organization, source)

    assert unknown_canonical_columns(clean.columns) == []


@pytest.mark.parametrize("organization, source", WITH_FIXTURE, ids=_id)
def test_a_source_passes_the_common_checks(organization, source):
    instance = source(SimpleNamespace(organization=organization), client=None)
    canonical_rows = instance.select_regulation_measure_fields(_clean_data(organization, source))

    failing = checks.failing_rows(canonical_rows)

    assert failing.is_empty(), failing.group_by("reason").len().to_dicts()


@pytest.mark.parametrize("organization, source", WITH_FIXTURE, ids=_id)
def test_a_source_builds_the_same_regulations_twice_inside_its_prefix(organization, source):
    integration_class = _integration_class(organization)
    integration = integration_class(SimpleNamespace(organization=organization), client=None)  # type: ignore[arg-type]
    instance = source(SimpleNamespace(organization=organization), client=None)

    def build():
        canonical_rows = instance.select_regulation_measure_fields(
            _clean_data(organization, source)
        )
        unbuilt: set[str] = set()
        regulations = integration.create_regulations(canonical_rows, source, unbuilt)
        return {str(r.identifier): compute_regulation_digest(r) for r in regulations}, unbuilt

    first, unbuilt = build()
    second, _ = build()

    assert first, "the fixture builds no regulation"
    assert unbuilt == set()
    assert first == second
    prefix = source.identifier_prefix or integration_class.identifier_prefix
    if prefix:
        assert all(identifier.startswith(prefix) for identifier in first)
