"""Offline end-to-end run of the co_paris integration, on the frozen fixture.

Equivalent to `tests/test_integration.py` for the other organizations, except that the raw
data comes from the fixture pointed at by `EUDONET_PARIS_FIXTURE` rather than a CSV. The
POST is mocked out: nothing ever reaches DiaLog from a test.
"""

import pytest

from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.base_integration import BaseIntegration
from integrations.co_paris.eudonet.data_source_integration import FIXTURE_ENV_VAR
from settings import OrganizationSettings

FIXTURE = "tests/co_paris/eudonet.json"


class OfflineSettings:
    """No credential is needed: the API client is built but never called."""

    base_url = "http://offline.invalid"
    client_id = "offline"
    client_secret = "offline"


@pytest.fixture
def captured_regulations():
    """Whatever the integration would have POSTed. Nothing leaves the process."""
    return []


@pytest.fixture
def integration(monkeypatch, captured_regulations):
    monkeypatch.setenv(FIXTURE_ENV_VAR, FIXTURE)
    settings = OrganizationSettings(OfflineSettings(), "co_paris")  # type: ignore[arg-type]
    integration = BaseIntegration.from_settings(settings)

    def capture(regulations):
        captured_regulations.extend(regulations)
        # The base class reads back the identifiers it managed to create.
        return [regulation.identifier for regulation in regulations]

    monkeypatch.setattr(integration, "_integrate_regulations_add", capture)
    monkeypatch.setattr(integration, "fetch_regulation_ids", lambda: [])
    return integration


def test_the_pipeline_runs_from_the_fixture_to_the_pivot(integration):
    """fetch → validate → clean → select, without touching the network."""
    for data_source in integration.data_sources:
        source = data_source(integration.organization_settings, integration.client)

        raw = source.fetch_raw_data()
        validated = source.validate_raw_data(raw)
        clean = source.compute_clean_data(validated)
        selected = source.select_regulation_measure_fields(clean)

        assert raw.height > 0
        assert validated.height == raw.height
        # The transformations filter: a location DiaLog cannot geocode, a measure outside
        # the five types and a date that fails R-38 all leave here.
        assert 0 < clean.height < raw.height
        assert selected.height == clean.height
        assert selected.width > 0
        assert selected["regulation_identifier"].null_count() == 0


def test_the_whole_integration_runs_offline(integration, captured_regulations):
    """`integrate_regulations()` end to end: the payloads are built, none is sent."""
    integration.integrate_regulations()

    assert captured_regulations, "no regulation reached the (mocked) creation step"
    identifiers = [regulation.identifier for regulation in captured_regulations]
    assert len(identifiers) == len(set(identifiers))
    assert all(identifier.startswith("PARIS-EUDO-") for identifier in identifiers)
    assert all(len(identifier) <= 60 for identifier in identifiers)

    for regulation in captured_regulations:
        assert regulation.status == PostApiRegulationsAddBodyStatus.DRAFT
        assert regulation.title and len(regulation.title) <= 255
        assert regulation.other_category_text is None or (
            len(regulation.other_category_text) <= 100
        )
        assert regulation.measures
        for measure in regulation.measures:
            body = measure.to_dict()
            assert body["type"] in {
                "alternateRoad",
                "noEntry",
                "noOvertaking",
                "parkingProhibited",
                "speedLimitation",
            }
            assert body["periods"][0]["startDate"].endswith(("+01:00", "+02:00"))
            assert body["locations"][0]["roadType"] == "lane"
            vehicle_set = body["vehicleSet"]
            if "other" in vehicle_set.get("restrictedTypes", []):
                assert vehicle_set.get("otherRestrictedTypeText")
            if "other" in vehicle_set.get("exemptedTypes", []):
                assert vehicle_set.get("otherExemptedTypeText")
            if body["type"] == "speedLimitation":
                assert body["maxSpeed"] > 0
