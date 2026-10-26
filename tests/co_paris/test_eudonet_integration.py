from datetime import date

import httpx
import pytest

from api.dia_log_client.models import PostApiRegulationsAddBodyStatus
from integrations.api import DialogApi, PublicExportUnreadable
from integrations.base_integration import BaseIntegration
from integrations.co_paris.eudonet import data_source_integration
from integrations.co_paris.eudonet.data_source_integration import (
    DataSourceIntegration,
)
from settings import OrganizationSettings
from tests.co_paris.eudonet_fixture import read_eudonet_from_the_fixture


class OfflineSettings:
    base_url = "http://offline.invalid"
    client_id = "offline"
    client_secret = "offline"


@pytest.fixture
def captured_regulations():
    return []


@pytest.fixture
def integration(monkeypatch, captured_regulations):
    read_eudonet_from_the_fixture(monkeypatch)
    settings = OrganizationSettings(OfflineSettings(), "co_paris")  # type: ignore[arg-type]
    integration = BaseIntegration.from_settings(settings)

    def capture(regulations):
        captured_regulations.extend(regulations)
        return [regulation.identifier for regulation in regulations], []

    monkeypatch.setattr(integration, "_integrate_regulations_add", capture)
    monkeypatch.setattr(integration, "fetch_regulation_ids", lambda: [])
    return integration


def _eudonet_source():
    settings = OrganizationSettings(OfflineSettings(), "co_paris")  # type: ignore[arg-type]
    return DataSourceIntegration(settings, BaseIntegration.from_settings(settings).client)


def test_a_municipality_the_export_cannot_serve_falls_back_on_its_last_read(monkeypatch):
    """R-80: a number held back yesterday is not created tonight because of a 504."""

    def first_night(self, insee_code):
        return [("2024T11570", "Paris")] if insee_code == "75104" else []

    def second_night(self, insee_code):
        if insee_code == "75104":
            raise httpx.ReadTimeout("no answer")
        return []

    monkeypatch.setattr(DialogApi, "public_identifiers", first_night)
    assert "2024T11570" in _eudonet_source().fetch_numbers_published_elsewhere()
    monkeypatch.setattr(DialogApi, "public_identifiers", second_night)
    assert "2024T11570" in _eudonet_source().fetch_numbers_published_elsewhere()


def test_a_municipality_never_read_stops_the_run_rather_than_create_duplicates(monkeypatch):
    """Read as empty, it would let the night create what another channel holds (R-80)."""

    def timeout(self, insee_code):
        if insee_code == "75104":
            raise httpx.ReadTimeout("no answer")
        return []

    monkeypatch.setattr(DialogApi, "public_identifiers", timeout)
    with pytest.raises(PublicExportUnreadable, match="75104"):
        _eudonet_source().fetch_numbers_published_elsewhere()


def test_an_export_of_another_shape_stops_the_run(monkeypatch):

    def changed_contract(self, insee_code):
        raise KeyError("metadata")

    monkeypatch.setattr(DialogApi, "public_identifiers", changed_contract)
    with pytest.raises(KeyError):
        _eudonet_source().fetch_numbers_published_elsewhere()


def test_the_pipeline_runs_from_the_fixture_to_the_pivot(integration):
    for data_source in integration.data_sources:
        source = data_source(integration.organization_settings, integration.client)

        raw = source.fetch_raw_data()
        validated = source.validate_raw_data(raw)
        clean = source.compute_clean_data(validated)
        selected = source.select_regulation_measure_fields(clean)

        assert raw.height > 0
        assert validated.height == raw.height
        assert 0 < clean.height < raw.height
        assert selected.height == clean.height
        assert selected.width > 0
        assert selected["regulation_identifier"].null_count() == 0


def test_the_whole_integration_runs_offline(integration, captured_regulations):
    integration.integrate_regulations()

    assert captured_regulations, "no regulation reached the (mocked) creation step"
    identifiers = [regulation.identifier for regulation in captured_regulations]
    assert len(identifiers) == len(set(identifiers))
    assert all(identifier.startswith("PARIS-EUDO-") for identifier in identifiers)
    assert all(len(identifier) <= 60 for identifier in identifiers)

    for regulation in captured_regulations:
        assert regulation.status == PostApiRegulationsAddBodyStatus.PUBLISHED
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


def _street(city_code, road_name, start=None, end=None):
    bounds = {}
    for side, bound in (("from", start), ("to", end)):
        number = bound if bound and bound[0].isdigit() else None
        crossing = bound if bound and not number else None
        bounds |= {
            f"{side}PointType": "houseNumber" if number else "intersection" if crossing else None,
            f"{side}HouseNumber": number,
            f"{side}RoadName": crossing,
        }
    return {
        "roadType": "lane",
        "namedStreet": {
            "cityCode": city_code,
            "cityLabel": "Paris",
            "roadName": road_name,
            "direction": "BOTH",
            **bounds,
        },
    }


def test_the_fixture_locations_reach_the_api_field_by_field(
    integration, captured_regulations, monkeypatch
):
    monkeypatch.setattr(data_source_integration, "today_in_paris", lambda: date(2026, 10, 23))
    integration.integrate_regulations()
    locations = {
        regulation.identifier: [
            location
            for measure in regulation.to_dict()["measures"]
            for location in measure["locations"]
        ]
        for regulation in captured_regulations
    }

    assert locations["PARIS-EUDO-2024T16585"] == [_street("75108", "Rue du Boccador", "12", "12")]
    assert locations["PARIS-EUDO-2026T11821"] == [
        _street("75104", "Rue du Cloître Notre-Dame", "8", "Rue Chanoinesse")
    ]
    assert locations["PARIS-EUDO-2005-154"] == [
        _street("75104", "Rue Saint-Merri", "Rue du Renard", "Rue du Temple")
    ]
