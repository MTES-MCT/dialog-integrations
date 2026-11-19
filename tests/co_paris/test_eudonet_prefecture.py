from integrations.co_paris import integration as paris
from integrations.co_paris.eudonet import data_source_integration
from run.organization import BaseIntegration
from sync.outcome import FAILED, IntegrationOutcome
from tests.co_paris.eudonet_fixture import load_fixture


def test_each_authority_goes_to_its_own_organization(integration, created_by_organization):
    integration.integrate_regulations()
    city = set(created_by_organization["co_paris"])
    prefecture = set(created_by_organization[paris.PREFECTURE_ORGANIZATION])
    assert city and prefecture and not city & prefecture

    created_by_organization.clear()
    BaseIntegration.integrate_regulations(integration)
    # Together, the two give what one organization alone publishes.
    assert set(created_by_organization["co_paris"]) == city | prefecture


def test_eudonet_is_read_once_for_both(integration, monkeypatch):
    reads = []

    def extract(client, today, articles_of=None, locations_of=None):
        reads.append(today)
        return load_fixture()

    monkeypatch.setattr(data_source_integration, "extract_tables", extract)
    integration.integrate_regulations()
    assert len(reads) == 1


def test_without_its_credentials_the_prefecture_is_left_out(
    integration, created_by_organization, captured_logs
):
    integration.organization_settings.pref_client_id = None
    integration.integrate_regulations()
    assert list(created_by_organization) == ["co_paris"]
    assert any("left out, rather than published under the city's name" in m for m in captured_logs)


def test_a_failed_prefecture_run_leaves_the_city_run_whole(
    integration, created_by_organization, monkeypatch
):
    def unreadable(self):
        if self.organization == paris.PREFECTURE_ORGANIZATION:
            raise RuntimeError("401")
        return []

    # `from_settings` loads the module afresh: patch the instance's own class.
    monkeypatch.setattr(type(integration), "fetch_regulation_ids", unreadable)
    outcome = integration.integrate_regulations()
    assert list(created_by_organization) == ["co_paris"]
    assert outcome.frozen_sources == {paris.PREFECTURE_LABEL: FAILED}
    assert outcome.failed_sources == [paris.PREFECTURE_LABEL]


def test_the_two_outcomes_add_up():
    city = IntegrationOutcome("co_paris", created=3, closed=None, planned={"create": 3})
    city.source_alerts = ["texte libre"]
    prefecture = IntegrationOutcome("co_paris_prefecture", created=2, closed=1)
    prefecture.planned = {"create": 2, "close": 1}
    prefecture.source_alerts = ["texte libre"]
    prefecture.frozen_sources = {"eudonet": FAILED}
    out = paris.combine(city, prefecture)
    assert (out.organization, out.created, out.closed) == ("co_paris", 5, 1)
    assert out.planned == {"create": 5, "close": 1}
    assert out.source_alerts == ["texte libre"]
    assert out.frozen_sources == {"eudonet (Préfecture de Police)": FAILED}


def test_a_failed_read_is_not_tried_again_for_the_prefecture(integration, monkeypatch):
    reads = []

    def unreadable(client, today, articles_of=None, locations_of=None):
        reads.append(today)
        raise RuntimeError("Eudonet")

    monkeypatch.setattr(data_source_integration, "extract_tables", unreadable)
    outcome = integration.integrate_regulations()
    assert len(reads) == 1
    assert "eudonet" in outcome.failed_sources
