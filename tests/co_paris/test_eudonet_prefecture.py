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
    integration.organization_settings.paris_prefecture_client_id = None
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


def test_each_organization_keeps_its_own_total_shift():
    city = IntegrationOutcome("co_paris")
    city.total_shift = {"dialog": [900, 600], "produced": [900, 900]}
    prefecture = IntegrationOutcome("co_paris_prefecture")
    prefecture.total_shift = {"dialog": [500, 100], "produced": [500, 500]}
    out = paris.combine(city, prefecture)
    assert out.total_shift == city.total_shift
    assert out.source_alerts == [
        f"{paris.PREFECTURE_LABEL} : changement important du nombre total d'arrêtés de "
        "l'organisation dans DiaLog : 500 → 100 (produits par la pipeline : 500 → 500)"
    ]


def test_the_prefecture_checks_the_rules_the_shared_decision_applied(integration, monkeypatch):
    applied = {}
    original = paris.Eudonet._apply_rules_to_canonical

    def spy(self, df):
        applied[self.organization_settings.organization] = set(self._applied_codes)
        return original(self, df)

    monkeypatch.setattr(paris.Eudonet, "_apply_rules_to_canonical", spy)
    integration.integrate_regulations()
    city, prefecture = applied["co_paris"], applied[paris.PREFECTURE_ORGANIZATION]
    assert city and prefecture == city


def test_a_failed_read_is_not_tried_again_for_the_prefecture(integration, monkeypatch):
    reads = []

    def unreadable(client, today, articles_of=None, locations_of=None):
        reads.append(today)
        raise RuntimeError("Eudonet")

    monkeypatch.setattr(data_source_integration, "extract_tables", unreadable)
    outcome = integration.integrate_regulations()
    assert len(reads) == 1
    assert "eudonet" in outcome.failed_sources
