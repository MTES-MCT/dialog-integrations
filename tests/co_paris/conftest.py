import pytest
from loguru import logger

from run.organization import BaseIntegration
from settings import OrganizationSettings
from tests.co_paris.eudonet_fixture import read_eudonet_from_the_fixture


class OfflineSettings:
    base_url = "http://offline.invalid"
    client_id = "offline"
    client_secret = "offline"
    pref_client_id = "offline-prefecture"
    pref_client_secret = "offline-prefecture"


@pytest.fixture
def captured_regulations():
    return []


@pytest.fixture
def created_by_organization():
    return {}


@pytest.fixture
def integration(monkeypatch, captured_regulations, created_by_organization, tmp_path):
    read_eudonet_from_the_fixture(monkeypatch)
    monkeypatch.setenv("DIALOG_STATE_DIR", str(tmp_path))
    settings = OrganizationSettings(OfflineSettings(), "co_paris")  # type: ignore[arg-type]
    integration = BaseIntegration.from_settings(settings)

    def capture(self, regulations):
        captured_regulations.extend(regulations)
        created_by_organization.setdefault(self.organization, []).extend(
            regulation.identifier for regulation in regulations
        )
        return [regulation.identifier for regulation in regulations], []

    # On the class: the police prefecture's organization is another instance of it.
    monkeypatch.setattr(type(integration), "_integrate_regulations_add", capture)
    monkeypatch.setattr(type(integration), "fetch_regulation_ids", lambda self: [])
    return integration


@pytest.fixture
def captured_logs():
    """Loguru does not go through `logging`: collect its records ourselves."""
    collected: list[str] = []
    handle = logger.add(collected.append, level="INFO", format="{message}")
    yield collected
    logger.remove(handle)
