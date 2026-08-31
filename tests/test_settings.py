"""Tests de résolution des fichiers d'environnement (.env.{env} et .env.{org}.{env})."""

import pytest

from settings import OrganizationSettings, Settings

SHARED = """\
DIALOG_BASE_URL="https://shared.example"
DIALOG_CLIENT_ID="shared-id"
DIALOG_CLIENT_SECRET="shared-secret"
"""

ORG_ONLY = """\
DIALOG_BASE_URL="https://org.example"
DIALOG_CLIENT_ID="org-id"
DIALOG_CLIENT_SECRET="org-secret"
"""

ORG_PARTIAL = """\
DIALOG_CLIENT_ID="org-id"
DIALOG_CLIENT_SECRET="org-secret"
"""


@pytest.fixture(autouse=True)
def isolated_cwd(tmp_path, monkeypatch):
    """Isole la résolution : répertoire vide et aucune variable DIALOG_ héritée du shell."""
    for name in ("DIALOG_BASE_URL", "DIALOG_CLIENT_ID", "DIALOG_CLIENT_SECRET"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_org_file_alone_is_still_used(isolated_cwd):
    """Comportement historique : le fichier de l'organisation suffit, sans fichier commun."""
    (isolated_cwd / ".env.co_maville.dev").write_text(ORG_ONLY)

    settings = Settings(organization="co_maville", env="dev")

    assert settings.base_url == "https://org.example"
    assert settings.client_id == "org-id"


def test_shared_file_alone_is_used(isolated_cwd):
    """Nouveau : `.env.{env}` seul suffit, sans fichier par organisation."""
    (isolated_cwd / ".env.dev").write_text(SHARED)

    settings = Settings(organization="co_maville", env="dev")

    assert settings.base_url == "https://shared.example"
    assert settings.client_id == "shared-id"


def test_org_file_overrides_shared_file(isolated_cwd):
    """Le fichier de l'organisation écrase le fichier commun, clé par clé."""
    (isolated_cwd / ".env.dev").write_text(SHARED)
    (isolated_cwd / ".env.co_maville.dev").write_text(ORG_PARTIAL)

    settings = Settings(organization="co_maville", env="dev")

    # base_url n'est pas redéfini par l'organisation : il vient du fichier commun.
    assert settings.base_url == "https://shared.example"
    assert settings.client_id == "org-id"
    assert settings.client_secret == "org-secret"


def test_shared_file_is_scoped_to_its_env(isolated_cwd):
    """`.env.prod` ne doit pas alimenter une résolution en dev."""
    (isolated_cwd / ".env.prod").write_text(SHARED)

    settings = Settings(organization="co_maville", env="dev")

    assert settings.base_url is None


def test_no_shared_fallback_in_prod(isolated_cwd):
    """Le repli partagé est réservé à dev : en prod, `.env.prod` doit être ignoré."""
    (isolated_cwd / ".env.prod").write_text(SHARED)

    settings = Settings(organization="co_maville", env="prod")

    assert settings.base_url is None
    assert settings.client_id is None


def test_prod_uses_only_the_organization_file(isolated_cwd):
    """En prod, l'organisation doit porter la totalité de ses identifiants."""
    (isolated_cwd / ".env.prod").write_text(SHARED)
    (isolated_cwd / ".env.co_maville.prod").write_text(ORG_ONLY)

    settings = Settings(organization="co_maville", env="prod")

    # Aucune valeur de SHARED ne doit transparaître, même sur une clé absente de ORG_ONLY.
    assert settings.base_url == "https://org.example"
    assert settings.client_id == "org-id"
    assert settings.client_secret == "org-secret"


def test_prod_organization_without_file_fails(isolated_cwd):
    """Pas d'emprunt d'identité en prod : une organisation sans fichier doit échouer."""
    (isolated_cwd / ".env.prod").write_text(SHARED)

    with pytest.raises(Exception, match="base_url"):
        OrganizationSettings.from_env("co_maville", env="prod")


def test_process_env_wins_over_files(isolated_cwd, monkeypatch):
    """La CI n'a aucun fichier : les variables du processus doivent primer."""
    (isolated_cwd / ".env.dev").write_text(SHARED)
    monkeypatch.setenv("DIALOG_BASE_URL", "https://ci.example")

    settings = Settings(organization="co_maville", env="dev")

    assert settings.base_url == "https://ci.example"
    assert settings.client_id == "shared-id"


def test_no_file_leaks_between_organizations(isolated_cwd):
    """Une organisation sans fichier ne doit pas hériter de celui de la précédente."""
    (isolated_cwd / ".env.co_maville.dev").write_text(ORG_ONLY)

    first = Settings(organization="co_maville", env="dev")
    second = Settings(organization="co_autreville", env="dev")

    assert first.client_id == "org-id"
    assert second.client_id is None


def test_organization_settings_reports_missing_values(isolated_cwd):
    """Un fichier commun incomplet doit échouer explicitement, pas silencieusement."""
    (isolated_cwd / ".env.dev").write_text('DIALOG_BASE_URL="https://shared.example"\n')

    with pytest.raises(Exception, match="client_id"):
        OrganizationSettings.from_env("co_maville", env="dev")
