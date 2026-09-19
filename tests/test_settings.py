"""Resolution of the environment files (`.env.{env}` and `.env.{org}.{env}`)."""

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
    """Empty directory, and no DIALOG_ variable inherited from the shell."""
    for name in ("DIALOG_BASE_URL", "DIALOG_CLIENT_ID", "DIALOG_CLIENT_SECRET"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)
    return tmp_path


def test_org_file_alone_is_still_used(isolated_cwd):
    """The organization's file is enough, without a shared file."""
    (isolated_cwd / ".env.co_maville.dev").write_text(ORG_ONLY)

    settings = Settings(organization="co_maville", env="dev")

    assert settings.base_url == "https://org.example"
    assert settings.client_id == "org-id"


def test_shared_file_alone_is_used(isolated_cwd):
    """`.env.{env}` alone is enough, without an organization file."""
    (isolated_cwd / ".env.dev").write_text(SHARED)

    settings = Settings(organization="co_maville", env="dev")

    assert settings.base_url == "https://shared.example"
    assert settings.client_id == "shared-id"


def test_org_file_overrides_shared_file(isolated_cwd):
    """The organization's file overrides the shared one, key by key."""
    (isolated_cwd / ".env.dev").write_text(SHARED)
    (isolated_cwd / ".env.co_maville.dev").write_text(ORG_PARTIAL)

    settings = Settings(organization="co_maville", env="dev")

    # base_url is not set by the organization: it comes from the shared file.
    assert settings.base_url == "https://shared.example"
    assert settings.client_id == "org-id"
    assert settings.client_secret == "org-secret"


def test_shared_file_is_scoped_to_its_env(isolated_cwd):
    """`.env.prod` must not feed a dev resolution."""
    (isolated_cwd / ".env.prod").write_text(SHARED)

    settings = Settings(organization="co_maville", env="dev")

    assert settings.base_url is None


def test_no_shared_fallback_in_prod(isolated_cwd):
    """The shared fallback is dev-only: in prod, `.env.prod` is ignored."""
    (isolated_cwd / ".env.prod").write_text(SHARED)

    settings = Settings(organization="co_maville", env="prod")

    assert settings.base_url is None
    assert settings.client_id is None


def test_prod_uses_only_the_organization_file(isolated_cwd):
    """In prod, the organization's file must carry all of its credentials."""
    (isolated_cwd / ".env.prod").write_text(SHARED)
    (isolated_cwd / ".env.co_maville.prod").write_text(ORG_ONLY)

    settings = Settings(organization="co_maville", env="prod")

    # No SHARED value may show through, even for a key ORG_ONLY lacks.
    assert settings.base_url == "https://org.example"
    assert settings.client_id == "org-id"
    assert settings.client_secret == "org-secret"


def test_prod_organization_without_file_fails(isolated_cwd):
    """No borrowed identity in prod: an organization without a file fails."""
    (isolated_cwd / ".env.prod").write_text(SHARED)

    with pytest.raises(Exception, match="base_url"):
        OrganizationSettings.from_env("co_maville", env="prod")


def test_process_env_wins_over_files(isolated_cwd, monkeypatch):
    """The CI has no file: process variables win."""
    (isolated_cwd / ".env.dev").write_text(SHARED)
    monkeypatch.setenv("DIALOG_BASE_URL", "https://ci.example")

    settings = Settings(organization="co_maville", env="dev")

    assert settings.base_url == "https://ci.example"
    assert settings.client_id == "shared-id"


def test_no_file_leaks_between_organizations(isolated_cwd):
    """An organization without a file does not inherit the previous one's."""
    (isolated_cwd / ".env.co_maville.dev").write_text(ORG_ONLY)

    first = Settings(organization="co_maville", env="dev")
    second = Settings(organization="co_autreville", env="dev")

    assert first.client_id == "org-id"
    assert second.client_id is None


def test_organization_settings_reports_missing_values(isolated_cwd):
    """An incomplete shared file fails explicitly, not silently."""
    (isolated_cwd / ".env.dev").write_text('DIALOG_BASE_URL="https://shared.example"\n')

    with pytest.raises(Exception, match="client_id"):
        OrganizationSettings.from_env("co_maville", env="dev")
