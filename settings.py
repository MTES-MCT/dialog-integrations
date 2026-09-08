from enum import Enum
from pathlib import Path
from typing import Any

from loguru import logger
from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

Organization = Enum(
    "Organization",
    {
        name: name
        for name in [
            p.name
            for p in Path("integrations").iterdir()
            if p.is_dir() and p.name != "shared" and not p.name.startswith("__")
        ]
    },
    type=str,
)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="DIALOG_",
        case_sensitive=False,
        extra="allow",
    )

    base_url: str | None = None
    client_id: str | None = None
    client_secret: str | None = None

    # Source credentials that are not DiaLog's own, hence read without the DIALOG_ prefix.
    # Only co_paris uses this one; it stays None everywhere else.
    eudonet_paris_credentials: str | None = Field(
        default=None,
        validation_alias=AliasChoices("EUDONET_PARIS_CREDENTIALS", "eudonet_paris_credentials"),
    )

    def __init__(self, organization: str, env: str = "dev", **data: Any):
        candidates = []
        if env == "dev":
            candidates.append(Path(f".env.{env}"))
        candidates.append(Path(f".env.{organization}.{env}"))

        env_files = [env_file for env_file in candidates if env_file.exists()]

        for env_file in candidates:
            if env_file in env_files:
                logger.info(f"Loading environment variables from {env_file}")
            else:
                logger.debug(f"Environment file not found: {env_file}")

        # report if env.prod (or other equivalent) have been created but can't be used
        ignored_shared_file = Path(f".env.{env}")
        if env != "dev" and ignored_shared_file.exists():
            logger.warning(
                f"Ignoring {ignored_shared_file}: the shared fallback identity is dev-only"
            )

        if not env_files:
            logger.warning(f"No environment file found for {organization} ({env})")
            logger.warning("Using environment variables from CI/CD.")

        # use _env_file instead of mutating self.model_config, which is shared at class
        # level (ensures the env file doesn't leak into subsequent objects)
        super().__init__(_env_file=env_files or None, **data)


class OrganizationSettings:
    # Every organization must carry these to talk to the DiaLog API.
    REQUIRED_VALUES = ("base_url", "client_id", "client_secret")

    organization: str
    # Which environment the settings were loaded for; only used to label reports and
    # to print a copy-pastable command line.
    env: str = "dev"
    base_url: str | None = None
    client_id: str | None = None
    client_secret: str | None = None
    # Optional, source-specific credentials. Absent for every organization but co_paris.
    eudonet_paris_credentials: str | None = None

    def __init__(self, settings: Settings, organization: str, env: str = "dev"):
        self.organization = organization
        self.env = env
        self.base_url = settings.base_url
        self.client_id = settings.client_id
        self.client_secret = settings.client_secret
        # getattr: offline tools pass a duck-typed stand-in that only carries the required
        # values.
        self.eudonet_paris_credentials = getattr(settings, "eudonet_paris_credentials", None)

        missing_values = [name for name in self.REQUIRED_VALUES if getattr(self, name) is None]
        if missing_values:
            raise Exception(f"Invalid settings for {organization}: {missing_values}")

    @classmethod
    def from_env(cls, organization: str, env: str = "dev") -> "OrganizationSettings":
        """Create OrganizationSettings from organization and environment."""
        settings = Settings(organization=organization, env=env)
        return cls(settings, organization, env=env)
