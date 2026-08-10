from enum import Enum
from pathlib import Path
from typing import Any

from loguru import logger
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
    organization: str
    base_url: str | None = None
    client_id: str | None = None
    client_secret: str | None = None

    def __init__(self, settings: Settings, organization: str):
        self.organization = organization
        self.base_url = settings.base_url
        self.client_id = settings.client_id
        self.client_secret = settings.client_secret

        missing_values = [
            name for (name, value) in vars(self).items() if value is None and name != "organization"
        ]
        if missing_values:
            raise Exception(f"Invalid settings for {organization}: {missing_values}")

    @classmethod
    def from_env(cls, organization: str, env: str = "dev") -> "OrganizationSettings":
        """Create OrganizationSettings from organization and environment."""
        settings = Settings(organization=organization, env=env)
        return cls(settings, organization)
