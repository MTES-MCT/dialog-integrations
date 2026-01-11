from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_prefix="DIALOG_", case_sensitive=False, extra="ignore"
    )

    client_id: str = Field(...)
    client_secret: str = Field(...)
    base_url: str = Field(...)
