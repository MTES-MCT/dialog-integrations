import json
from datetime import datetime
from importlib import util as importlib_util
from pathlib import Path
from typing import TypedDict, get_type_hints

import polars as pl
from loguru import logger

from api.dia_log_client import Client
from api.dia_log_client.api.private.get_api_organization_identifiers import (
    sync_detailed as _get_identifiers,
)
from api.dia_log_client.api.private.post_api_regulations_add import (
    sync_detailed as add_regulation,
)
from api.dia_log_client.api.private.put_api_regulations_publish import (
    sync_detailed as publish_regulation,
)
from api.dia_log_client.models import (
    PostApiRegulationsAddBody,
)
from settings import OrganizationSettings

PY_TO_POLARS = {
    str: pl.Utf8,
    int: pl.Int64,
    float: pl.Float64,
    bool: pl.Boolean,
    datetime: pl.Datetime,
}


def typed_dict_to_polars_schema(td: type[TypedDict]) -> dict[str, pl.DataType]:  # type: ignore
    schema = {}
    for k, t in get_type_hints(td).items():
        # Optional[T] → T
        origin = getattr(t, "__origin__", None)
        if origin is type(None):
            continue
        if origin is list or origin is dict:
            raise NotImplementedError
        if origin is None and hasattr(t, "__args__"):
            t = t.__args__[0]
        schema[k] = PY_TO_POLARS[t]
    return schema


class DialogIntegration:
    client: Client
    draft_status: bool = False
    organization_settings: OrganizationSettings

    def __init__(self, organization_settings: OrganizationSettings, client: Client):  # type: ignore
        self.organization_settings = organization_settings
        self.client = client

    @property
    def organization(self) -> str:
        return self.organization_settings.organization

    @classmethod
    def from_organization(cls, organization: str, env: str = "dev") -> "DialogIntegration":
        """Create DialogIntegration from organization name and environment."""
        organization_settings = OrganizationSettings.from_env(organization, env=env)
        return cls.from_settings(organization_settings)

    @classmethod
    def from_settings(cls, organization_settings: OrganizationSettings) -> "DialogIntegration":
        """Create DialogIntegration from pre-configured settings."""
        client = Client(
            base_url=organization_settings.base_url,  # type: ignore
            raise_on_unexpected_status=True,
            headers={
                "X-Client-Id": organization_settings.client_id,
                "X-Client-Secret": organization_settings.client_secret,
                "Accept": "application/json",
            },  # type: ignore
        )

        integration_file = (
            Path("integrations") / organization_settings.organization / "integration.py"
        )
        if not integration_file.exists():
            raise FileNotFoundError(integration_file)

        spec = importlib_util.spec_from_file_location(
            f"integrations.{organization_settings.organization}.integrations", integration_file
        )
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load {integration_file}")

        module = importlib_util.module_from_spec(spec)
        spec.loader.exec_module(module)

        if not hasattr(module, "Integration"):
            raise AttributeError("Integration class not found")

        return getattr(module, "Integration")(organization_settings, client)

    def integrate_regulations(self) -> None:
        raw_data = self.fetch_raw_data()
        logger.info(f"Fetched {raw_data.shape[0]} raw records")
        clean_data = raw_data.pipe(self.compute_clean_data)
        logger.info(f"After cleaning, got {clean_data.shape[0]} records")

        regulations = self.create_regulations(clean_data)
        for regulation in regulations:
            regulation.identifier = f"{regulation.identifier}-0"
        num_measures = sum([len(regulation.measures or []) for regulation in regulations])
        logger.info(f"Created {len(regulations)} regulations with a total of {num_measures}")

        integrated_regulation_ids = self.fetch_regulation_ids()
        regulation_ids_to_integrate = set([r.identifier for r in regulations]) - set(
            integrated_regulation_ids
        )
        regulations_to_integrate = [
            regulation
            for regulation in regulations
            if regulation.identifier in regulation_ids_to_integrate
        ]
        logger.info(f"Found {len(regulations_to_integrate)} new regulations to integrate")

        self._integrate_regulations(regulations_to_integrate)

    def publish_regulations(self) -> None:
        regulation_ids = self.fetch_regulation_ids()
        count_error = 0
        for index, regulation_id in enumerate(regulation_ids):
            try:
                publish_regulation(identifier=regulation_id, client=self.client)
                logger.success(
                    f"Measure {index}/{len(regulation_ids)} successfully published: {regulation_id}"
                )
            except Exception:
                logger.error(
                    f"Measure {index}/{len(regulation_ids)} failed to publish: {regulation_id}"
                )
                count_error += 1

        if count_error > 0:
            logger.error(f"Failed to publish {count_error} identifier(s)")
        logger.success(
            f"Finished publishing {len(regulation_ids) - count_error} measures successfully"
        )

    def _integrate_regulations(self, regulations: list[PostApiRegulationsAddBody]) -> None:
        count_error = 0
        for index, regulation in enumerate(regulations):
            logger.info(f"Creating regulation {index}/{len(regulations)}: {regulation.identifier}")
            try:
                resp = add_regulation(client=self.client, body=regulation)
                assert resp.status_code == 201, f"got status {resp.status_code}"
            except Exception as e:
                logger.error(f"Failed to create: {regulation.identifier} - {e}")
                logger.error(json.loads(resp.content))
                count_error += 1

        count_success = len(regulations) - count_error
        logger.success(
            f"Finished integrating {count_success}/{len(regulations)} regulations successfully"
        )

    def fetch_raw_data(self) -> pl.DataFrame:
        """
        Fetch raw data from the source system.
        Returns as typed polars dataframe.
        """
        raise NotImplementedError("Subclasses must implement fetch_data method")

    def compute_clean_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        """
        Clean and transform the raw data into the desired format.
        Returns as typed polars dataframe.
        """
        raise NotImplementedError("Subclasses must implement compute_clean_data method")

    def create_regulations(self, clean_data: pl.DataFrame) -> list[PostApiRegulationsAddBody]:
        """
        Create regulation payloads from clean data.
        Returns a dict mapping regulation_id to PostApiRegulationsAddBody.
        """
        raise NotImplementedError("Subclasses must implement create_regulations method")

    def fetch_regulation_ids(self) -> list[str]:
        logger.info(f"Fetching identifiers for organization: {self.organization}")
        resp = _get_identifiers(client=self.client)

        if resp.parsed is None or not hasattr(resp.parsed, "identifiers"):
            raise Exception("Failed to fetch identifiers")

        identifiers: list[str] = resp.parsed.identifiers  # type: ignore

        logger.info(f"Found {len(identifiers)} identifier(s) for organization {self.organization}")

        return list(identifiers)
