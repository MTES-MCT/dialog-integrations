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
from api.dia_log_client.api.private.put_api_regulations_publish import (
    sync_detailed as publish_regulation,
)
from settings import OrganizationSettings, Settings

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
    organization: str

    def __init__(
        self, organization: str, organization_settings: OrganizationSettings, client: Client
    ):  # type: ignore
        self.organization = organization
        self.organization_settings = organization_settings
        self.client = client

    @classmethod
    def from_organization(cls, organization: str) -> "DialogIntegration":
        organization_settings = OrganizationSettings(Settings(), organization)
        client = Client(
            base_url=organization_settings.base_url,  # type: ignore
            raise_on_unexpected_status=True,
            headers={
                "X-Client-Id": organization_settings.client_id,
                "X-Client-Secret": organization_settings.client_secret,
                "Accept": "application/json",
            },  # type: ignore
        )

        integration_file = Path("integrations") / organization / "integration.py"
        if not integration_file.exists():
            raise FileNotFoundError(integration_file)

        spec = importlib_util.spec_from_file_location(
            f"integrations.{organization}.integrations", integration_file
        )
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load {integration_file}")

        module = importlib_util.module_from_spec(spec)
        spec.loader.exec_module(module)

        if not hasattr(module, "Integration"):
            raise AttributeError("Integration class not found")

        return getattr(module, "Integration")(organization, organization_settings, client)

    def integrate_measures(self) -> None:
        df = self.fetch_raw_data().pipe(self.compute_clean_data)
        logger.info(f"Fetched {len(df)} records")
        self.get_identifiers()

    def publish_measures(self) -> None:
        identifiers = self.get_identifiers()
        count_error = 0
        for index, identifier in enumerate(identifiers):
            try:
                publish_regulation(identifier=identifier, client=self.client)
                logger.success(
                    f"Measure {index}/{len(identifiers)} successfully published: {identifier}"
                )
            except Exception:
                logger.error(f"Measure {index}/{len(identifiers)} failed to publish: {identifier}")
                count_error += 1

        if count_error > 0:
            logger.error(f"Failed to publish {count_error} identifier(s)")
        logger.success(
            f"Finished publishing {len(identifiers) - count_error} measures successfully"
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

    def get_identifiers(self) -> list[str]:
        logger.info(f"Fetching identifiers for organization: {self.organization}")
        resp = _get_identifiers(client=self.client)

        if resp.parsed is None or not hasattr(resp.parsed, "identifiers"):
            raise Exception("Failed to fetch identifiers")

        identifiers: list[str] = resp.parsed.identifiers  # type: ignore

        logger.info(f"Found {len(identifiers)} identifier(s) for organization {self.organization}")

        return list(identifiers)
