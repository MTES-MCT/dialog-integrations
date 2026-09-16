"""The DiaLog API as the pipeline uses it: one method per call, logging and status
handling in one place.

The generated client (`api/dia_log_client`, rebuilt from `api/spec.json`) is verbose:
every call returns a `Response` whose status has to be checked, and raises on any
status the spec does not document. This wrapper turns each write into a boolean the
orchestration can count, and keeps the error messages consistent.
"""

import json

from loguru import logger

from api.dia_log_client import Client
from api.dia_log_client.api.private.delete_api_regulations_delete import (
    sync_detailed as delete_regulation,
)
from api.dia_log_client.api.private.get_api_organization_identifiers import (
    sync_detailed as get_identifiers,
)
from api.dia_log_client.api.private.get_api_regulations_get import (
    sync_detailed as get_regulation,
)
from api.dia_log_client.api.private.post_api_regulations_add import (
    sync_detailed as add_regulation,
)
from api.dia_log_client.api.private.put_api_regulations_publish import (
    sync_detailed as publish_regulation,
)
from api.dia_log_client.models import PostApiRegulationsAddBody
from settings import OrganizationSettings


def build_client(settings: OrganizationSettings) -> Client:
    """The authenticated client for one organization."""
    return Client(
        base_url=settings.base_url,  # type: ignore
        raise_on_unexpected_status=True,
        headers={
            "X-Client-Id": settings.client_id,
            "X-Client-Secret": settings.client_secret,
            "Accept": "application/json",
        },  # type: ignore
    )


class DialogApi:
    """The calls the pipeline makes, for one organization's client."""

    def __init__(self, client: Client):
        self.client = client

    def identifiers(self) -> list[str]:
        """Every regulation identifier of the organization. Raises when unreadable."""
        resp = get_identifiers(client=self.client)
        if resp.parsed is None or not hasattr(resp.parsed, "identifiers"):
            raise Exception("Failed to fetch identifiers")
        return list(resp.parsed.identifiers)  # type: ignore

    def get(self, identifier: str) -> dict | None:
        """GET a regulation as the API serializes it; None when it cannot be read."""
        try:
            resp = get_regulation(identifier=identifier, client=self.client)
        except Exception as e:
            logger.error(f"Failed to read: {identifier} - {e}")
            return None
        if resp.status_code != 200:
            logger.error(f"Failed to read: {identifier} - got status {resp.status_code}")
            return None
        return json.loads(resp.content)

    def add(self, regulation: PostApiRegulationsAddBody) -> bool:
        """POST a regulation; True when the API answered 201."""
        try:
            resp = add_regulation(client=self.client, body=regulation)
        except Exception as e:
            logger.error(f"Failed to create: {regulation.identifier} - {e}")
            return False
        if resp.status_code != 201:
            logger.error(
                f"Failed to create: {regulation.identifier} - got status {resp.status_code}"
            )
            logger.error(json.loads(resp.content))
            return False
        return True

    def delete(self, identifier: str) -> bool:
        """DELETE a regulation; True when the API answered 204."""
        try:
            resp = delete_regulation(identifier=identifier, client=self.client)
        except Exception as e:
            logger.error(f"Failed to delete: {identifier} - {e}")
            return False
        if resp.status_code != 204:
            logger.error(f"Failed to delete: {identifier} - got status {resp.status_code}")
            return False
        return True

    def publish(self, identifier: str) -> bool:
        """Publish a draft; True unless the call raised.

        A documented non-200 status (404, 400) is not reported as a failure here: that
        is the historical behaviour of `publish_regulations`, kept as is.
        """
        try:
            publish_regulation(identifier=identifier, client=self.client)
        except Exception:
            return False
        return True
