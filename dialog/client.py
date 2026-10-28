"""The authenticated HTTP client of one organization."""

import httpx

from api.dia_log_client import Client
from settings import OrganizationSettings

# The router answers 504 after 60 s, so a read past that means a silent server: without a
# limit the run waits for the job to be killed.
TIMEOUT = httpx.Timeout(10, read=120)


def build_client(settings: OrganizationSettings) -> Client:
    """The authenticated client for one organization."""
    return Client(
        base_url=settings.base_url,  # type: ignore
        raise_on_unexpected_status=True,
        timeout=TIMEOUT,
        headers={
            "X-Client-Id": settings.client_id,
            "X-Client-Secret": settings.client_secret,
            "Accept": "application/json",
        },  # type: ignore
    )
