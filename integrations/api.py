"""The DiaLog API as the pipeline uses it: one method per call, logging and status
handling in one place.

The generated client (`api/dia_log_client`, rebuilt from `api/spec.json`) is verbose:
every call returns a `Response` whose status has to be checked, and raises on any
status the spec does not document. This wrapper turns each write into a boolean the
orchestration can count, and keeps the error messages consistent.
"""

import json
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from http import HTTPStatus

import httpx
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
from api.dia_log_client.api.private.put_api_regulations_update import (
    sync_detailed as update_regulation,
)
from api.dia_log_client.errors import UnexpectedStatus
from api.dia_log_client.models import PostApiRegulationsAddBody, PutApiRegulationsUpdateBody
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


OUTAGE_4XX = frozenset(
    {
        HTTPStatus.UNAUTHORIZED,
        HTTPStatus.FORBIDDEN,
        HTTPStatus.PROXY_AUTHENTICATION_REQUIRED,
        HTTPStatus.REQUEST_TIMEOUT,
        HTTPStatus.TOO_MANY_REQUESTS,
    }
)
UNRECORDED_CAUSE = "autre"


@dataclass(frozen=True)
class WriteFailure:
    """Why a write failed. `status` is None when the call got no answer."""

    status: int | None

    @property
    def is_rejection(self) -> bool:
        return (
            self.status is not None and 400 <= self.status < 500 and self.status not in OUTAGE_4XX
        )

    @property
    def cause(self) -> str:
        return f"HTTP {self.status}" if self.status is not None else "sans réponse"


def split_failures(
    identifiers: Iterable[str], failures: Mapping[str, WriteFailure]
) -> tuple[int, dict[str, int]]:
    """Regulations that were not written: (rejections, outages by cause).

    A refusal's motive is only in the log, which keeps the API's whole answer.
    """
    rejections = 0
    outages: Counter[str] = Counter()
    for identifier in identifiers:
        failure = failures.get(identifier)
        if failure is None:
            outages[UNRECORDED_CAUSE] += 1
        elif failure.is_rejection:
            rejections += 1
        else:
            outages[failure.cause] += 1
    return rejections, dict(outages.most_common())


class DialogApi:
    """The calls the pipeline makes, for one organization's client."""

    def __init__(self, client: Client):
        self.client = client
        self.write_failures: dict[str, WriteFailure] = {}

    def _record_error_answer(self, verb: str, identifier: str, status: int, content: bytes) -> None:
        failure = WriteFailure(status)
        self.write_failures[identifier] = failure
        log = logger.warning if failure.is_rejection else logger.error
        log(
            f"Failed to {verb}: {identifier} - got status {status} - "
            f"{content.decode('utf-8', errors='replace')}"
        )

    def _record_no_answer(self, verb: str, identifier: str, error: Exception) -> None:
        # The generated client raises on any status its spec leaves out, 5xx included.
        if isinstance(error, UnexpectedStatus):
            self._record_error_answer(verb, identifier, error.status_code, error.content)
            return
        self.write_failures[identifier] = WriteFailure(None)
        logger.error(f"Failed to {verb}: {identifier} - {error}")

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
        """POST a regulation; True when the API answered 201, or when it exists anyway.

        A 5xx or a transport error says nothing about what the back end did: the router
        can answer 504 while the back end commits (D-20). Such an answer is followed by a
        GET, and the regulation counts as created when it is there. A 4xx is a refusal:
        no GET.
        """
        identifier = str(regulation.identifier)
        try:
            resp = add_regulation(client=self.client, body=regulation)
        except Exception as e:
            self._record_no_answer("create", identifier, e)
            return self._created_anyway(identifier)
        if resp.status_code != 201:
            self._record_error_answer("create", identifier, resp.status_code, resp.content)
            return resp.status_code >= 500 and self._created_anyway(identifier)
        return True

    def _created_anyway(self, identifier: str) -> bool:
        """Whether a POST whose answer was lost went through."""
        if self.get(identifier) is None:
            return False
        logger.warning(f"{identifier} exists despite the failed answer: counted as created")
        return True

    def update(self, regulation: PostApiRegulationsAddBody) -> bool:
        """PUT a regulation — a full replacement; True when the API answered 2xx.

        Not for a regulation holding a zone: the API answers 500 on those (S-14).
        """
        identifier = str(regulation.identifier)
        body = PutApiRegulationsUpdateBody.from_dict(regulation.to_dict())
        try:
            resp = update_regulation(client=self.client, body=body)
        except Exception as e:
            self._record_no_answer("update", identifier, e)
            return False
        if resp.status_code not in (200, 201, 204):
            self._record_error_answer("update", identifier, resp.status_code, resp.content)
            return False
        return True

    def delete(self, identifier: str, *, missing_is_gone: bool = False) -> bool:
        """DELETE a regulation; True when the API answered 204.

        With `missing_is_gone`, a 404 counts as success: the goal state — the regulation
        is not there — is reached.
        """
        try:
            resp = delete_regulation(identifier=identifier, client=self.client)
        except Exception as e:
            self._record_no_answer("delete", identifier, e)
            return False
        if resp.status_code == 204:
            return True
        if resp.status_code == 404 and missing_is_gone:
            logger.warning(f"{identifier} was already absent from DiaLog (404)")
            return True
        self._record_error_answer("delete", identifier, resp.status_code, resp.content)
        return False

    def publish(self, identifier: str) -> bool:
        """Publish a draft; True unless the call raised.

        A documented non-200 status (404, 400) is not reported as a failure here, as
        `publish_regulations` always did.
        """
        try:
            publish_regulation(identifier=identifier, client=self.client)
        except Exception:
            return False
        return True
