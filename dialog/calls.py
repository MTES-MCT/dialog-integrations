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
from api.dia_log_client.api.private.put_api_regulations_update import (
    sync_detailed as update_regulation,
)
from api.dia_log_client.api.public.get_api_regulations_json import (
    sync_detailed as get_public_regulations,
)
from api.dia_log_client.errors import UnexpectedStatus
from api.dia_log_client.models import (
    GetApiRegulationsJsonStatus,
    PostApiRegulationsAddBody,
    PutApiRegulationsUpdateBody,
)
from dialog.failures import WriteFailure


class PublicExportUnreadable(Exception):
    """A page of the public export answered something else than 200."""


# The public export serves at most 100 regulations a page, whatever `pageSize` asks
# (measured on the staging, 2026-10-17).
PUBLIC_PAGE_SIZE = 100


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

    def public_identifiers(self, insee_code: str) -> list[tuple[str, str]]:
        """(identifier, organization name) of every regulation the public export lists for
        one municipality, whatever its validity and whichever organization published it.
        Raises when a page is unreadable.

        `/api/organization/identifiers` only sees our own organization; this is how another
        organization's regulations are read. Filtered by municipality: the whole export
        takes ~37 s a page (2026-10-17), one municipality well under a second.
        """
        found: list[tuple[str, str]] = []
        page = 1
        while True:
            resp = get_public_regulations(
                client=self.client,
                status=GetApiRegulationsJsonStatus.ALL,
                insee_code=insee_code,
                page=page,
                page_size=PUBLIC_PAGE_SIZE,
            )
            if resp.status_code != 200:
                raise PublicExportUnreadable(
                    f"Failed to read the public export of {insee_code}, page {page}: "
                    f"got status {resp.status_code}"
                )
            body = json.loads(resp.content)
            regulations = body.get("regulations") or []
            found.extend(
                (regulation["identifier"], regulation["organization"]["name"])
                for regulation in regulations
            )
            if not regulations or page >= body["metadata"]["lastPage"]:
                return found
            page += 1

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
