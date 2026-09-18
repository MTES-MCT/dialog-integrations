"""Post a message to a Tchap (Matrix) room.

Authenticates with a service account token. The account must have joined the room, and
the room must be unencrypted. Configuration comes from three environment variables, or
from the constructor:

    TCHAP_HOMESERVER_URL="https://matrix.agent.dinum.tchap.gouv.fr"
    TCHAP_ROOM_ID="!xxxx:agent.dinum.tchap.gouv.fr"
    TCHAP_ACCESS_TOKEN="syt_..."

Usage:

    TchapBot().send("Bonjour", "<p>Bonjour</p>")
"""

import os
from urllib.parse import quote
from uuid import uuid4

import requests
from loguru import logger

REQUEST_TIMEOUT_SECONDS = 10

ENV_VARS = ("TCHAP_HOMESERVER_URL", "TCHAP_ACCESS_TOKEN", "TCHAP_ROOM_ID")


class TchapBot:
    """One service account posting to one room."""

    def __init__(
        self,
        homeserver_url: str | None = None,
        access_token: str | None = None,
        room_id: str | None = None,
    ):
        homeserver_url = self._clean(homeserver_url or os.getenv("TCHAP_HOMESERVER_URL"))
        self.homeserver_url = homeserver_url.rstrip("/") if homeserver_url else None
        self.access_token = self._clean(access_token or os.getenv("TCHAP_ACCESS_TOKEN"))
        self.room_id = self._clean(room_id or os.getenv("TCHAP_ROOM_ID"))

    def missing_configuration(self) -> list[str]:
        """The environment variables still needed before a message can be sent."""
        values = (self.homeserver_url, self.access_token, self.room_id)
        return [name for name, value in zip(ENV_VARS, values, strict=True) if not value]

    def send(self, body: str, formatted_body: str | None = None) -> str | None:
        """Post one message; return the event id the server assigned, or None when unsent.

        `body` is the plain text every client can show; `formatted_body`, when given, is
        the HTML enrichment (`org.matrix.custom.html`). Without a complete configuration
        the message is skipped locally, and the call fails in CI: a silent notification
        there hides a broken pipeline.
        """
        missing = self.missing_configuration()
        if missing:
            message = f"Incomplete Tchap configuration: {', '.join(missing)}"
            if os.getenv("GITHUB_ACTIONS"):
                raise RuntimeError(message)
            logger.warning(
                f"{message}, skipping notification "
                "(locally: `set -a && source .env && set +a` first, `uv run` does not "
                "read that file)"
            )
            return None

        url = (
            f"{self.homeserver_url}/_matrix/client/v3/rooms/{quote(self.room_id or '', safe='')}"
            # Matrix requires a transaction id; a fresh one per call, we never retry.
            f"/send/m.room.message/{uuid4()}"
        )
        payload: dict[str, str] = {"msgtype": "m.text", "body": body}
        if formatted_body is not None:
            payload.update(format="org.matrix.custom.html", formatted_body=formatted_body)
        try:
            response = requests.put(
                url,
                headers={"Authorization": f"Bearer {self.access_token}"},
                json=payload,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            # Matrix puts errcode and error in the body.
            detail = e.response.text[:500] if e.response is not None else str(e)
            if status in (401, 403):
                logger.error(
                    f"Tchap rejected the token (HTTP {status}): expired or revoked, or "
                    f"the service account has not joined room {self.room_id} — {detail}"
                )
            else:
                logger.error(f"Failed to send Tchap message (HTTP {status}): {detail}")
            raise
        except requests.RequestException as e:
            logger.error(f"Failed to send Tchap message: {e}")
            raise

        # The event_id proves the server accepted the message, and locates it in Tchap.
        try:
            event_id = str(response.json().get("event_id", "?"))
        except ValueError:
            event_id = "?"
        logger.info(f"Message sent to Tchap room {self.room_id} (event_id={event_id})")
        return event_id

    @staticmethod
    def _clean(value: str | None) -> str | None:
        """Strip whitespace and one pair of enclosing quotes from a config value."""
        if value is None:
            return None
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1].strip()
        return value or None
