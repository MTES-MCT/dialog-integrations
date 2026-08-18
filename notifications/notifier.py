import html
import json
import os
from datetime import datetime
from urllib.parse import quote
from uuid import uuid4

import requests
from loguru import logger

REQUEST_TIMEOUT_SECONDS = 10


class TchapNotifier:
    """Post the integration report to a Tchap (Matrix) room.

    Authenticates with a service account token taken from the environment.
    The account must have joined the room, and the room must be unencrypted.
    """

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

    def send_notification(self, results_data: dict) -> None:
        """Send the integration report to the configured Tchap room."""
        if not (self.homeserver_url and self.access_token and self.room_id):
            missing = [
                name
                for name, value in (
                    ("TCHAP_HOMESERVER_URL", self.homeserver_url),
                    ("TCHAP_ACCESS_TOKEN", self.access_token),
                    ("TCHAP_ROOM_ID", self.room_id),
                )
                if not value
            ]
            message = f"Incomplete Tchap configuration: {', '.join(missing)}"
            # A silent notification in CI hides a broken pipeline: fail loudly.
            if os.getenv("GITHUB_ACTIONS"):
                raise RuntimeError(message)
            logger.warning(
                f"{message}, skipping notification "
                "(locally: `set -a && source .env && set +a` first, `uv run` does not "
                "read that file)"
            )
            return

        body, formatted_body = self.format_message(results_data)
        url = (
            f"{self.homeserver_url}/_matrix/client/v3/rooms/{quote(self.room_id, safe='')}"
            # Matrix requires a transaction id; a fresh one per call, we never retry.
            f"/send/m.room.message/{uuid4()}"
        )
        try:
            response = requests.put(
                url,
                headers={"Authorization": f"Bearer {self.access_token}"},
                json={
                    "msgtype": "m.text",
                    "body": body,
                    "format": "org.matrix.custom.html",
                    "formatted_body": formatted_body,
                },
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
        except requests.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            # Matrix puts errcode and error in the body;
            detail = e.response.text[:500] if e.response is not None else str(e)
            if status in (401, 403):
                logger.error(
                    f"Tchap rejected the token (HTTP {status}): expired or revoked, or "
                    f"the service account has not joined room {self.room_id} — {detail}"
                )
            else:
                logger.error(f"Failed to send Tchap notification (HTTP {status}): {detail}")
            raise
        except requests.RequestException as e:
            logger.error(f"Failed to send Tchap notification: {e}")
            raise

        # The event_id proves the server accepted the message, and locates it in Tchap.
        try:
            event_id = response.json().get("event_id", "?")
        except ValueError:
            event_id = "?"
        logger.info(f"Notification sent to Tchap room {self.room_id} (event_id={event_id})")

    def format_message(self, results_data: dict) -> tuple[str, str]:
        """Build the message: plain-text fallback first, then HTML.

        Matrix requires `body` and treats `formatted_body` as the optional enrichment.
        """
        now = datetime.now().strftime("%d/%m/%Y %H:%M")
        title = "Rapport d'intégration Open Data"

        text_lines = [title, f"Rapport généré le {now}.", ""]
        html_items: list[str] = []

        # Sorted: the output order of a GitHub matrix job is not guaranteed.
        for key, raw in sorted(results_data.items()):
            if not key.startswith("result_"):
                continue
            org = key.removeprefix("result_")
            # Values are JSON strings such as '{"success": true}'
            try:
                result = json.loads(raw) if isinstance(raw, str) else raw
            except (json.JSONDecodeError, TypeError):
                result = {}

            success = bool(result.get("success", False)) if isinstance(result, dict) else False
            icon = "✅" if success else "❌"
            status_text = "Importé avec succès" if success else "Erreur lors de l'import"

            text_lines.append(f"{icon} {org} : {status_text}")
            html_items.append(
                f"<li>{icon} <strong>{html.escape(org, quote=False)}</strong> : {status_text}</li>"
            )

        if not html_items:
            # An empty report means the integration job produced nothing.
            # This is an anomaly, and it shoul be reported.
            anomaly = "Aucun résultat d'intégration reçu."
            text_lines.append(f"⚠️ {anomaly}")
            html_items.append(f"<li>⚠️ <strong>{anomaly}</strong></li>")

        formatted_body = (
            f"<h4>{title}</h4><p>Rapport généré le {now}.</p><ul>{''.join(html_items)}</ul>"
        )
        return "\n".join(text_lines), formatted_body

    @staticmethod
    def _clean(value: str | None) -> str | None:
        """Strip whitespace and one pair of enclosing quotes from a config value."""
        if value is None:
            return None
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1].strip()
        return value or None
