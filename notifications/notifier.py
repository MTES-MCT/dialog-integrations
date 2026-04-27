import json
import os
from datetime import datetime

import requests
from loguru import logger


class MattermostNotifier:
    def __init__(self, webhook_url: str | None = None):
        self.webhook_url = webhook_url or os.getenv("MATTERMOST_WEBHOOK_URL")

    def send_notification(self, results_data: dict) -> None:
        """Send integration results to Mattermost."""
        if not self.webhook_url:
            logger.warning("MATTERMOST_WEBHOOK_URL not configured, skipping notification")
            return

        message = self._format_message(results_data)
        try:
            response = requests.post(
                self.webhook_url,
                json={"text": message},
            )
            response.raise_for_status()
            logger.info("Notification sent to Mattermost successfully")
        except requests.RequestException as e:
            logger.error(f"Failed to send notification to Mattermost: {e}")
            raise

    def _format_message(self, results_data: dict) -> str:
        now = datetime.now().strftime("%d/%m/%Y %H:%M")
        lines = [
            "#### Rapport d'intégration Litteralis",
            f"Rapport généré le {now}.\n",
        ]

        for key, raw in results_data.items():
            if not key.startswith("result_"):
                continue
            org = key.removeprefix("result_")
            # Each value is a JSON string like '{"success": true}'
            try:
                result = json.loads(raw) if isinstance(raw, str) else raw
            except (json.JSONDecodeError, TypeError):
                result = {}

            success = result.get("success", False)
            status = ":white_check_mark:" if success else ":x:"
            status_text = "Importé avec succès" if success else "Erreur lors de l'import"
            lines.append(f"{status} {org} : {status_text}")

        return "\n".join(lines)
