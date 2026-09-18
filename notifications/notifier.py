"""The daily integration report, built from the result of every organization's run.

Formatting only: `Notifier.format_message` returns the text and HTML bodies, and
`notifications.tchap_bot.TchapBot` posts them. The results are the `--json` outputs of
`dialog integrate`, one per organization, keyed `result_{org}`.
"""

import html
import json
from datetime import datetime

# Runs against any other host (a staging) are filed under their own section.
PRODUCTION_HOST = "dialog.beta.gouv.fr"


class Notifier:
    """Turn the integration results into the message the team reads in Tchap."""

    def format_message(self, results_data: dict) -> tuple[str, str]:
        """Build the message: plain-text fallback first, then HTML.

        Matrix requires `body` and treats `formatted_body` as the optional enrichment.
        """
        now = datetime.now().strftime("%d/%m/%Y %H:%M")
        title = "Rapport d'intégration Open Data"

        text_lines = [title, f"Rapport généré le {now}."]
        html_parts: list[str] = []

        results = self._parse_results(results_data)
        if not results:
            # An empty report means the integration job produced nothing.
            # This is an anomaly, and it should be reported.
            anomaly = "Aucun résultat d'intégration reçu."
            text_lines += ["", f"⚠️ {anomaly}"]
            html_parts.append(f"<ul><li>⚠️ <strong>{anomaly}</strong></li></ul>")

        # Production first, then every staging: the team reads what is live before what
        # is under review. A result without a target predates the staging runs.
        production = [(org, r) for org, r in results if self._target(r) in (None, PRODUCTION_HOST)]
        staging = [(org, r) for org, r in results if self._target(r) not in (None, PRODUCTION_HOST)]
        hosts = sorted({self._target(r) or "" for _, r in staging})
        for heading, group, single_host in (
            ("Production", production, True),
            (f"Staging — {', '.join(hosts)}", staging, len(hosts) == 1),
        ):
            if not group:
                continue
            text_lines += ["", heading]
            html_items: list[str] = []
            for org, result in group:
                # With several staging hosts, each organization names its own.
                target = self._target(result)
                if not single_host and target:
                    org = f"{org} [{target}]"
                text, item = self._format_organization(org, result)
                text_lines += text
                html_items.append(item)
            html_parts.append(
                f"<p><strong>{html.escape(heading, quote=False)}</strong></p>"
                f"<ul>{''.join(html_items)}</ul>"
            )

        formatted_body = f"<h4>{title}</h4><p>Rapport généré le {now}.</p>{''.join(html_parts)}"
        return "\n".join(text_lines), formatted_body

    @staticmethod
    def _parse_results(results_data: dict) -> list[tuple[str, dict]]:
        """The (organization, result) pairs, in a stable order.

        Sorted: the output order of a GitHub matrix job is not guaranteed. Values are
        JSON strings such as '{"success": true}'; anything unreadable counts as a failure.
        """
        results = []
        for key, raw in sorted(results_data.items()):
            if not key.startswith("result_"):
                continue
            try:
                result = json.loads(raw) if isinstance(raw, str) else raw
            except (json.JSONDecodeError, TypeError):
                result = {}
            if not isinstance(result, dict):
                result = {}
            results.append((key.removeprefix("result_"), result))
        return results

    @staticmethod
    def _target(result: dict) -> str | None:
        target = result.get("target")
        return target if isinstance(target, str) and target else None

    def _format_organization(self, org: str, result: dict) -> tuple[list[str], str]:
        """One organization: the text lines, and the HTML list item."""
        success = bool(result.get("success", False))
        icon = "✅" if success else "❌"
        status_text = "Importé avec succès" if success else "Erreur lors de l'import"

        counts = self._format_counts(result)
        headline = f"{icon} {org} : {status_text}"
        if counts:
            headline += f" - {counts}"

        details = self._format_details(result)
        text_lines = [headline] + [f"    {detail}" for detail in details]

        html_details = "".join(f"<li>{html.escape(detail, quote=False)}</li>" for detail in details)
        item = (
            f"<li>{icon} <strong>{html.escape(org, quote=False)}</strong> : {status_text}"
            + (f" - {html.escape(counts, quote=False)}" if counts else "")
            + (f"<ul>{html_details}</ul>" if html_details else "")
            + "</li>"
        )
        return text_lines, item

    # Wording of the synchronization counters, as the team reads them in Tchap.
    # `closed` only appears for organizations that close what left their source.
    COUNT_LABELS = (
        ("created", "créé"),
        ("updated", "mis à jour"),
        ("deleted", "supprimé"),
        ("closed", "clos"),
    )
    INVARIABLE_LABELS = ("mis à jour", "clos")
    HELD_LABELS = {
        "create": "créations",
        "update": "mises à jour",
        "delete": "suppressions",
        "close": "clôtures",
    }

    @classmethod
    def _format_counts(cls, result: dict) -> str:
        """Created / updated / deleted, when the run reported them.

        Results from before synchronization existed only carry `success`; they must
        keep rendering exactly as they did.
        """
        parts = []
        for key, label in cls.COUNT_LABELS:
            value = result.get(key)
            if not isinstance(value, int) or isinstance(value, bool):
                continue
            plural = "s" if value > 1 and label not in cls.INVARIABLE_LABELS else ""
            parts.append(f"{value} {label}{plural}")

        if not parts:
            return ""
        if all(part.startswith("0 ") for part in parts):
            return "aucun changement"

        errors = result.get("errors")
        if isinstance(errors, int) and errors > 0:
            parts.append(f"{errors} en échec")
        return ", ".join(parts)

    @classmethod
    def _format_details(cls, result: dict) -> list[str]:
        """Held batches and source volumes, on their own lines."""
        details = []

        held = result.get("held")
        if isinstance(held, dict) and held:
            rendered = ", ".join(
                f"{count} {cls.HELD_LABELS.get(operation, operation)}"
                for operation, count in sorted(held.items())
            )
            details.append(f"⚠️ lot retenu (plafond dépassé) : {rendered} - à revoir manuellement")

        # One line per dataset (permanent, temporaire), or the organization's total for
        # results from before datasets were reported apart. Raw row counts mean
        # something different in every organization, so they are left out.
        datasets = result.get("datasets")
        if isinstance(datasets, list) and datasets:
            details += [cls._format_dataset(d) for d in datasets if isinstance(d, dict)]
        else:
            integrated = result.get("integrated")
            if isinstance(integrated, dict) and integrated:
                regulations = integrated.get("regulations", 0)
                measures = integrated.get("measures", 0)
                details.append(f"{regulations} arrêtés, {measures} mesures")

        source = result.get("source")
        if isinstance(source, dict) and source:
            rendered = ", ".join(f"{name} : {value}" for name, value in source.items())
            details.append(f"Volumétries source : {rendered}")

        return details

    @staticmethod
    def _format_dataset(dataset: dict) -> str:
        """Regulations, measures, and the share of the dataset's restrictions retained."""
        line = f"{dataset.get('regulations', 0)} arrêtés, {dataset.get('measures', 0)} mesures"
        label = dataset.get("label")
        if isinstance(label, str) and label:
            line = f"{label} : {line}"
        restrictions = dataset.get("restrictions")
        retained = dataset.get("retained")
        if isinstance(restrictions, int) and restrictions > 0 and isinstance(retained, int):
            rate = f"{100 * retained / restrictions:.1f}".replace(".", ",")
            line += f", {rate} % du jeu retenu"
        return line
