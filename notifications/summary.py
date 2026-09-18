"""The run summary of one organization, in markdown, for the GitHub run page.

Each job of the matrix appends its own to `$GITHUB_STEP_SUMMARY`; GitHub shows them all
on the run page, one after the other. Nothing is parsed from a log file: the counters
come from the run's outcome, and the alerts from a loguru sink `cli.py` keeps open for
the duration of the run.
"""

from collections.abc import Iterable, Mapping

from integrations.sync.reconciliation import IntegrationOutcome
from notifications.notifier import Notifier

# Beyond this, a list of alerts says "something is very wrong" better than its content.
MAX_LINES_PER_LEVEL = 100


def render_summary(
    organization: str,
    target: str,
    outcome: IntegrationOutcome | None,
    records: Iterable[Mapping],
    failure: str | None = None,
) -> str:
    """Markdown for one run. `failure` is the exception that ended it, when it crashed."""
    lines = [f"## {organization} — {target}", ""]

    if failure is not None:
        lines += [f"❌ **Échec de l'intégration** : `{failure}`", ""]
    if outcome is not None:
        counts = Notifier._format_counts(outcome.to_result()) or "aucune écriture"
        if outcome.dry_run:
            planned = ", ".join(
                f"{n} {Notifier.HELD_LABELS.get(op, op)}"
                for op, n in sorted(outcome.planned.items())
            )
            lines += [f"🧪 Simulation, rien n'a été écrit. Prévu : {planned or 'rien'}", ""]
        else:
            lines += [f"✅ {counts}", ""]
        lines += _datasets_table(outcome.datasets)
        lines += [
            "<details><summary>Rapport de synchronisation</summary>",
            "",
            "```text",
            outcome.report.strip("\n"),
            "```",
            "",
            "</details>",
            "",
        ]

    by_level: dict[str, list[str]] = {"ERROR": [], "WARNING": []}
    for record in records:
        level = record["level"].name if hasattr(record["level"], "name") else str(record["level"])
        message = str(record["message"]).strip()
        by_level.setdefault("ERROR" if level in ("ERROR", "CRITICAL") else "WARNING", []).append(
            f"- `{record.get('name', '?')}` : {message}"
        )
    for level, title in (("ERROR", "Erreurs"), ("WARNING", "Alertes")):
        entries = by_level[level]
        if not entries:
            continue
        lines += [f"### {title} ({len(entries)})", ""]
        lines += entries[:MAX_LINES_PER_LEVEL]
        if len(entries) > MAX_LINES_PER_LEVEL:
            lines.append(f"- … et {len(entries) - MAX_LINES_PER_LEVEL} autres")
        lines.append("")
    return "\n".join(lines) + "\n"


def _datasets_table(datasets: list[dict]) -> list[str]:
    if not datasets:
        return []
    rows = [
        "| Jeu | Arrêtés | Mesures | Restrictions du jeu | Retenues | Part retenue |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for d in datasets:
        restrictions, retained = d.get("restrictions"), d.get("retained")
        share = (
            f"{100 * retained / restrictions:.1f} %".replace(".", ",")
            if isinstance(restrictions, int) and restrictions > 0 and isinstance(retained, int)
            else "—"
        )
        rows.append(
            f"| {d.get('label', '')} | {d.get('regulations', 0)} | {d.get('measures', 0)} "
            f"| {restrictions if restrictions is not None else '—'} "
            f"| {retained if retained is not None else '—'} | {share} |"
        )
    return rows + [""]
