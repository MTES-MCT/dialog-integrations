"""Render the synchronization report — in French, it is read by the team.

Two things live here: a field-by-field diff between two digests
(`integrations/state.py`), and the report printed by `--dry-run` and logged before
every real run.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from integrations.reconciliation import CREATE, DELETE, UPDATE, Batch, ReconciliationPlan
from integrations.state import Digest

# Beyond that, only the head of the list is printed: the report has to stay readable
# in a terminal and in the CI log.
MAX_LISTED_IDENTIFIERS = 20
MAX_DETAILED_DIFFS = 20
MAX_DIFF_LINES_PER_REGULATION = 10
MAX_VALUE_LENGTH = 80

OPERATION_LABELS = {
    CREATE: "à créer",
    UPDATE: "à mettre à jour",
    DELETE: "à supprimer",
}

_MISSING = object()


@dataclass
class SourceFunnel:
    """Volumes measured on one data source during the run."""

    name: str
    clean_rows: int = 0
    regulations: int = 0
    measures: int = 0
    raw_rows: int | None = None
    metrics: dict[str, int] = field(default_factory=dict)


def diff_digests(old: Any, new: Any, path: str = "") -> list[tuple[str, Any, Any]]:
    """Compare two digests and return (path, old value, new value) triples."""
    if isinstance(old, Mapping) and isinstance(new, Mapping):
        differences: list[tuple[str, Any, Any]] = []
        for key in list(old.keys()) + [k for k in new.keys() if k not in old]:
            differences += diff_digests(
                old.get(key, _MISSING),
                new.get(key, _MISSING),
                f"{path}.{key}" if path else str(key),
            )
        return differences

    if isinstance(old, Sequence) and isinstance(new, Sequence) and not _is_text(old, new):
        differences = []
        for index in range(max(len(old), len(new))):
            differences += diff_digests(
                old[index] if index < len(old) else _MISSING,
                new[index] if index < len(new) else _MISSING,
                f"{path}[{index}]",
            )
        return differences

    if old == new:
        return []
    return [(path, old, new)]


def format_diff(identifier: str, old: Digest, new: Digest) -> list[str]:
    """Human-readable lines describing what changed in one regulation."""
    differences = diff_digests(old, new)
    if not differences:
        return [f"  {identifier} : aucune différence lisible dans le digest"]

    lines = [f"  {identifier} ({len(differences)} champ(s))"]
    for path, before, after in differences[:MAX_DIFF_LINES_PER_REGULATION]:
        lines.append(f"    - {path} : {_render_value(before)} → {_render_value(after)}")
    if len(differences) > MAX_DIFF_LINES_PER_REGULATION:
        lines.append(f"    … et {len(differences) - MAX_DIFF_LINES_PER_REGULATION} autre(s)")
    return lines


def render_report(
    *,
    organization: str,
    environment: str,
    dry_run: bool,
    plan: ReconciliationPlan,
    funnels: Sequence[SourceFunnel],
    produced: Mapping[str, Digest],
    snapshot: Mapping[str, Digest] | None,
    force_deletions: bool = False,
) -> str:
    """Build the whole report. Same text in --dry-run and before a real run."""
    mode = "simulation (--dry-run), aucune écriture" if dry_run else "exécution réelle"
    lines = [
        "",
        "=" * 78,
        f"Rapport de synchronisation — {organization} ({environment})",
        f"Mode : {mode}",
        "=" * 78,
        "",
        "Entonnoir",
    ]

    for funnel in funnels:
        raw = f"{funnel.raw_rows} lignes brutes → " if funnel.raw_rows is not None else ""
        lines.append(
            f"  {funnel.name} : {raw}{funnel.clean_rows} lignes nettoyées → "
            f"{funnel.regulations} arrêtés, {funnel.measures} mesures"
        )
        if funnel.metrics:
            measured = ", ".join(f"{name} : {value}" for name, value in funnel.metrics.items())
            lines.append(f"    volumétries source — {measured}")
    if not funnels:
        lines.append("  (aucune source n'a produit de données)")

    remote = f"DiaLog : {plan.remote_total} identifiant(s) dans l'organisation"
    if plan.identifier_prefix:
        remote += f", dont {plan.remote_in_prefix} sous le préfixe"
    lines += [
        "",
        f"Préfixe d'identifiant : {plan.identifier_prefix or 'aucun'}",
        remote,
        f"Instantané : {_render_snapshot_state(plan)}",
        "",
        "Lots",
    ]

    for batch in plan.batches:
        lines.append(f"  {_render_batch_header(batch)}")
    lines.append(f"  inchangés : {len(plan.unchanged)}")

    for batch in plan.batches:
        if batch.size:
            lines += ["", *_render_identifiers(batch)]

    update_diffs = _render_update_diffs(plan.updates, produced, snapshot)
    if update_diffs:
        lines += ["", "Différences détectées", *update_diffs]

    held = [batch for batch in plan.batches if batch.held]
    if held:
        lines += ["", "⚠ Lots retenus — à revoir manuellement"]
        for batch in held:
            lines.append(
                f"  {OPERATION_LABELS[batch.operation]} : {batch.size} > plafond "
                f"{batch.limit} — lot non appliqué, redétecté à l'identique demain"
            )
        if plan.deletions.held and not force_deletions:
            lines.append(
                f"  Relâcher les suppressions : uv run dialog integrate {organization} "
                f"--env={environment} --force-deletions"
            )

    lines.append("")
    return "\n".join(lines)


def _render_snapshot_state(plan: ReconciliationPlan) -> str:
    if plan.update_mode == "none":
        return "non utilisé — les mises à jour sont désactivées pour cette organisation"
    if plan.snapshot_present:
        return "présent"
    return "absent — aucune mise à jour ne sera détectée, il sera reconstruit"


def _render_batch_header(batch: Batch) -> str:
    limit = "aucun" if batch.limit is None else str(batch.limit)
    held = " — RETENU" if batch.held else ""
    return f"{OPERATION_LABELS[batch.operation]} : {batch.size} (plafond : {limit}){held}"


def _render_identifiers(batch: Batch) -> list[str]:
    shown = batch.identifiers[:MAX_LISTED_IDENTIFIERS]
    if batch.size <= MAX_LISTED_IDENTIFIERS:
        header = f"Identifiants {OPERATION_LABELS[batch.operation]} ({batch.size}) :"
    else:
        header = (
            f"Identifiants {OPERATION_LABELS[batch.operation]} "
            f"({MAX_LISTED_IDENTIFIERS} premiers sur {batch.size}) :"
        )
    return [header] + [f"  - {identifier}" for identifier in shown]


def _render_update_diffs(
    updates: Batch,
    produced: Mapping[str, Digest],
    snapshot: Mapping[str, Digest] | None,
) -> list[str]:
    if not updates.size or snapshot is None:
        return []

    lines: list[str] = []
    for identifier in updates.identifiers[:MAX_DETAILED_DIFFS]:
        old = snapshot.get(identifier)
        new = produced.get(identifier)
        if old is None or new is None:
            # `--update-existing` updates regulations we have no digest for.
            lines.append(f"  {identifier} : pas d'empreinte antérieure, contenu remplacé")
            continue
        lines += format_diff(identifier, old, new)
    if updates.size > MAX_DETAILED_DIFFS:
        lines.append(f"  … et {updates.size - MAX_DETAILED_DIFFS} autre(s) mise(s) à jour")
    return lines


def _render_value(value: Any) -> str:
    if value is _MISSING:
        return "(absent)"
    rendered = repr(value)
    if len(rendered) > MAX_VALUE_LENGTH:
        rendered = rendered[: MAX_VALUE_LENGTH - 1] + "…"
    return rendered


def _is_text(*values: Any) -> bool:
    return any(isinstance(value, (str, bytes)) for value in values)
