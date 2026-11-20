"""The formulas free texts repeat, counted on every run so a frequent one gets a status.

A formula is a sentence, compared without case, spacing or digits. It is neutral (read as
already published), blocking (its row is dropped), or not yet arbitrated, which drops the row
too. A formula not yet arbitrated in more than `LOG_THRESHOLD` records is logged; beyond
`ALERT_THRESHOLD`, the team is alerted in Tchap (thresholds decided 2026-11-18).
"""

import re
from collections import Counter
from collections.abc import Iterable

from loguru import logger

LOG_THRESHOLD = 10
ALERT_THRESHOLD = 100
_EXAMPLE_LENGTH = 150


def formula_key(sentence: str) -> str:
    return re.sub(r"\d+", "0", re.sub(r"\s+", " ", sentence.lower())).strip()


def report_recurring_formulas(
    unarbitrated_by_record: Iterable[Iterable[str]],
    texts: str,
    team_label: str,
    alerts: list[str] | None,
) -> None:
    """`unarbitrated_by_record`: per record (an act, a measure), its sentences that are
    neither neutral nor blocking. A formula counts once per record."""
    records: Counter[str] = Counter()
    example: dict[str, str] = {}
    for sentences in unarbitrated_by_record:
        keys = {formula_key(sentence): sentence for sentence in sentences}
        for key, sentence in keys.items():
            example.setdefault(key, sentence)
        records.update(keys.keys())

    recurring = sorted(
        ((count, key) for key, count in records.items() if count > LOG_THRESHOLD), reverse=True
    )
    for count, key in recurring:
        logger.warning(
            f"{texts}: {count} records repeat a formula neither neutral nor blocking, to "
            f"arbitrate: {example[key][:_EXAMPLE_LENGTH]!r}"
        )
    beyond = sum(count > ALERT_THRESHOLD for count, _ in recurring)
    if beyond and alerts is not None:
        alerts.append(
            f"texte libre présent à de très nombreuses reprises ({team_label} : {beyond} "
            f"formule{'s' if beyond > 1 else ''} au-delà de {ALERT_THRESHOLD}) ; à prendre en "
            "compte dans le code"
        )
