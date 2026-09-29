"""Day-over-day check of what the organization holds in DiaLog against what we produce.

The report counts what the pipeline produces. When identifiers are not stable, DiaLog
fills up with the old ones and nothing in a single run shows it (co_nantes on the
staging, 2026-09-29: 3 907 regulations for 793 produced).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

from loguru import logger

from integrations.sync.state import state_dir

# No floor: chosen on 2026-09-29.
SHIFT_SHARE = 0.05


@dataclass(frozen=True)
class Totals:
    dialog_in_prefix: int
    produced: int


class TotalsStore:
    def __init__(self, organization: str, base_dir: Path | str | None = None):
        self.path = state_dir(base_dir) / organization / "totals.json"

    def load(self) -> Totals | None:
        """The previous run's totals, or None when there is no usable file."""
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
            return Totals(
                dialog_in_prefix=int(payload["dialog_in_prefix"]), produced=int(payload["produced"])
            )
        except FileNotFoundError:
            return None
        except (OSError, ValueError, KeyError, TypeError) as e:
            logger.warning(f"Unreadable totals {self.path} ({e}), treated as missing")
            return None

    def save(self, totals: Totals) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(asdict(totals)), encoding="utf-8")


def total_shift(previous: Totals | None, current: Totals) -> dict | None:
    """Both totals, when DiaLog moved and what we produce does not explain it."""
    if previous is None:
        return None
    limit = SHIFT_SHARE * current.produced
    dialog_moved = current.dialog_in_prefix - previous.dialog_in_prefix
    unexplained = dialog_moved - (current.produced - previous.produced)
    if abs(dialog_moved) <= limit or abs(unexplained) <= limit:
        return None
    return {
        "dialog": [previous.dialog_in_prefix, current.dialog_in_prefix],
        "produced": [previous.produced, current.produced],
    }
