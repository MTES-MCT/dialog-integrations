"""Day-over-day totals: what the organization holds in DiaLog against what we produce.

Unstable identifiers fill DiaLog with old ones unseen in a single run (co_nantes on the
staging, 2026-09-29: 3 907 regulations for 793 produced). Each source's count is kept too:
an empty export would read as the whole corpus leaving the source.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path

from loguru import logger

from sync.snapshot import organization_dirs, readable

# No floor: chosen on 2026-09-29.
SHIFT_SHARE = 0.05
# Below this share of yesterday's count, a source is not trusted to say what left it
# until `--force-deletions` (chosen in October 2026).
VOLUME_DROP_SHARE = 0.7


@dataclass(frozen=True)
class Totals:
    dialog_in_prefix: int
    produced: int
    sources: dict[str, int] = field(default_factory=dict)


class TotalsStore:
    def __init__(
        self, organization: str, base_dir: Path | str | None = None, target: str | None = None
    ):
        directory, legacy = organization_dirs(organization, target, base_dir)
        self.path = directory / "totals.json"
        self.legacy_path = legacy / "totals.json" if legacy else None

    def load(self) -> Totals | None:
        """The previous run's totals, or None when there is no usable file."""
        path = readable(self.path, self.legacy_path)
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            sources = payload.get("sources") or {}
            return Totals(
                dialog_in_prefix=int(payload["dialog_in_prefix"]),
                produced=int(payload["produced"]),
                sources={str(name): int(count) for name, count in sources.items()},
            )
        except FileNotFoundError:
            return None
        except (OSError, ValueError, KeyError, TypeError, AttributeError) as e:
            logger.warning(f"Unreadable totals {path} ({e}), treated as missing")
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


def volume_drops(previous: Totals | None, produced: dict[str, int]) -> dict[str, list[int]]:
    """Sources under `VOLUME_DROP_SHARE` of their previous count, as [before, today].

    A source unknown yesterday has no baseline and is not judged.
    """
    if previous is None:
        return {}
    drops = {}
    for name, today in produced.items():
        before = previous.sources.get(name)
        if before and today < VOLUME_DROP_SHARE * before:
            drops[name] = [before, today]
    return drops
