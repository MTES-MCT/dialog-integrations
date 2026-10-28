"""Snapshot of what the pipeline last sent to DiaLog, used to detect updates.

Never a re-read of DiaLog: the API rewrites part of what it receives (`timeSlots` dated
1970, `documentUrl` dropped), and the whole corpus would be republished every day. A
missing snapshot means no update and a rebuild: a lost cache costs one day of updates,
never a mass rewrite. Kept per target: what was sent to the staging says nothing about
production.
"""

from __future__ import annotations

import gzip
import json
import os
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger

from sync.digest import Digest

STATE_DIR_ENV = "DIALOG_STATE_DIR"
DEFAULT_STATE_DIR = "state"
# 2: undated permanent periods digested as null; version 1 carried run days and would
# flag every such regulation.
SNAPSHOT_VERSION = 2


def state_dir(base_dir: Path | str | None = None) -> Path:
    """Directory holding the snapshots, overridable with DIALOG_STATE_DIR."""
    if base_dir is not None:
        return Path(base_dir)
    return Path(os.getenv(STATE_DIR_ENV) or DEFAULT_STATE_DIR)


def organization_dirs(
    organization: str, target: str | None, base_dir: Path | str | None = None
) -> tuple[Path, Path | None]:
    """Where an organization's state is written, and the older path still read.

    Without a target (tests, tools), the older layout is the only one.
    """
    root = state_dir(base_dir)
    if not target:
        return root / organization, None
    return root / target / organization, root / organization


def readable(path: Path, legacy: Path | None) -> Path:
    """`path` when it exists, else the older `legacy` path when that one does."""
    if path.exists() or legacy is None or not legacy.exists():
        return path
    logger.info(f"Reading {legacy} written before the target was part of the path")
    return legacy


class SnapshotStore:
    """Read and write the digest snapshot of one data source."""

    def __init__(
        self,
        organization: str,
        source: str,
        base_dir: Path | str | None = None,
        target: str | None = None,
    ):
        self.organization = organization
        self.source = source
        directory, legacy = organization_dirs(organization, target, base_dir)
        self.path = directory / f"{source}.json.gz"
        self.legacy_path = legacy / f"{source}.json.gz" if legacy else None

    def exists(self) -> bool:
        return readable(self.path, self.legacy_path).exists()

    def load(self) -> dict[str, Digest] | None:
        """The stored digests, or None when there is no usable snapshot: guessing an
        unreadable one could cost a mass rewrite."""
        path = readable(self.path, self.legacy_path)
        if not path.exists():
            logger.info(f"No snapshot at {path}: no update will be detected this run")
            return None

        try:
            with gzip.open(path, "rt", encoding="utf-8") as handle:
                payload = json.load(handle)
        except (OSError, ValueError) as e:
            logger.warning(f"Unreadable snapshot {self.path} ({e}), treated as missing")
            return None

        regulations = payload.get("regulations") if isinstance(payload, dict) else None
        if not isinstance(regulations, dict):
            logger.warning(f"Malformed snapshot {self.path}, treated as missing")
            return None
        version = payload.get("version")
        if version != SNAPSHOT_VERSION:
            # Not comparable to today's digests: every regulation would look changed.
            logger.warning(
                f"Snapshot {self.path} has version {version}, this pipeline writes "
                f"{SNAPSHOT_VERSION}: treated as missing and rebuilt today"
            )
            return None

        logger.info(f"Loaded {len(regulations)} digest(s) from {self.path}")
        return regulations

    def save(self, digests: Mapping[str, Digest]) -> None:
        payload = {
            "version": SNAPSHOT_VERSION,
            "organization": self.organization,
            "source": self.source,
            "updatedAt": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "regulations": dict(digests),
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # Write then rename: a truncated snapshot would cost a day of updates.
        temporary = self.path.with_suffix(".tmp")
        with gzip.open(temporary, "wt", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True)
        temporary.replace(self.path)
        logger.info(f"Wrote {len(payload['regulations'])} digest(s) to {self.path}")
