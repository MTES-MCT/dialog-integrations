"""What the nightly workflow integrates, and where: `integrations.toml`.

Standard library only: the workflow reads it before installing the project.

    python3 registry.py matrix [organization]         the GitHub matrix, as JSON
    python3 registry.py organizations [organization]  the organizations, comma-separated
"""

import json
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

REGISTRY_PATH = Path(__file__).parent / "integrations.toml"
TARGETS = ("prod", "staging")


class RegistryError(Exception):
    """`integrations.toml` says something the workflow cannot run."""


@dataclass(frozen=True)
class Entry:
    organization: str
    target: str
    paused: tuple[str, ...] = ()


def load(path: Path = REGISTRY_PATH) -> list[Entry]:
    with open(path, "rb") as handle:
        document = tomllib.load(handle)

    entries: list[Entry] = []
    for raw in document.get("integration", []):
        organization = raw.get("organization")
        target = raw.get("target")
        paused = raw.get("paused", [])
        if not isinstance(organization, str) or not organization:
            raise RegistryError(f"An entry has no organization: {raw}")
        if target not in TARGETS:
            raise RegistryError(f"{organization}: target must be one of {TARGETS}, not {target!r}")
        if not isinstance(paused, list) or not all(isinstance(p, str) for p in paused):
            raise RegistryError(f"{organization}: paused must be a list of source names")
        if any(entry.organization == organization for entry in entries):
            raise RegistryError(f"{organization} is listed twice")
        entries.append(Entry(organization, target, tuple(paused)))
    return entries


def entry(organization: str, path: Path = REGISTRY_PATH) -> Entry | None:
    """The organization's entry, or None for one that does not run every night."""
    return next((e for e in load(path) if e.organization == organization), None)


def select(entries: list[Entry], only: str | None) -> list[Entry]:
    """Every entry, or the one named by `only`, which must exist."""
    if not only:
        return entries
    chosen = [e for e in entries if e.organization == only]
    if not chosen:
        raise RegistryError(f"{only} is not in integrations.toml")
    return chosen


def matrix(entries: list[Entry]) -> dict:
    return {"include": [{"organization": e.organization, "target": e.target} for e in entries]}


def main(argv: list[str]) -> int:
    if not argv or argv[0] not in ("matrix", "organizations"):
        print(__doc__, file=sys.stderr)
        return 2
    try:
        entries = select(load(), argv[1] if len(argv) > 1 else None)
    except RegistryError as error:
        print(f"integrations.toml: {error}", file=sys.stderr)
        return 1
    if argv[0] == "matrix":
        print(json.dumps(matrix(entries)))
    else:
        print(",".join(e.organization for e in entries))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
