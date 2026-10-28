"""The acts another channel already published in DiaLog (R-80).

Bare `1101` numbers (the former PHP channel, into "Paris") are read live from the public
export, municipality by municipality. The `Paris_{1101}` ones ("Paris (préfecture)", 100
permanent pedestrian areas) carry no municipality and the whole export takes ~1 h
(2026-10-17): they are frozen in `published_elsewhere.json`, so one added since is missed.
"""

import json
import re
from collections.abc import Iterable
from pathlib import Path

import httpx
import polars as pl
from loguru import logger

from api.dia_log_client.errors import UnexpectedStatus
from dialog.calls import DialogApi, PublicExportUnreadable

from .locations import CITY_CODE_BY_DISTRICT
from .regulations import IDENTIFIER_PREFIX

OTHER_CHANNEL_PREFIX = re.compile(r"^paris_", re.IGNORECASE)
PUBLIC_EXPORT_CITY_CODES = ("75056", *CITY_CODE_BY_DISTRICT.values())
FROZEN_ELSEWHERE_PATH = Path(__file__).with_name("published_elsewhere.json")
ELSEWHERE_STATE_FILE = "published_elsewhere_live.json"
FAILURE_TEXT_MAX_LENGTH = 200


def load_frozen_elsewhere(path: Path = FROZEN_ELSEWHERE_PATH) -> list[tuple[str, str]]:
    """The (identifier, organization) pairs the live read cannot see, frozen in the repo."""
    entries = json.loads(path.read_text(encoding="utf-8"))["regulations"]
    return [(identifier, organization) for identifier, organization in entries]


def read_numbers_published_elsewhere(
    api: DialogApi, state_path: Path, frozen: list[tuple[str, str]]
) -> dict[str, str]:
    """Read the public export and keep the Eudonet numbers other channels published.

    A municipality the export cannot serve falls back on its last successful read; one
    never read is read as empty, with a warning.
    """
    last_read = _load_state(state_path)
    read: dict[str, list[list[str]]] = {}
    unreadable: dict[str, str] = {}
    for code in PUBLIC_EXPORT_CITY_CODES:
        try:
            read[code] = [list(found) for found in api.public_identifiers(code)]
        # KeyError, TypeError, ValueError: an export of another shape than expected.
        except (
            httpx.HTTPError,
            UnexpectedStatus,
            PublicExportUnreadable,
            KeyError,
            TypeError,
            ValueError,
        ) as error:
            unreadable[code] = _describe_failure(error)
            if code in last_read:
                read[code] = last_read[code]
    never_read = sorted(code for code in unreadable if code not in last_read)
    _save_state(state_path, read)
    if never_read:
        logger.warning(
            f"R-80: the public export could not be read for {never_read}, never read before: "
            "read as empty, their regulations published by another channel may be created"
        )
    if unreadable:
        logger.warning(
            f"R-80: the public export could not be read for {len(unreadable)} "
            f"municipalities, their last successful read is used "
            f"({sum(len(read.get(code, [])) for code in unreadable)} regulations): {unreadable}"
        )
    live = [
        (identifier, organization) for found in read.values() for identifier, organization in found
    ]
    numbers = index_by_eudonet_number([*frozen, *live])
    logger.info(
        f"{len(numbers)} numbers published by another channel: {len(frozen)} frozen "
        f"identifiers, {len(live)} regulations of the public export in Paris"
    )
    return numbers


def _load_state(path: Path) -> dict[str, list[list[str]]]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        logger.warning(f"Ignoring the unreadable state file {path}: {type(error).__name__}")
        return {}


def _save_state(path: Path, read: dict[str, list[list[str]]]) -> None:
    """Written then renamed, so that a run killed halfway leaves the previous file whole."""
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_name(f"{path.name}.partial")
    partial.write_text(json.dumps(read, ensure_ascii=False), encoding="utf-8")
    partial.replace(path)


def _describe_failure(error: Exception) -> str:
    """The failure in a few words: an error page in full has no place in the public log."""
    if isinstance(error, UnexpectedStatus):
        return f"HTTP {error.status_code}"
    return f"{type(error).__name__}: {str(error)[:FAILURE_TEXT_MAX_LENGTH]}"


def index_by_eudonet_number(identifiers: Iterable[tuple[str, str]]) -> dict[str, str]:
    """`1101` numbers behind the identifiers of other channels → "organization: identifier".

    The `Paris_` prefix is stripped, a bare identifier is taken whole; ours are skipped.
    """
    numbers: dict[str, str] = {}
    for identifier, organization in identifiers:
        if identifier.startswith(IDENTIFIER_PREFIX):
            continue
        number = OTHER_CHANNEL_PREFIX.sub("", identifier, count=1)
        numbers.setdefault(number, f"{organization}: {identifier}")
    return numbers


def discard_published_elsewhere(df: pl.DataFrame, numbers: dict[str, str] | None) -> pl.DataFrame:
    """Drop the regulations whose `1101` number another channel already published (R-80).

    `None` is a list never loaded (`fetch_raw_data` did not run): said, not passed over.
    """
    if numbers is None:
        logger.warning("R-80: the numbers published elsewhere were never loaded, none held back")
        return df
    if not numbers:
        return df
    published = pl.col("a_identifier").is_in(list(numbers))
    held_back = df.filter(published)
    if held_back.height:
        collisions = sorted(
            f"{number} ({numbers[number]})" for number in held_back["a_identifier"].unique()
        )
        logger.warning(
            f"Dropping {held_back.height} rows of {len(collisions)} regulations already "
            f"published by another channel (R-80): {collisions}"
        )
    return df.filter(~published)
