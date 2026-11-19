"""The act numbers another act repeals, amends, replaces or suspends."""

import re
from enum import StrEnum

from .free_text import clauses
from .text import plain_text

# An act number as another act cites it: « n° 89-10393 », « n°2013P0842 », « n° 64.10389 ».
_CITED_NUMBER = re.compile(r"n\s?°\s?([PpTt]?\d{2,4}\s?[-.]?\s?[A-Za-z]?\s?\d{2,6})")
_REPEALS_OR_AMENDS = re.compile(r"abrog|modifi|remplac|suspend|supprim", re.IGNORECASE)
_NUMBER_PARTS = re.compile(r"P?(\d{4}|\d{2})(?:[-.]|(?=[A-Z]))([A-Z]?)0*(\d+)(?:-\d+)?")
# "arrêté municipal n° 2011-104" is not the prefect's "2011-00104", which reads the same once
# the zeros are dropped: the adjective before a number, or before the list it ends, says whose.
_AUTHORITY = re.compile(r"\b(municipa|préfectora)\w*", re.IGNORECASE)


class Authority(StrEnum):
    CITY = "city"
    PREFECT = "prefect"


def act_number_key(number: str) -> tuple[int, str, int] | None:
    """ "89-10393", "1989-10393", "64.10389", "2013P0842" → (year, letter, serial): the forms an
    act number takes in Eudonet and in the texts that cite it."""
    match = _NUMBER_PARTS.fullmatch(re.sub(r"\s", "", number).upper())
    if match is None:
        return None
    year = int(match.group(1))
    if year < 100:
        year += 1900 if year > 30 else 2000
    return year, match.group(2), int(match.group(3))


def numbers_repealed_or_amended(
    texts: list[str],
) -> set[tuple[tuple[int, str, int], Authority | None]]:
    """The act numbers a sentence of `texts` repeals, amends, replaces or suspends, whether the
    sentence is otherwise understood or not, with the authority the citation names, if any."""
    keys = set()
    for text in texts:
        for sentence in clauses(text) or [plain_text(text)]:
            if not _REPEALS_OR_AMENDS.search(sentence):
                continue
            for match in _CITED_NUMBER.finditer(sentence):
                key = act_number_key(match.group(1))
                if key is not None:
                    keys.add((key, _authority(sentence[: match.start()])))
    return keys


def _authority(before: str) -> Authority | None:
    adjectives = _AUTHORITY.findall(before)
    if not adjectives:
        return None
    return Authority.CITY if adjectives[-1].lower().startswith("municipa") else Authority.PREFECT
