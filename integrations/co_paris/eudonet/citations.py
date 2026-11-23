import re
from enum import StrEnum

from .free_text import clauses
from .text import plain_text

# An act number as another act cites it: « n° 89-10393 », « n°2013P0842 », « n° 64.10389 ».
_CITED_NUMBER = re.compile(r"n\s?°\s?([PpTt]?\d{2,4}\s?[-.]?\s?[A-Za-z]?\s?\d{2,6})")
_REPEALS_OR_AMENDS = re.compile(r"abrog|modifi|remplac|suspend|supprim", re.IGNORECASE)
_REPEALS = re.compile(r"abrog|remplac|supprim", re.IGNORECASE)
_IN_PART = re.compile(
    r"en ce qui concerne|pour ce qui concerne|concernant|s'agissant|en tant qu|relatives? (?:à|aux)"
    r"|dans (?:sa|leur|la) partie|partiellement|portions?|parties? de|tronçons?|sections?"
    r"|\barticles?\b|alinéa|paragraphe|annexe",
    re.IGNORECASE,
)
_BY_ACT = re.compile(r"\bpar (?:l['’]\s?)?arrêtés?\b[^.;:]*$", re.IGNORECASE)
_AUTHOR = re.compile(
    r"\b(?:abrog|modifi|remplac|suspend|supprim)\w*\s+(?:par|dans)\s+"
    r"(?:(?:l['’]\s?|les?\s+)?arrêtés?\b(?:(?!n\s?°)[^.;:])*|les?\s+)$",
    re.IGNORECASE,
)
_NUMBER_PARTS = re.compile(r"P?(\d{4}|\d{2})(?:[-.]|(?=[A-Z]))([A-Z]?)0*(\d+)(?:-\d+)?")
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


def citations(texts: list[str]) -> set[tuple[tuple[int, str, int], Authority | None, bool]]:
    """The act numbers a sentence of `texts` repeals, amends, replaces or suspends, whether the
    sentence is otherwise understood or not. The act named as the one that repeals or amends is
    not cited."""
    found = set()
    for text in texts:
        for sentence in clauses(text) or [plain_text(text)]:
            if not _REPEALS_OR_AMENDS.search(sentence):
                continue
            repeals = _REPEALS.search(sentence) is not None and not _IN_PART.search(sentence)
            for match in _CITED_NUMBER.finditer(sentence):
                key = act_number_key(match.group(1))
                before = sentence[: match.start()]
                if key is not None and not _AUTHOR.search(before):
                    found.add((key, _authority(before), repeals and not _BY_ACT.search(before)))
    return found


def _authority(before: str) -> Authority | None:
    adjectives = _AUTHORITY.findall(before)
    if not adjectives:
        return None
    return Authority.CITY if adjectives[-1].lower().startswith("municipa") else Authority.PREFECT
