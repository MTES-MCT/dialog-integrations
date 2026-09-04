"""Reading an order number out of the Métropole de Lyon free-text field.

`precisionreglementation` is typed by hand by the 58 municipalities of the metropolitan
area, so every spelling of "order number" coexists in it:

    ZCA : 2022 - Arrêté N°2024RP44520 du 29/09/2022
    Arrêté n°2024CIR177610A1 du 10/10/24
    Arrêté N°UC 22-272 du 12/05/22
    Arrêté n°VN-2021-AP-001 le 19/10/21
    Proposition zone apaisable = Zone 30 - Arrêté n°2021-055 le 02/06/21

This is a level-3 identifier in the sense of R-20: the number is real, but it is
recovered by parsing free text. If a clerk reformats their entry, the rule produces a
different identifier and the pipeline creates a duplicate. The rule below absorbs the
eight spellings observed on 2026-08-12; it will not absorb the ninth.
"""

import re

# « Arrêté N° », « Arrêté n », « Arrêté » — then the number, non-greedy, up to the first
# closing delimiter: "du", "le", a full stop followed by a space, a comma, or the end of
# the string. The number itself may contain a space ("UC 22-272").
ORDER_NUMBER_PATTERN = re.compile(
    r"[Aa]rr[eê]t[eé]\s*[Nn]?\s*[°ºo]?\s*"
    r"([A-Za-z0-9][A-Za-z0-9\-/_\.\s]*?)"
    r"(?=\s+(?:du|le|en|à)\s|\.\s|\.$|,|$)",
    re.IGNORECASE,
)

# The annotation that marks a *project* rather than an order in force. 6 057 segments
# carry it, and 5 985 of them are still limited to 50 km/h: reading the order number it
# cites as a justification would publish 682 km of 30 km/h zones that do not exist.
PROJECT_PATTERN = re.compile(r"proposition", re.IGNORECASE)

# Words that show the cited order is about vehicle dimensions and not only about the
# calmed-traffic zone the field otherwise describes.
DIMENSION_PATTERN = re.compile(
    r"tonnage|gabarit|poids|hauteur|largeur|longueur",
    re.IGNORECASE,
)


def order_number(text: str | None) -> str | None:
    """Order number read from the free-text field, or None.

    Internal whitespace is collapsed and a trailing full stop dropped, so that one order
    always yields one identifier. Case is preserved: source numbers are case-sensitive
    (`2023P0051-LP`).
    """
    if not text:
        return None
    match = ORDER_NUMBER_PATTERN.search(text)
    if not match:
        return None
    number = " ".join(match.group(1).split()).rstrip(".")
    # A one- or two-character number is not a number, it is a parsing leftover.
    return number if len(number) > 2 else None


def order_key(number: str | None) -> str | None:
    """Grouping key for an order number, blind to spelling variants.

    The same order is typed `2021 355RGC` or `2021_355RGC`, `2021-23` or `2021.23`.
    Without this normalisation one act yields two DiaLog regulations, hence a duplicate.
    Measured on the calmed-traffic zones: 558 raw numbers collapse to 555 keys, and the
    three merges were checked one by one — same municipality, same date, same act.
    """
    if not number:
        return None
    return re.sub(r"[^A-Z0-9]", "", number.upper())


def is_project(text: str | None) -> bool:
    """Does the field describe a planned zone rather than an order in force?"""
    return bool(text) and bool(PROJECT_PATTERN.search(text))  # type: ignore[arg-type]


def mentions_dimensions(text: str | None) -> bool:
    """Does the field tie its order number to a vehicle dimension limit?

    `precisionreglementation` describes the *calmed-traffic zone*. Of the 2 245 segments
    carrying a dimension limit, 1 191 also carry an order number, but only 46 mention a
    dimension. Attaching the other 1 145 to the number they cite would make Lyon's
    "Ville 30" order say something it never said.
    """
    return bool(text) and bool(DIMENSION_PATTERN.search(text))  # type: ignore[arg-type]
