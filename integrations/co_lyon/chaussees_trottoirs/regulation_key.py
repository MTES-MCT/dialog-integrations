"""Reading an order number out of `precisionreglementation`, hand-typed by 58 municipalities.

    ZCA : 2022 - Arrêté N°2024RP44520 du 29/09/2022
    Arrêté N°UC 22-272 du 12/05/22
    Proposition zone apaisable = Zone 30 - Arrêté n°2021-055 le 02/06/21

A last-resort identifier, read from free text: a clerk who reformats an entry creates a
duplicate. The
pattern absorbs the eight spellings observed on 2026-08-12, not a ninth.
"""

import re
from collections.abc import Iterable

ORDER_NUMBER_PATTERN = re.compile(
    # Also "Arrété" (2 rows) and "Arrêtéé" (2 rows).
    r"arr[eéê]t[eéê]{1,2}"
    # One word in between: "Arrêté permanent N° : AR-12/12/2025-17".
    r"(?:\s+permanent)?"
    # "N°", "n", "N° :", or nothing at all. Repeated: "Arrêté n°n°08/2023" exists.
    r"(?:\s*[Nn]\s*[°ºo]?)*"
    r"\s*:?\s*"
    # Non-greedy; may hold a space ("UC 22-272") and a glued slash ("08/2023").
    r"([A-Za-z0-9][A-Za-z0-9\-/_\.\s]*?)"
    # A spaced slash closes the number unless another order follows: in "N°2024RP44520 /
    # Lim hauteur - Arrêté N°2022RP40610" the second order carries the height limit.
    # Not a colon: only 3 rows would need it, and it separates "ZCA" from its vintage.
    r"(?=\s+(?:du|le|en|à)\s|\s+/(?!.*arr[eéê]t[eéê])"
    # Orders stack newest first, so a dash closes the one in force, but only before "ZCA",
    # "ZTL" or a year: "VOI-2023 - 120" (6 rows) and "PV 2023 - 478" (3) are one number.
    r"|\s+-\s(?=(?:ZCA|ZTL)\b|\d{4}\b)|[\r\n]|\.\s|\.$|,|$)",
    re.IGNORECASE,
)

# Marks a project, not an order in force: 5 985 of its 6 057 segments are still at 50 km/h,
# and reading its order number would publish 682 km of 30 km/h zones that do not exist.
# "apaisab" catches the typo "Prpoposition" (1 row).
PROJECT_PATTERN = re.compile(r"propos|apaisab", re.IGNORECASE)


def order_number(text: str | None) -> str | None:
    """Order number read from the free-text field, or None.

    Case is kept: source numbers are case-sensitive (`2023P0051-LP`).
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
    """Grouping key blind to spelling variants: `2021 355RGC` and `2021_355RGC` are one act.

    On the calmed-traffic zones, 558 raw numbers collapse to 555 keys; the three merges
    were checked one by one (same municipality, same date, same act).
    """
    if not number:
        return None
    return re.sub(r"[^A-Z0-9]", "", number.upper())


def is_project(text: str | None) -> bool:
    """Does the field describe a planned zone rather than an order in force?"""
    return bool(text) and bool(PROJECT_PATTERN.search(text))  # type: ignore[arg-type]


def foreign_order_keys(identifiers: Iterable[str], own_prefix: str) -> frozenset[str]:
    """Order keys already published in the organisation by a channel other than ours.

    That channel's ~800 identifiers read `{MUNICIPALITY}_{number}` (`LYON_2022RP40610`);
    publishing `MGL-CT-2022RP40610` too would put one act twice in DiaLog (19 numbers on
    2026-09-22). The prefix follows no convention (52 spellings), so every suffix cut at
    an underscore is a candidate; cutting at underscores rather than matching a trailing
    substring keeps `2019` from matching `TASSIN_2019_145`.
    """
    keys: set[str] = set()
    for identifier in identifiers:
        if identifier.startswith(own_prefix):
            continue
        parts = identifier.split("_")
        for start in range(len(parts)):
            key = order_key("_".join(parts[start:]))
            if key:
                keys.add(key)
    return frozenset(keys)
