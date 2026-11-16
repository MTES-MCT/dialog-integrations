"""Free texts of Eudonet that say nothing the published measure does not already carry.

Closed lists, read on the published regulations of 2026-09-08 and 2026-11-16: every sentence
of a text must match one of them, or the row it qualifies is dropped. A sentence about
something DiaLog does not publish (cycles riding against the flow, barriers) counts as
neutral; a time, a day, a vehicle, an exemption or an excluded street never does.
"""

import re

from .text import plain_text

# Splits a text into sentences; a list ("sont abrogés : - l'arrêté n°…") stays with its head,
# and an initial ("R. 417-10") ends no sentence.
_SENTENCE_END = re.compile(r"(?<=[.;!?])(?<!\b[A-Z]\.)\s+(?!-)")
# A time or a day narrows the measure, whatever the sentence around it.
_TIME_OR_DAY = re.compile(
    r"\b\d{1,2}\s?h\s?\d{0,2}\b|\bheures?\b|\b(lundi|mardi|mercredi|jeudi|vendredi|samedi|"
    r"dimanche|week-end|fériés?|nuit|marchés?)s?\b",
    re.IGNORECASE,
)

# A repeal lists the acts it repeals, with their own days ("les jours du marché"): those are
# not a time of the measure.
REPEAL_SENTENCE = (
    r"^(les dispositions (de l'|des )|l'|les )?(arrêtés?|articles?|dispositions)\b.*\b(est|sont) "
    r"abrogée?s?\b|^(sont|est) (également |partiellement )?abrogée?s?\b"
    r"|\bénumérés ci-après sont abrogés"
)
# This act amends another one ("l'annexe de l'arrêté n° 2010-00831 est modifiée…"): its own
# measures stay as published, and the days or places quoted are the other act's.
AMENDS_ANOTHER_ACT = (
    r"^(?!.*présent arrêté).*\b(de l'|l'|l’|de l’)arrêté (préfectoral |municipal )?"
    r"(n° ?[\w./-]+|du \d)"
    r".*\b(modifiée?s?|complétée?s?|remplacée?s?|ajoutée?s?|deviennent|inchangée?s?)\b"
)
ARTICLE_SENTENCES = {
    "contrary provisions": (
        r"(abrog|annul|suspend|remplac)\w*\b.*\bdispositions? (antérieures? )?contraires?"
        r"|dispositions? contraires?.*\b(abrog|annul|suspend|remplac)"
    ),
    "repeals another act": REPEAL_SENTENCE,
    "suspends another act": (
        r"^(les dispositions (de l'|des )|l'|les )?(arrêtés?|dispositions)\b.*\b(est|sont) "
        r"suspendue?s?\b"
    ),
    "applies from the signage or until the end of the works": (
        r"(applicables?|s'appliquent|s'applique|entreront en vigueur|entre en vigueur)\b.*"
        r"\b(fin des travaux|signalisation|publication)"
    ),
    "completes another act": r"\bcompl[èe]t\w* (le |les )?(paragraphes?|l'arrêté)",
    "penalties": r"procès-verbaux|\benlèvement\b|R\.? ?417-10|L\.? ?325-1",
    "cycles against the flow": (
        r"(cycl\w*|vélos|cyclistes).*(contre[- ]?sens|sens inverse|deux sens|double sens"
        r"|céder le passage)|contre-sens cyclables?"
    ),
    "barriers": r"\b(barrières?|portails?)\b",
    "amends another act": AMENDS_ANOTHER_ACT,
    # Vigipirate: "applicables jusqu'au 31 décembre … et reconduites tacitement", a permanent.
    "renewed tacitly": r"reconduites? tacitement",
}

# Only on a regulation made of pedestrian areas, which DiaLog publishes with the local
# access exemption: the categories of vehicles of that access.
PEDESTRIAN_ACCESS_ARTICLE = re.compile(
    r"desserte (interne|locale)?.*\b(aires?|voies?) piétonnes?"
    r"|accès (à|de) (cette|ces) (aires?|voies?)",
    re.IGNORECASE,
)

# A title that names a day, an hour or a market: the act holds at those times only, which no
# field carries ("une aire piétonne les dimanches", "à certaines heures"). Not "km/heure",
# nor a street ("rue du Marché Saint-Honoré").
TITLE_NAMES_A_TIME = re.compile(
    r"\b(lundi|mardi|mercredi|jeudi|vendredi|samedi|dimanche)s?\b|jours? fériés?"
    r"|certaines heures|(?<!km/)\bheures?\b|\bnuits?\b"
    r"|marchés? (alimentaires?|découverts?|aux puces)|tenue (de|des) marchés?"
    r"|abords (immédiats )?(du|des) marchés?",
    re.IGNORECASE,
)
# Catalog keys (`1103`, `1122`) of operations that hold on some days only.
TIME_LIMITED_KEYS = frozenset({"PARIS RESPIRE", "MARCHES"})

# Regulation notes (`1194`): the office's own bookkeeping.
NOTE_SENTENCES = {
    "sent for publication": (
        r"\b(transmis|envoyée?)\b.*\b(mail|mairie|bmo|moa|scop|mpc|affichage)\b"
        r"|^(arrêté )?transmis\b|^envoyée? le\b"
    ),
    "entered in the software": r"injecté dans gacs",
    "film shooting": r"^tournage de film$",
}

# Measure paragraphs (`1254`).
PARAGRAPH_SENTENCES = {
    "penal qualification": (
        r"^tout (arrêt ou )?stationnement d'un véhicule en infraction avec cette interdiction "
        r"est considéré comme (très )?gênant"
    ),
    "names the zone 30": (
        r"^il est institué une zone 30 dénommée [\"“«][^\"”»]+[\"”»] délimitée? comme suit$"
    ),
}
# The name of a zone 30 ("Quartier Vert « Censier »"): a few words, no number.
_ZONE_NAME = re.compile(
    r"^(zone 30 |quartier vert |qt vert )?[\"“« ]*[^\d.;:,]{2,40}[\"”» ]*$", re.I
)
_ZONE_NAME_FORBIDDEN = re.compile(r"\b(sauf|pas|exclu\w*|inclus\w*|autoris\w*|interdit\w*)\b", re.I)


# An act number as another act cites it: « n° 89-10393 », « n°2013P0842 », « n° 64.10389 ».
_CITED_NUMBER = re.compile(r"n\s?°\s?([PpTt]?\d{2,4}\s?[-.]?\s?[A-Za-z]?\s?\d{2,6})")
_REPEALS_OR_AMENDS = re.compile(r"abrog|modifi|remplac|suspend|supprim", re.IGNORECASE)
_NUMBER_PARTS = re.compile(r"P?(\d{4}|\d{2})(?:[-.]|(?=[A-Z]))([A-Z]?)0*(\d+)(?:-\d+)?")


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


def numbers_repealed_or_amended(texts: list[str]) -> set[tuple[int, str, int]]:
    """The act numbers a sentence of `texts` repeals, amends, replaces or suspends."""
    keys = set()
    for text in texts:
        for sentence in sentences(text) or [plain_text(text)]:
            if not _REPEALS_OR_AMENDS.search(sentence):
                continue
            for number in _CITED_NUMBER.findall(sentence):
                key = act_number_key(number)
                if key is not None:
                    keys.add(key)
    return keys


def sentences(text: str | None) -> list[str]:
    plain = plain_text(text)
    return [part.strip(" .;:-–") for part in _SENTENCE_END.split(plain) if part.strip(" .;:-–")]


def _all_match(parts: list[str], patterns: dict[str, str]) -> bool:
    return all(
        any(re.search(pattern, part, re.IGNORECASE) for pattern in patterns.values())
        for part in parts
    )


def article_is_neutral(text: str | None, has_pedestrian_area: bool) -> bool:
    """`has_pedestrian_area`: the act holds a pedestrian area, which DiaLog publishes with the
    local access exemption; the categories of that access say nothing more."""
    if not plain_text(text):
        return True
    if has_pedestrian_area and PEDESTRIAN_ACCESS_ARTICLE.search(plain_text(text)):
        return not _TIME_OR_DAY.search(plain_text(text))
    parts = sentences(text)
    return _all_match(parts, ARTICLE_SENTENCES) and not any(
        _TIME_OR_DAY.search(part)
        and not re.search(REPEAL_SENTENCE, part, re.IGNORECASE)
        and not re.search(AMENDS_ANOTHER_ACT, part, re.IGNORECASE)
        for part in parts
    )


def title_names_a_time(title: str | None) -> bool:
    return bool(TITLE_NAMES_A_TIME.search(plain_text(title)))


def note_is_neutral(text: str | None) -> bool:
    return _all_match(sentences(text), NOTE_SENTENCES)


# "Toutefois ces dispositions ne sont pas applicables aux véhicules de secours", "sauf aux
# véhicules de secours": an exemption the measure's own parameter would carry.
_PARAGRAPH_EXEMPTION = re.compile(
    r"^(toutefois,? )?(ces dispositions ne sont pas applicables|sauf|à l'exception des?)\s+"
    r"(?P<who>.+?)[\s.]*$",
    re.IGNORECASE,
)


def paragraph_exemptions(text: str | None, known: frozenset[str]) -> list[str] | None:
    """The exempted vehicle labels a paragraph names, if it says nothing else and names only
    `known` labels ("aux véhicules de secours"); None otherwise."""
    match = _PARAGRAPH_EXEMPTION.match(plain_text(text))
    if match is None:
        return None
    labels = []
    for part in re.split(r",\s*|\s+et\s+", match.group("who")):
        label = re.sub(r"^(aux |des |les )?", "aux ", part.strip(), flags=re.IGNORECASE).lower()
        if label not in known:
            return None
        labels.append(label)
    return labels or None


def paragraph_is_neutral(text: str | None, is_zone_30: bool) -> bool:
    plain = plain_text(text)
    if not plain:
        return True
    if is_zone_30 and _ZONE_NAME.match(plain) and not _ZONE_NAME_FORBIDDEN.search(plain):
        return True
    return _all_match(
        sentences(text),
        {**PARAGRAPH_SENTENCES, "cycles": ARTICLE_SENTENCES["cycles against the flow"]},
    )
