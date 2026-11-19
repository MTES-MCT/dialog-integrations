"""One reading for every free text of Eudonet: understood in full, or not.

A text is cut into clauses. A clause is understood when what is left of it, once the words the
record's own fields explain are cut out, is only punctuation and linking words, or matches one
of the `FORMULAS`. A formula carries a code: `NEUTRAL`, or an effect the caller applies to the
published measure (an exemption, bicycles on a one-way street…). A clause nothing explains makes
the text not understood, and the caller drops the record the text qualifies.

The formulas are closed lists, read on the published regulations of 2026-09-08, 2026-11-16 and
2026-11-17: a sentence about something DiaLog does not publish (barriers, penalties) counts as
neutral; a time, a day, a vehicle, an exemption or an excluded street never does.
"""

import json
import re
import unicodedata
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from enum import StrEnum
from functools import cache

from .locations import STREET_NAMES_FILE
from .text import plain_text


class Code(StrEnum):
    NEUTRAL = "neutral"
    # Values: the exempted vehicle labels of `vocabulary.EXEMPTION_BY_LABEL`.
    EXEMPTION = "exemption"
    # Values: who may ride against the flow, exempted on the act's one-way streets; a vehicle
    # without an exemption type of the API (buses, scooters) goes as `other` with its words
    # (decided 2026-11-19).
    AGAINST_THE_FLOW = "against the flow"
    # Values: the categories of vehicles that may enter a pedestrian area.
    PEDESTRIAN_ACCESS = "pedestrian access"
    # The name of a zone 30, said by its own measure only.
    ZONE_NAME = "zone name"
    # A number of places: the point itself on a single house number, narrower than a section.
    RESERVED_PLACES = "reserved places"
    # This act amends another one and quotes the new wording ("… est ainsi modifié : - le
    # trottoir côté impair…"): the other act's business in an article, this measure's own
    # extent in a paragraph.
    AMENDMENT_WORDING = "amendment wording"


@dataclass(frozen=True)
class Effect:
    code: Code
    values: tuple[str, ...] = ()


@dataclass(frozen=True)
class Reading:
    effects: tuple[Effect, ...] = ()
    # The clauses nothing explains, as written.
    unread: tuple[str, ...] = ()
    # Among them, those a formula of `BLOCKING_FORMULAS` names.
    blocking: tuple[str, ...] = ()

    @property
    def understood(self) -> bool:
        return not self.unread

    def codes(self) -> set[Code]:
        return {effect.code for effect in self.effects}


@dataclass(frozen=True)
class Explained:
    """What the record's own fields print into a generated text: the locations (`spans`), the
    template's wording (formulas, parameter values), and house numbers.

    The wording is cut to tell a clause says nothing more, not before the formulas are tried:
    "Tout stationnement … considéré comme" + "gênant" still reads as the penal qualification."""

    spans: tuple[str, ...] = ()
    numbers: frozenset[str] = field(default_factory=frozenset)
    wording: tuple[str, ...] = ()
    # Every location of the record is a point or a bounded section: a street the text sets
    # it "entre" or "au croisement de" narrows nothing the location does not.
    narrower_than_the_street: bool = False


@dataclass(frozen=True)
class Formula:
    name: str
    pattern: re.Pattern[str]
    code: Code = Code.NEUTRAL
    # A repeal quotes the other act's days ("les jours du marché"): not a time of this act.
    names_times: bool = False
    # The formula is a whole sentence: what follows the match must say nothing more.
    whole: bool = False


# A time or a day narrows the measure, whatever the clause around it.
_TIME_OR_DAY = re.compile(
    r"\b\d{1,2}\s?h\s?\d{0,2}\b|\bheures?\b|\b(lundi|mardi|mercredi|jeudi|vendredi|samedi|"
    r"dimanche|week-end|fériés?|nuit|marchés?)s?\b",
    re.IGNORECASE,
)

# Splits a text into sentences; a list ("sont abrogés : - l'arrêté n°…") stays with its head,
# and an initial ("R. 417-10") ends no sentence.
_SENTENCE_END = re.compile(r"(?<=[.;!?])(?<!\b[A-Z]\.)\s+(?!-)")
# The mail merge glues its clauses without a full stop: a clause starts at each formula of the
# catalogs `1236`, `1204` and `1207` (2026-11-18).
TEMPLATE_CLAUSES = (
    "Toutefois ces dispositions ne sont pas applicables",
    "Tout stationnement d'un véhicule en infraction avec cette interdiction",
    "Ces dispositions sont applicables",
    "Le stationnement y est autorisé de manière",
    "Le mouvement de tourne à droite est autorisé",
    "Le mouvement direct est autorisé",
)
_CLAUSE_START = re.compile(
    r"(?<=\S)\s+(?=(?:{}))".format("|".join(re.escape(c) for c in TEMPLATE_CLAUSES)),
    re.IGNORECASE,
)

# Printed before the opening formula of a temporary act's measure.
TEMPLATE_PREFIX = "A titre provisoire"

# Linking words, as `_tokens` writes them: no accent, no apostrophe.
LINKING_WORDS = frozenset(
    {
        *("le", "la", "les", "l", "un", "une", "de", "du", "des", "d", "a", "au", "aux"),
        *("et", "ni", "en", "sur", "entre", "n", "no", "jusqu", "bis", "ter", "quater"),
    }
)
# "n°21", "21bis", "13b"; not "10h", a time.
_NUMBER_TOKEN = re.compile(r"(?:n|no)?0*(\d+)(?:bis|ter|quater|[a-gi-z])?")

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
# Says the whole location, as the location itself does.
WHOLE_LOCATION = (
    r"sur (toute la longueur( de la voie)?|toutes les places|tout le stationnement"
    r"|l'intégralité de la voie|la totalité de la voie)"
)
WHOLE_LOCATION_WORDING = (
    "sur toute la longueur de la voie",
    "sur toute la longueur",
    "sur toutes les places",
    "sur tout le stationnement",
    "sur l'intégralité de la voie",
    "sur la totalité de la voie",
)
COMPLETES_ANOTHER_ACT = r"\bcompl[èe]t\w* (le |les )?(paragraphes?|l'arrêté)"
_CYCLES = r"(cycl\w*|vélos|cyclistes)"
_AGAINST_THE_FLOW = r"(contre[- ]?sens|sens inverse|deux sens|double sens)"
_OTHERS = r"(autobus|\bbus\b|taxis?|transports? en commun|engins|trottinettes)"
# Who rides against the flow: the catalog label of its exemption, or its own words.
_AGAINST_THE_FLOW_RIDERS = (
    (re.compile(r"\b(cycl\w*|vélos|cyclistes)\b", re.IGNORECASE), "aux cycles"),
    (re.compile(r"\btaxis?\b", re.IGNORECASE), "aux taxis"),
    (re.compile(r"\bvéhicules de secours\b", re.IGNORECASE), "aux véhicules de secours"),
    (
        re.compile(
            r"\b(véhicules de transports? en commun|transports? en commun|autobus|bus)\b",
            re.IGNORECASE,
        ),
        None,
    ),
    (re.compile(r"\bengins de déplacement personnels?( motorisés)?", re.IGNORECASE), None),
    (re.compile(r"\btrottinettes\b", re.IGNORECASE), None),
)
_BOUND_TO_A_TRACK = re.compile(r"tenus d'emprunter la (piste|bande) cyclable", re.IGNORECASE)


def _formula(name: str, pattern: str, code: Code = Code.NEUTRAL, **options) -> Formula:
    return Formula(name, re.compile(pattern, re.IGNORECASE), code, **options)


# Tried in order, the first that matches wins: the formulas that carry an effect come before
# the neutral ones that would match the same clause ("l'arrêté n° … est modifié en ce sens que
# les cycles sont autorisés à double sens").
FORMULAS: tuple[Formula, ...] = (
    _formula(
        "cycles may not ride against the flow",
        rf"{_CYCLES}.*\bne sont pas autorisés\b.*{_AGAINST_THE_FLOW}"
        rf"|{_CYCLES}.*\bcéder le passage",
    ),
    _formula(
        "others ride against the flow with the cycles",
        rf"({_OTHERS}.*{_CYCLES}|{_CYCLES}.*{_OTHERS}).*{_AGAINST_THE_FLOW}",
        Code.AGAINST_THE_FLOW,
    ),
    _formula(
        "cycles bound to a cycle track",
        rf"{_CYCLES}.*{_AGAINST_THE_FLOW}.*\btenus d'emprunter\b",
        Code.AGAINST_THE_FLOW,
    ),
    _formula(
        "cycles ride against the flow",
        rf"{_CYCLES}.*{_AGAINST_THE_FLOW}|contre-sens cyclables?|cyclable à contre-?sens",
        Code.AGAINST_THE_FLOW,
    ),
    _formula(
        "pedestrian access",
        r"desserte (interne |locale )?(de |des |du )?(cette |ces |catte |la |les |l'|l’)?"
        r"(aires?|voies?) piétonnes?"
        r".*\b(limitée|réservée|autorisée)s?\b[^:]*\baux\b[^:]*:\s*\S"
        r"|\baccès (à|de) (cette|ces) (aires?|voies?)\b.*\bautorisée? qu'aux\b[^:]*:\s*\S",
        Code.PEDESTRIAN_ACCESS,
    ),
    _formula(
        "exemption",
        r"^(toutefois,? )?(ces dispositions ne sont pas applicables|sauf|à l'exception des?)\s+"
        r"(?P<who>.+?)[\s.]*$",
        Code.EXEMPTION,
    ),
    _formula(
        "reserved places",
        # The kind of place may follow ("payant", "ZL permanente"), not who may use it.
        r"^(?!.*\b(sauf|pas|ni|hors|except\w*|réservée?s? aux|uniquement|seulement)\b)"
        r"\(?(sur )?(\d+|une?) (places?(\.s)?|emplacements?)\b[^\d;:]{0,60}\)?$|^\(?\d+\)?$",
        Code.RESERVED_PLACES,
    ),
    _formula(
        "contrary provisions",
        r"(abrog|annul|suspend|remplac)\w*\b.*\bdispositions? (antérieures? )?contraires?"
        r"|dispositions? contraires?.*\b(abrog|annul|suspend|remplac)",
    ),
    _formula("repeals another act", REPEAL_SENTENCE, names_times=True),
    _formula(
        "suspends another act",
        r"^(les dispositions (de l'|des )|l'|les )?(arrêtés?|dispositions)\b.*\b(est|sont) "
        r"suspendue?s?\b",
        names_times=True,
    ),
    _formula(
        "applies from the signage or until the end of the works",
        r"(applicables?|s'appliquent|s'applique|entreront en vigueur|entre en vigueur)\b.*"
        r"\b(fin des travaux|signalisation|publication)",
    ),
    _formula("penalties", r"procès-verbaux|\benlèvement\b|R\.? ?417-10|L\.? ?325-1"),
    _formula(
        "penal qualification",
        r"^tout (arrêt (ou|et) )?stationnement d'un véhicule en infraction avec cette interdiction "
        r"est considéré comme (très )?gênant( et très gênant)?",
        whole=True,
    ),
    _formula("barriers", r"\b(barrières?|portails?)\b"),
    _formula(
        "amends another act, with the new wording",
        rf"(?:{AMENDS_ANOTHER_ACT}|{COMPLETES_ANOTHER_ACT}).*(:\s*\S|[«“\"])",
        Code.AMENDMENT_WORDING,
        names_times=True,
    ),
    _formula("amends another act", AMENDS_ANOTHER_ACT, names_times=True),
    _formula("completes another act", COMPLETES_ANOTHER_ACT),
    # Vigipirate: "applicables jusqu'au 31 décembre … et reconduites tacitement", a permanent.
    _formula("renewed tacitly", r"reconduites? tacitement"),
    _formula(
        "names the zone 30",
        r"^il est institué une zone 30 dénommée [\"“«][^\"”»]+[\"”»] délimitée? comme suit$",
    ),
    _formula(
        "whole location",
        rf"^({WHOLE_LOCATION})$",
    ),
    # The office's own bookkeeping, in the regulation notes (`1194`).
    _formula(
        "sent for publication",
        r"\b(transmis|envoyée?)\b.*\b(mail|mairie|bmo|moa|scop|mpc|affichage)\b"
        r"|^(arrêté )?transmis\b|^envoyée? le\b",
        names_times=True,
    ),
    _formula("entered in the software", r"injecté dans gacs"),
    _formula("film shooting", r"^tournage de film$"),
    # A few words without a number ("Quartier Vert « Censier »"): only a zone 30 has a name.
    _formula(
        "zone name",
        r"^(?!.*\b(sauf|pas|exclu\w*|inclus\w*|autoris\w*|interdit\w*)\b)"
        r"(zone 30 |quartier vert |qt vert )?[\"“« ]*[^\d.;:,]{2,40}[\"”» ]*$",
        Code.ZONE_NAME,
    ),
)

# Formulas arbitrated as blocking: the record is dropped, as for any clause no formula covers,
# even if a formula matches too. Listed, they leave the count of `recurring_formulas`.
BLOCKING_FORMULAS: dict[str, str] = {}


def _fold(text: str) -> str:
    """Lower case, without accents, and the same length as `text`: an offset of one is an
    offset of the other."""
    folded = []
    for char in text.lower():
        base = "".join(
            c for c in unicodedata.normalize("NFKD", char) if not unicodedata.combining(c)
        )
        folded.append(base if len(base) == 1 else char)
    return "".join(folded)


def _tokens(text: str) -> list[tuple[str, int, int]]:
    return [(m.group(), m.start(), m.end()) for m in re.finditer(r"[a-z0-9]+", _fold(text))]


# A line ends a sentence: a break or a paragraph in HTML, a new line in plain text.
_HTML_LINE_END = re.compile(r"<br\s*/?>|</p>|</div>|</li>", re.IGNORECASE)
_LINE_MARK = "\x00"


# A line that goes on with the one before: a list item ("- l'arrêté n°…", "« rue … »") after a
# colon or another item, or the rest of a sentence cut in two ("à Paris 8ème, est abrogé").
_LIST_ITEM = re.compile(r"^([-–•·«\"“]|l['’]arrêté|les arrêtés|n\s?°|\d)", re.IGNORECASE)
_GOES_ON = re.compile(r"^[a-zàâçéèêëîïôûù(,]")


def _lines(text: str) -> list[str]:
    """The lines of a text, a list kept with its head and a cut sentence made whole."""
    marked = _HTML_LINE_END.sub(_LINE_MARK, text) if "<" in text else text.replace("\n", _LINE_MARK)
    lines: list[str] = []
    in_list = False
    for line in (plain_text(part) for part in marked.split(_LINE_MARK)):
        if not line:
            continue
        item = _LIST_ITEM.match(line) is not None
        if lines and (
            _GOES_ON.match(line) or line.startswith(("-", "–", "•", "·")) or (item and in_list)
        ):
            lines[-1] = f"{lines[-1]} {line}"
            in_list = in_list or item
        else:
            lines.append(line)
            in_list = False
        if line.rstrip().endswith(":"):
            in_list = True
    return lines


def clauses(text: str | None) -> list[str]:
    """The sentences of a text, its lines, and the clauses the mail merge glued into one."""
    parts = (
        clause.strip(" .;:-–,")
        for line in _lines(text or "")
        for sentence in _SENTENCE_END.split(line)
        for clause in _CLAUSE_START.split(sentence)
    )
    return [part for part in parts if part]


def _cut(clause: str, explained_spans: Iterable[str]) -> str:
    """The clause without the spans the fields explain, longest first, on whole words."""
    tokens = _tokens(clause)
    words = [word for word, _, _ in tokens]
    cut = [False] * len(tokens)
    spans = sorted(
        {tuple(word for word, _, _ in _tokens(span)) for span in explained_spans},
        key=len,
        reverse=True,
    )
    for span in spans:
        if not span:
            continue
        size = len(span)
        for start in range(len(words) - size + 1):
            if tuple(words[start : start + size]) == span and not any(cut[start : start + size]):
                cut[start : start + size] = [True] * size
    return _without(clause, tokens, cut)


def _without(clause: str, tokens: list[tuple[str, int, int]], cut: list[bool]) -> str:
    kept, last = [], 0
    for (_, begin, end), removed in zip(tokens, cut):
        if removed:
            kept.append(clause[last:begin])
            last = end
    kept.append(clause[last:])
    return " ".join(kept)


# The street is the locations' business (decided 2026-11-19): a street of the Paris referential
# named in a free text says nothing more. Where it bounds another one ("entre … et …", "au
# croisement de", "à l'intersection avec", "à l'angle de"), only if the location is narrower than
# the whole street too: on a whole street, the text would narrow what is published.
_CROSSING_WORDS = frozenset({"entre", "croisement", "intersection", "angle"})
_BEFORE_A_NAME = frozenset({"de", "du", "des", "d", "la", "le", "l", "les", "avec", "et"})


@cache
def _street_names_by_head() -> dict[tuple[str, str], list[tuple[str, ...]]]:
    """The referential's names as words, indexed by their first two, longest first."""
    index: dict[tuple[str, str], list[tuple[str, ...]]] = defaultdict(list)
    for name in json.loads(STREET_NAMES_FILE.read_text(encoding="utf-8")):
        words = tuple(word for word, _, _ in _tokens(name))
        if len(words) >= 2:
            index[words[:2]].append(words)
    for names in index.values():
        names.sort(key=len, reverse=True)
    return dict(index)


def _crossing_word(words: list[str], named: list[bool], start: int) -> int | None:
    """Where the "entre", "croisement"… the name at `start` follows is, maybe after another
    name; None if it follows none."""
    before = start - 1
    while before >= 0 and (words[before] in _BEFORE_A_NAME or named[before]):
        before -= 1
    return before if before >= 0 and words[before] in _CROSSING_WORDS else None


def cut_street_names(clause: str, narrower_than_the_street: bool = False) -> str:
    """The clause without the streets it names, except a bound of a location that has none."""
    tokens = _tokens(clause)
    words = [word for word, _, _ in tokens]
    cut = [False] * len(words)
    named = [False] * len(words)
    index = _street_names_by_head()
    start = 0
    while start < len(words) - 1:
        for name in index.get((words[start], words[start + 1]), ()):
            end = start + len(name)
            if tuple(words[start:end]) == name:
                named[start:end] = [True] * len(name)
                crossing = _crossing_word(words, named, start)
                if crossing is None:
                    cut[start:end] = [True] * len(name)
                elif narrower_than_the_street:
                    cut[crossing:end] = [True] * (end - crossing)
                start = end - 1
                break
        start += 1
    return _without(clause, tokens, cut)


def _says_nothing(clause: str, numbers: frozenset[str]) -> bool:
    for word, _, _ in _tokens(clause):
        if word in LINKING_WORDS:
            continue
        number = _NUMBER_TOKEN.fullmatch(word)
        if number is None or number.group(1) not in numbers:
            return False
    return True


def _match(clause: str, numbers: frozenset[str] = frozenset()) -> Formula | None:
    has_time = _TIME_OR_DAY.search(clause) is not None
    for formula in FORMULAS:
        if not formula.names_times and has_time:
            continue
        match = formula.pattern.search(clause)
        if match is None:
            continue
        if formula.whole and not _says_nothing(
            clause[: match.start()] + " " + clause[match.end() :], numbers
        ):
            continue
        return formula
    return None


def _values(formula: Formula, clause: str) -> tuple[str, ...]:
    if formula.code == Code.EXEMPTION:
        match = formula.pattern.search(clause)
        who = match.group("who") if match else ""
        parts = re.split(r",\s*|\s+et\s+|\s+ni\s+", who)
        return tuple(part.strip() for part in parts if part.strip())
    if formula.code == Code.AGAINST_THE_FLOW:
        riders = []
        for pattern, label in _AGAINST_THE_FLOW_RIDERS:
            match = pattern.search(clause)
            if match is not None:
                riders.append(label or match.group(0).lower())
        track = _BOUND_TO_A_TRACK.search(clause)
        if track is not None:
            riders.append(f"cycles {track.group(0).lower()}")
        return tuple(dict.fromkeys(riders)) or ("aux cycles",)
    if formula.code == Code.PEDESTRIAN_ACCESS:
        listed = clause.split(":", 1)[1] if ":" in clause else ""
        items = (item.strip(" .;-–") for item in re.split(r"\s*;\s*|\s*-\s+|^-|;-", listed))
        return tuple(item for item in items if item)
    return ()


def read_text(text: str | None, explained: Explained = Explained()) -> Reading:
    effects: list[Effect] = []
    unread: list[str] = []
    blocking: list[str] = []
    for clause in clauses(text):
        if any(re.search(p, clause, re.IGNORECASE) for p in BLOCKING_FORMULAS.values()):
            unread.append(clause)
            blocking.append(clause)
            continue
        if explained.spans or explained.wording:
            everything = _cut(clause, (*explained.spans, *explained.wording))
            narrower = explained.narrower_than_the_street
            if _says_nothing(cut_street_names(everything, narrower), explained.numbers):
                continue
        rest = cut_street_names(
            _cut(clause, explained.spans) if explained.spans else clause,
            explained.narrower_than_the_street,
        )
        if _says_nothing(rest, explained.numbers):
            continue
        formula = _match(rest.strip(" .;:-–,"), explained.numbers)
        if formula is None:
            unread.append(clause)
        elif formula.code != Code.NEUTRAL:
            effects.append(Effect(formula.code, _values(formula, rest.strip(" .;:-–,"))))
    return Reading(tuple(effects), tuple(unread), tuple(blocking))


def read_texts(texts: Iterable[str | None], explained: Explained = Explained()) -> Reading:
    readings = [read_text(text, explained) for text in texts]
    return Reading(
        tuple(effect for reading in readings for effect in reading.effects),
        tuple(clause for reading in readings for clause in reading.unread),
        tuple(clause for reading in readings for clause in reading.blocking),
    )
