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
    EXEMPTION = "exemption"
    AGAINST_THE_FLOW = "against the flow"
    PEDESTRIAN_ACCESS = "pedestrian access"
    ZONE_NAME = "zone name"
    # Values: the dimension alone ("longueur"), the one the gauge value of a dimension limit
    # bounds: the catalog value says "plus de 10 mètres", never of what.
    DIMENSION = "dimension"
    RESERVED_PLACES = "reserved places"
    AMENDMENT_WORDING = "amendment wording"


@dataclass(frozen=True)
class Effect:
    code: Code
    values: tuple[str, ...] = ()


@dataclass(frozen=True)
class Reading:
    effects: tuple[Effect, ...] = ()
    unread: tuple[str, ...] = ()
    blocking: tuple[str, ...] = ()

    @property
    def understood(self) -> bool:
        return not self.unread

    def codes(self) -> set[Code]:
        return {effect.code for effect in self.effects}


@dataclass(frozen=True)
class Explained:
    spans: tuple[str, ...] = ()
    numbers: frozenset[str] = field(default_factory=frozenset)
    wording: tuple[str, ...] = ()
    narrower_than_the_street: bool = False


@dataclass(frozen=True)
class Formula:
    name: str
    pattern: re.Pattern[str]
    code: Code = Code.NEUTRAL
    names_times: bool = False
    whole: bool = False


_TIME_OR_DAY = re.compile(
    r"\b\d{1,2}\s?h\s?\d{0,2}\b|\bheures?\b|\b(lundi|mardi|mercredi|jeudi|vendredi|samedi|"
    r"dimanche|week-end|fériés?|nuit|marchés?)s?\b",
    re.IGNORECASE,
)

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

LINKING_WORDS = frozenset(
    {
        *("le", "la", "les", "l", "un", "une", "de", "du", "des", "d", "a", "au", "aux"),
        *("et", "ni", "en", "sur", "entre", "n", "no", "jusqu", "bis", "ter", "quater"),
    }
)
_NUMBER_TOKEN = re.compile(r"(?:n|no)?0*(\d+)(?:bis|ter|quater|[a-gi-z])?")

REPEAL_SENTENCE = (
    r"^(les dispositions (de l'|des )|l'|les )?(arrêtés?|articles?|dispositions)\b.*\b(est|sont) "
    r"abrogée?s?\b|^(sont|est) (également |partiellement )?abrogée?s?\b"
    r"|\bénumérés ci-après sont abrogés"
)
AMENDS_ANOTHER_ACT = (
    r"^(?!.*présent arrêté).*\b(de l'|l'|l’|de l’)arrêté (préfectoral |municipal )?"
    r"(n° ?[\w./-]+|du \d)"
    r".*\b(modifiée?s?|complétée?s?|remplacée?s?|ajoutée?s?|deviennent|inchangée?s?)\b"
)
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
# A rider without an exemption type of the API goes as `other` with its words (2026-11-19).
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


# Closed lists: what DiaLog does not publish is neutral; a time, a day or a vehicle never is.
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
    _formula("names the dimension", r"^(longueur|largeur|hauteur)$", Code.DIMENSION),
    _formula(
        "zone name",
        r"^(?!.*\b(sauf|pas|exclu\w*|inclus\w*|autoris\w*|interdit\w*)\b)"
        r"(zone 30 |quartier vert |qt vert )?[\"“« ]*[^\d.;:,]{2,40}[\"”» ]*$",
        Code.ZONE_NAME,
    ),
)

BLOCKING_FORMULAS: dict[str, str] = {}


def _fold(text: str) -> str:
    folded = []
    for char in text.lower():
        base = "".join(
            c for c in unicodedata.normalize("NFKD", char) if not unicodedata.combining(c)
        )
        folded.append(base if len(base) == 1 else char)
    return "".join(folded)


def _tokens(text: str) -> list[tuple[str, int, int]]:
    return [(m.group(), m.start(), m.end()) for m in re.finditer(r"[a-z0-9]+", _fold(text))]


_HTML_LINE_END = re.compile(r"<br\s*/?>|</p>|</div>|</li>", re.IGNORECASE)
_LINE_MARK = "\x00"


_LIST_ITEM = re.compile(r"^([-–•·«\"“]|l['’]arrêté|les arrêtés|n\s?°|\d)", re.IGNORECASE)
_GOES_ON = re.compile(r"^[a-zàâçéèêëîïôûù(,]")


def _lines(text: str) -> list[str]:
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


def cut(clause: str, explained_spans: Iterable[str]) -> str:
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


# A street named in a free text says nothing: the locations carry it (decided 2026-11-19).
_CROSSING_WORDS = frozenset({"entre", "croisement", "intersection", "angle"})
_BEFORE_A_NAME = frozenset({"de", "du", "des", "d", "la", "le", "l", "les", "avec", "et"})


@cache
def _street_names_by_head() -> dict[tuple[str, str], list[tuple[str, ...]]]:
    index: dict[tuple[str, str], list[tuple[str, ...]]] = defaultdict(list)
    for name in json.loads(STREET_NAMES_FILE.read_text(encoding="utf-8")):
        words = tuple(word for word, _, _ in _tokens(name))
        if len(words) >= 2:
            index[words[:2]].append(words)
    for names in index.values():
        names.sort(key=len, reverse=True)
    return dict(index)


def _crossing_word(words: list[str], named: list[bool], start: int) -> int | None:
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


def says_nothing(clause: str, numbers: frozenset[str]) -> bool:
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
        if formula.whole and not says_nothing(
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
    if formula.code == Code.DIMENSION:
        return (clause.lower(),)
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
            everything = cut(clause, (*explained.spans, *explained.wording))
            narrower = explained.narrower_than_the_street
            if says_nothing(cut_street_names(everything, narrower), explained.numbers):
                continue
        rest = cut_street_names(
            cut(clause, explained.spans) if explained.spans else clause,
            explained.narrower_than_the_street,
        )
        if says_nothing(rest, explained.numbers):
            continue
        formula = _match(rest.strip(" .;:-–,"), explained.numbers)
        if formula is None:
            unread.append(clause)
        elif formula.code != Code.NEUTRAL:
            effects.append(Effect(formula.code, _values(formula, rest.strip(" .;:-–,"))))
    return Reading(tuple(effects), tuple(unread), tuple(blocking))
