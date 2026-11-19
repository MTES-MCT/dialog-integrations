"""The act's title, read like its other free texts, then held against the act's fields.

The title is cut into clauses. From each, the title formulas are taken out, each noting what it
says (a family of measures, a speed, a weight, a provisional act, bounds); then what the act's
fields print (its locations, its measures) and the streets of the referential. Anything left
says more than the fields: the act is dropped. What the formulas noted must then hold on the
act: a value one of its measures has, a temporary act, bounds one of its locations has unless
they all are narrower than the street anyway (decided 2026-11-19). The families are noted, not
held: an act narrower than its title, or naming parking for a closure, publishes nothing false.
A title that amends another act describes that act: the citation rules deal with it, the rest
of that clause says nothing of this act.

The days, hours, markets and conditions of a title and "abrogé" are checked before it, by
their own checks; an exemption written in the title alone, after the vehicles are read.
"""

import re
from collections import defaultdict
from dataclasses import dataclass, field
from enum import StrEnum

import polars as pl

from toolkit.transforms.pedestrian_area import PEDESTRIAN_AREA_SPEED

from .free_text import Explained, clauses, cut, cut_street_names, says_nothing
from .locations import street_key
from .printed import bound_numbers, location_spans, narrower_than_the_street, parameter_spans
from .text import plain_text
from .verdicts import UNREAD_TITLE, among, by_key
from .vocabulary import (
    IMPLIED_MAX_SPEED_BY_LABEL,
    LABEL_PEDESTRIAN_AREA,
    PEDESTRIAN_AREA_SPEED_BY_KEY,
    SPEED_PARAMETER,
    TYPE_TEMPORARY_LABEL,
)


class Says(StrEnum):
    NEUTRAL = "neutral"
    FAMILY = "family"
    SPEED = "speed"
    WEIGHT = "weight"
    PROVISIONAL = "provisional"
    BOUNDS = "bounds"


@dataclass(frozen=True)
class TitleFormula:
    name: str
    pattern: re.Pattern[str]
    says: Says = Says.NEUTRAL
    families: tuple[str, ...] = ()


def _formula(
    name: str, pattern: str, says: Says = Says.NEUTRAL, families: tuple[str, ...] = ()
) -> TitleFormula:
    return TitleFormula(name, re.compile(pattern, re.IGNORECASE), says, families)


_ART = r"(?:la |les |le |l['’]|des |du |de la |de l['’]|de |d['’]|une |un )?"
_VERB = (
    r"(?:modifiant|complétant|réglementant|règlementant|instituant|instaurant|portant|créant"
    r"|interdisant|limitant|fixant|relatif (?:à|aux)|relative (?:à|aux)|concernant|désignant"
    r"|réservant|autorisant|supprimant|rétablissant|neutralisant)"
)

# Longest and most specific first: a formula is taken out of the clause before the next is tried.
TITLE_FORMULAS: tuple[TitleFormula, ...] = (
    # The prefecture's working prefixes ("IR_", "MD_ ") and the "gênant" designation act.
    _formula("working prefix", r"^\s*(?:IR|MD|DR|SN|AM|GL|AR)\s*(?:[_;:]\s*|$)"),
    _formula(
        "parking said to hinder traffic",
        r"(?:désignant (?:à paris )?les voies )?(?:où|dans lesquelles) l['’]arrêt (?:et|ou) le "
        r"stationnement (?:des véhicules )?(?:sont|est) (?:interdits? et )?considérés? comme "
        r"gênants?(?: (?:pour )?la circulation publique)?",
        Says.FAMILY,
        ("parking",),
    ),
    _formula(
        "hinders traffic",
        r"(?:la règle du |les règles du )?(?:stationnement )?(?:considéré )?(?:comme )?gênants? "
        r"(?:pour )?la circulation (?:publique|générale)",
        Says.FAMILY,
        ("parking",),
    ),
    _formula("provisional", r",?\s*(?:à titre provisoire|provisoirement)\s*,?", Says.PROVISIONAL),
    _formula(
        "weight",
        r"(?:dont le )?poids total (?:autorisé )?(?:en charge )?(?:excède|est supérieur à|"
        r"supérieur à|de plus de)\s*\d+(?:[,.]\d+)?\s*(?:tonnes|t)\b",
        Says.WEIGHT,
    ),
    _formula("speed", r"(?:à )?\d{1,3}\s?km\s?/?\s?h(?:eure)?\b", Says.SPEED),
    _formula(
        "bounds",
        r"(?:dans sa partie |dans la partie |dans une section )?(?:comprise )?entre "
        r"(?P<from>.+?) et (?P<to>.+?)(?=,|\.| à paris\b| dans le\b| \(|$)",
        Says.BOUNDS,
    ),
    _formula("replaces a zone", r"en remplacement (?:d['’]une|de la) (?:zone 30 )?existante"),
    _formula("zone 30", rf"{_ART}zones? 30", Says.FAMILY, ("zone 30", "speed")),
    _formula(
        "pedestrian area", rf"{_ART}(?:deux )?aires? piétonnes?", Says.FAMILY, ("pedestrian area",)
    ),
    _formula("shared zone", rf"{_ART}zones? de rencontre", Says.FAMILY, ("shared zone",)),
    _formula(
        "one way",
        rf"{_ART}sens uniques?(?: de circulation)?(?: générale)?"
        rf"|{_ART}sens de (?:la )?circulation(?: générale)?",
        Says.FAMILY,
        ("one way",),
    ),
    _formula(
        "speed limit",
        rf"{_ART}vitesse(?: (?:maximale )?(?:de circulation|autorisée))?"
        rf"(?: (?:générale|des véhicules))?",
        Says.FAMILY,
        ("speed",),
    ),
    _formula(
        "traffic and parking",
        rf"{_ART}(?:règles? |conditions )?(?:de )?(?:la )?circulation(?: générale)?"
        rf" et (?:du |de |le |de l['’]arrêt et du )?(?:la )?(?:règle du |règles du )?stationnement"
        rf"(?: des véhicules)?",
        Says.FAMILY,
        ("traffic", "parking"),
    ),
    _formula(
        "parking and traffic",
        rf"{_ART}(?:règles? |conditions )?(?:du |de |d['’])?(?:la )?stationnement(?: gênant)?"
        rf"(?: des véhicules)? et de (?:la )?circulation(?: générale)?(?: et des cycles)?",
        Says.FAMILY,
        ("parking", "traffic"),
    ),
    _formula(
        "parking",
        rf"{_ART}(?:règles? |conditions )?(?:du |de |d['’])?(?:l['’]arrêt et (?:du |le ))?"
        rf"(?:la )?stationnement(?: (?:interdits? et )?(?:considérés? comme )?gênants?)?"
        rf"(?: des véhicules)?",
        Says.FAMILY,
        ("parking",),
    ),
    _formula(
        "no driving nor parking",
        r"interdiction de circuler et de stationner",
        Says.FAMILY,
        ("traffic", "parking"),
    ),
    _formula("stopping", rf"{_ART}arrêt", Says.FAMILY, ("parking",)),
    _formula(
        "traffic",
        rf"{_ART}(?:règles? |conditions )?(?:de )?(?:la )?circulation(?: générale)?"
        rf"(?: des véhicules| de certains véhicules| des cycles| des poids lourds)?"
        rf"(?: et des cycles)?",
        Says.FAMILY,
        ("traffic",),
    ),
    _formula(
        "verb",
        rf"(?:^\s*|\bet\s+){_VERB}(?: interdiction| création| suppression)?"
        rf"(?: d['’]une?| de| des)?\b",
    ),
    _formula("in paris", r"\bà paris(?: centre)?\b|\bparis(?: centre)?\b"),
    _formula(
        "district",
        r"(?:dans |les |le |du )?\d{1,2}\s?(?:e|è|ème|eme|er)\b"
        r"(?: et \d{1,2}\s?(?:e|è|ème|eme|er)\b)*"
        r"(?: arrondissements?)?|arrondissements?",
    ),
    _formula("purpose", r",?\s*afin d['’].*$|,?\s*pour (?:assurer|permettre|garantir) .*$"),
    _formula("at all times", r"\ben tout temps\b"),
    _formula(
        "where",
        r"dans (?:plusieurs|diverses|certaines|deux|trois|quatre|cinq|une|les|des) voies?"
        r"(?: parisiennes)?(?: du| de)?"
        r"|dans le périmètre(?: du quartier)?|aux abords (?:des|de l['’]|du|de la) établissements? "
        r"scolaires?|dans (?:un|une) (?:tronçon|section|portion)(?: de| du| de la)?"
        r"|voies? non dénommées?"
        r"|devant (?:l['’]|un )établissement(?: scolaire)?(?: situé au n° ?\d+)?"
        r"|sur (?:certains tronçons|certaines portions|un tronçon|une portion)"
        r" (?:de la|du|de l['’]|des|de)",
    ),
    _formula(
        "quoted name", r"(?:quartiers?|dénommée?s?|zones? 30)?\s*(?:de |du )?[«\"“][^»\"”]*[»\"”]"
    ),
    _formula("in", r"\bdans(?: la| le| les| l['’])?(?=\s|$)"),
)

# A title clause that amends or completes another act describes that act.
_AMENDS_ANOTHER_ACT = re.compile(
    r"^(?:\w{2}_\s*)?(?:modifiant|complétant)(?: et (?:modifiant|complétant))?"
    r",?(?: dans le \d+\w* arrond\w*)?,?(?: les dispositions de)? (?:l['’]|les )arrêtés? "
    r"(?:préfectora\w+ |municipa\w+ )?(?:n° ?)?[\w./-]*\d",
    re.IGNORECASE,
)
_NUMBER = re.compile(r"\d+(?:[,.]\d+)?")
# Once the act's streets are cut, a type of road left alone ("rues X et Y" with X and Y
# explained) says nothing; an unknown name next to it still does.
_ROAD_TYPE = re.compile(
    r"\b(?:rues?|avenues?|boulevards?|places?|quais?|passages?|impasses?|villas?|cités?|allées?"
    r"|squares?|voies?|cours|chemins?|routes?|sentiers?|ponts?|portes?|carrefours?|hameaux?"
    r"|parvis|promenades?|esplanades?|jardins?|terrasses?)\b",
    re.IGNORECASE,
)
# "Rue de la Jonquière" is also "de la Jonquière" or "Jonquière" in a list of streets.
_LEADING_ROAD_TYPE = re.compile(rf"^{_ROAD_TYPE.pattern}\s+", re.IGNORECASE)
_LEADING_ARTICLE = re.compile(r"^(?:de la |de l['’]|des |du |de |d['’])", re.IGNORECASE)


def street_variants(name: str) -> list[str]:
    short = _LEADING_ROAD_TYPE.sub("", name).strip()
    if not short or short == name:
        return []
    return [short, _LEADING_ARTICLE.sub("", short).strip()]


@dataclass(frozen=True)
class TitleReading:
    families: frozenset[str] = frozenset()
    speeds: frozenset[int] = frozenset()
    weights: frozenset[float] = frozenset()
    provisional: bool = False
    bounds: tuple[tuple[str, str], ...] = ()
    unread: tuple[str, ...] = ()


def read_title(title: str | None, explained: Explained) -> TitleReading:
    """What the title says, formula by formula, and the clauses nothing explains."""
    families: set[str] = set()
    speeds: set[int] = set()
    weights: set[float] = set()
    bounds: list[tuple[str, str]] = []
    provisional = False
    unread: list[str] = []
    for clause in clauses(plain_text(title)):
        if _AMENDS_ANOTHER_ACT.search(clause):
            continue
        rest = clause
        for formula in TITLE_FORMULAS:
            for match in list(formula.pattern.finditer(rest)):
                if formula.says == Says.FAMILY:
                    families.update(formula.families)
                elif formula.says == Says.SPEED:
                    speeds.update(int(n) for n in _NUMBER.findall(match.group()))
                elif formula.says == Says.WEIGHT:
                    weights.update(
                        float(n.replace(",", ".")) for n in _NUMBER.findall(match.group())
                    )
                elif formula.says == Says.PROVISIONAL:
                    provisional = True
                elif formula.says == Says.BOUNDS:
                    bounds.append((match.group("from"), match.group("to")))
            rest = formula.pattern.sub(" ", rest)
        rest = cut_street_names(
            cut(rest, explained.spans) if explained.spans else rest,
            explained.narrower_than_the_street,
        )
        rest = _ROAD_TYPE.sub(" ", rest)
        if not says_nothing(rest, explained.numbers):
            unread.append(clause)
    return TitleReading(
        frozenset(families),
        frozenset(speeds),
        frozenset(weights),
        provisional,
        tuple(bounds),
        tuple(unread),
    )


@dataclass
class Act:
    """What the title is held against: every measure and location the act has, read or not."""

    temporary: bool
    narrower_than_the_street: bool = False
    speeds: set[int] = field(default_factory=set)
    weights: set[float] = field(default_factory=set)
    bound_keys: set[str] = field(default_factory=set)
    numbers: set[str] = field(default_factory=set)


_ARTICLE_WORDS = frozenset({"LA", "LE", "LES", "L"})
_ROAD_TYPE_WORDS = frozenset(
    {
        *("RUE", "AVENUE", "BOULEVARD", "PLACE", "QUAI", "PASSAGE", "IMPASSE", "VILLA", "CITE"),
        *("ALLEE", "SQUARE", "VOIE", "COURS", "CHEMIN", "ROUTE", "SENTIER", "PONT", "PORTE"),
        *("CARREFOUR", "HAMEAU", "PARVIS", "PROMENADE", "ESPLANADE", "JARDIN", "TERRASSE"),
    }
)
_LINK_WORDS = frozenset({"DE", "DES", "DU", "D", "LA", "L"})


def bound_key(name: str) -> str:
    """A street as a bound is written: "les rues Torricelli", "la rue du Faubourg Saint-Honoré",
    "RUE D AUTEUIL": compared without article, type of road or "de"."""
    words = street_key(name).split()
    while words and words[0] in _ARTICLE_WORDS:
        words.pop(0)
    if words and (words[0] in _ROAD_TYPE_WORDS or words[0].rstrip("SX") in _ROAD_TYPE_WORDS):
        words.pop(0)
    while len(words) > 1 and words[0] in _LINK_WORDS:
        words.pop(0)
    return " ".join(words)


def _bound_matches(text: str, act: Act) -> bool:
    """A bound the title names is one of the act's bounds: a street, or a house number."""
    if bound_key(text) in act.bound_keys:
        return True
    numbers = _NUMBER.findall(text)
    return bool(numbers) and all(n in act.numbers for n in numbers)


def disagreement(reading: TitleReading, act: Act) -> str | None:
    """Why the act's fields do not hold what its title says, or None."""
    if reading.unread:
        return "title not understood"
    if reading.provisional and not act.temporary:
        return "title says provisional, act is permanent"
    if reading.speeds - act.speeds:
        return "title speed not in the act"
    if reading.weights - act.weights:
        return "title weight not in the act"
    for start, end in reading.bounds:
        matched = _bound_matches(start, act) and _bound_matches(end, act)
        if not (matched or act.narrower_than_the_street):
            return "title bounds not in the act's locations"
    return None


_WEIGHT_IN_PARAMETER = re.compile(r"(\d+(?:[,.]\d+)?)\s*(?:t\b|tonnes)", re.IGNORECASE)
_BOUND_COLUMNS = (
    "l_road_name",
    "l_from_road_name",
    "l_to_road_name",
    "l_from_address_label",
    "l_to_address_label",
    "l_point_address_label",
)


def _acts(rows: pl.DataFrame) -> dict[int, tuple[str | None, Act, Explained]]:
    """Each act's title, fields and what they print, from every row it has, read or dropped."""
    acts: dict[int, tuple[str | None, Act, Explained]] = {}
    spans: dict[int, list[str]] = defaultdict(list)
    narrower: dict[int, list[bool]] = defaultdict(list)
    for row in rows.iter_rows(named=True):
        regulation = row["a_file_id"]
        if regulation not in acts:
            acts[regulation] = (
                row["a_title_html"],
                Act(temporary=row["a_type"] == TYPE_TEMPORARY_LABEL),
                Explained(),
            )
        act = acts[regulation][1]
        if row["m_file_id"] is not None:
            name = row["m_type"] or ""
            if name in IMPLIED_MAX_SPEED_BY_LABEL:
                act.speeds.add(IMPLIED_MAX_SPEED_BY_LABEL[name])
            if name == LABEL_PEDESTRIAN_AREA:
                keyed = [
                    PEDESTRIAN_AREA_SPEED_BY_KEY.get(row[key] or "")
                    for key in ("a_key_1", "a_key_2")
                ]
                act.speeds.add(next((speed for speed in keyed if speed), PEDESTRIAN_AREA_SPEED))
            for pair in row["m_params"] or []:
                if len(pair) != 2:
                    continue
                if pair[0].strip() == SPEED_PARAMETER:
                    act.speeds.update(int(n) for n in re.findall(r"\d+", pair[1]))
                act.weights.update(
                    float(n.replace(",", ".")) for n in _WEIGHT_IN_PARAMETER.findall(pair[1])
                )
            spans[regulation] += [name, row["m_opening"] or "", *parameter_spans(row)]
        if row["l_file_id"] is not None:
            narrower[regulation].append(narrower_than_the_street(row))
            act.bound_keys.update(bound_key(row[c]) for c in _BOUND_COLUMNS if row[c])
            act.numbers.update(bound_numbers(row))
            spans[regulation] += location_spans(row)
            spans[regulation] += [
                variant
                for column in ("l_road_name", "l_from_road_name", "l_to_road_name")
                for variant in street_variants(row[column] or "")
            ]
    for regulation, (_, act, _) in acts.items():
        act.narrower_than_the_street = bool(narrower[regulation]) and all(narrower[regulation])
    return {
        regulation: (
            title,
            act,
            Explained(
                tuple(span for span in spans[regulation] if span.strip()),
                frozenset(act.numbers),
                narrower_than_the_street=act.narrower_than_the_street,
            ),
        )
        for regulation, (title, act, _) in acts.items()
    }


def judge_unread_titles(rows: pl.DataFrame, alive: pl.Series) -> pl.DataFrame:
    """Refuse the acts still alive whose title says more than their fields, or what their
    fields do not hold. The title is held against every measure and location of the act, those
    refused already included (a parking act whose only measure went is still a parking act)."""
    present = rows.filter(alive)["a_file_id"].unique().to_list()
    reasons: dict[int, str] = {}
    for regulation, (title, act, explained) in _acts(
        rows.filter(pl.col("a_file_id").is_in(present))
    ).items():
        if why := disagreement(read_title(title, explained), act):
            reasons[regulation] = why
    return rows.with_columns(among(alive, by_key("a_file_id", reasons)).alias(UNREAD_TITLE.column))
