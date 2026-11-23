from collections.abc import Sequence
from dataclasses import dataclass
from enum import IntEnum, StrEnum

import polars as pl

from toolkit.rules.rule import Nature, Outcome, Rule

from .vocabulary import LABEL_SPEED_LIMIT, SPEED_PARAMETER


class Level(StrEnum):
    ACT = "act"
    MEASURE = "measure"
    LOCATION = "location"


LEVEL_RANK = {Level.ACT: 0, Level.MEASURE: 1, Level.LOCATION: 2}
LEVEL_KEY = {Level.ACT: "a_file_id", Level.MEASURE: "m_file_id", Level.LOCATION: "l_file_id"}


class Stage(IntEnum):
    PERIMETER = 0
    PREFILTER = 1
    FIELDS = 2
    TEXTS = 3
    SIBLINGS = 4
    MODEL = 5
    LOCATIONS = 6
    RULES = 7
    COHERENCE = 8


class Status(StrEnum):
    OK = "ok"
    REFUSED = "refused"
    FLAGGED = "flagged"
    UNJUDGED = "unjudged"


STATUS = pl.Enum([status.value for status in Status])
VERDICT = pl.Struct({"status": STATUS, "detail": pl.String})


@dataclass(frozen=True)
class Check:
    code: str
    label: str
    nature: Nature
    stage: Stage
    judged_on: Level
    refuses: Level
    logged: str
    reported: bool = True

    def __post_init__(self) -> None:
        if LEVEL_RANK[self.refuses] > LEVEL_RANK[self.judged_on]:
            raise ValueError(f"{self.code} refuses below the level it judges")

    @property
    def column(self) -> str:
        return f"v_{self.code.lower()}"


def refusal(detail: pl.Expr) -> pl.Expr:
    """Refused where `detail` is not null, the detail saying why; ok elsewhere."""
    return pl.struct(
        status=pl.when(detail.is_not_null())
        .then(pl.lit(Status.REFUSED.value))
        .otherwise(pl.lit(Status.OK.value))
        .cast(STATUS),
        detail=detail.cast(pl.String),
    )


def refusals(details: Sequence[str | None]) -> pl.Expr:
    """`refusal` of details computed row by row, in the rows' order."""
    return refusal(pl.lit(pl.Series(list(details), dtype=pl.String)))


def by_key(key: str, details: dict[int, str]) -> pl.Expr:
    """Refused where the row's `key` (an act, a measure, a location) has a detail."""
    return refusal(pl.col(key).replace_strict(details, default=None, return_dtype=pl.String))


def among(alive: pl.Series, verdict: pl.Expr) -> pl.Expr:
    """`verdict` on the rows `alive` only; null (not judged) on the others, already refused when
    a check that reads the act's other records comes."""
    return pl.when(pl.lit(alive)).then(verdict)


ACT, MEASURE, LOCATION = Level.ACT, Level.MEASURE, Level.LOCATION
DEFECT, LIMIT, SCOPE = Nature.DATA_DEFECT, Nature.DIALOG_LIMIT, Nature.OUT_OF_SCOPE

NO_NUMBER = Check(
    "NO_NUMBER",
    "Arrêté sans numéro (1101)",
    DEFECT,
    Stage.PERIMETER,
    ACT,
    ACT,
    "regulations without an order number (1101): no stable identifier",
)
UNKNOWN_REGULATION_TYPE = Check(
    "UNKNOWN_REGULATION_TYPE",
    "Type d'arrêté inconnu du catalogue 1108",
    DEFECT,
    Stage.PERIMETER,
    ACT,
    ACT,
    "regulations with a type unknown to catalog 1108 (qualify it in vocabulary.py)",
)
OUT_OF_STATE = Check(
    "OUT_OF_STATE",
    "État de l'arrêté hors périmètre",
    SCOPE,
    Stage.PERIMETER,
    ACT,
    ACT,
    "regulations whose state is out of scope",
)
CITED_BY_ANOTHER_ACT = Check(
    "CITED_BY_ANOTHER_ACT",
    "Abrogé, modifié ou suspendu par un autre arrêté du périmètre",
    DEFECT,
    Stage.PREFILTER,
    ACT,
    ACT,
    "regulations another act repeals, amends or suspends",
)
SUPERSEDED_BY_LINK = Check(
    "SUPERSEDED_BY_LINK",
    "Remplacé ou abrogé selon ses liens Eudonet",
    DEFECT,
    Stage.PREFILTER,
    ACT,
    ACT,
    "regulations a link says replaced or repealed",
)
SOME_DAYS_ONLY = Check(
    "SOME_DAYS_ONLY",
    "Certains jours ou certaines heures seulement (clés, titre)",
    LIMIT,
    Stage.PREFILTER,
    ACT,
    ACT,
    "regulations their keys or their title limit to some days or hours",
)
TITLE_SAYS_REPEALED = Check(
    "TITLE_SAYS_REPEALED",
    "Arrêté en vigueur dont le titre dit abrogé",
    DEFECT,
    Stage.PREFILTER,
    ACT,
    ACT,
    "regulations in force whose title says repealed",
)
TITLE_CONDITION = Check(
    "TITLE_CONDITION",
    "Titre qui pose une condition (pollution, neige, crue, « en cas de »…)",
    LIMIT,
    Stage.PREFILTER,
    ACT,
    ACT,
    "regulations whose title makes them hold under a condition no field carries",
)
PUBLISHED_ELSEWHERE = Check(
    "PUBLISHED_ELSEWHERE",
    "Déjà publié par un autre canal",
    SCOPE,
    Stage.PREFILTER,
    ACT,
    ACT,
    "regulations already published by another channel",
)
NO_MEASURE = Check(
    "NO_MEASURE",
    "Sans mesure lue",
    DEFECT,
    Stage.PREFILTER,
    MEASURE,
    MEASURE,
    "rows without any measure",
)
UNKNOWN_MEASURE_LABEL = Check(
    "UNKNOWN_MEASURE_LABEL",
    "Libellé de mesure inconnu du catalogue 1202, et non compris comme texte libre",
    DEFECT,
    Stage.PREFILTER,
    MEASURE,
    ACT,
    "regulations holding a measure whose label, unknown to catalog 1202, reads as a free text "
    "not understood (qualify it in vocabulary.MEASURE_TYPE_BY_LABEL)",
)
UNEXPECTED_OPENING = Check(
    "UNEXPECTED_OPENING",
    "Formule d'ouverture inattendue (1203)",
    LIMIT,
    Stage.FIELDS,
    MEASURE,
    MEASURE,
    "measures whose article opens on a formula their name does not (qualify it in "
    "vocabulary.OPENING_BY_LABEL)",
)
UNKNOWN_OPTION_FORMULA = Check(
    "UNKNOWN_OPTION_FORMULA",
    "Formule d'impression inconnue (1236, 1204, 1207)",
    LIMIT,
    Stage.FIELDS,
    MEASURE,
    MEASURE,
    "measures whose mail merge prints an unknown formula (qualify it in "
    "vocabulary.KNOWN_OPTION_FORMULAS)",
)
SPEED_UNREADABLE = Check(
    "SPEED_UNREADABLE",
    "Limitation de vitesse sans vitesse lisible",
    DEFECT,
    Stage.FIELDS,
    MEASURE,
    MEASURE,
    f"'{LABEL_SPEED_LIMIT}' measures without a readable '{SPEED_PARAMETER}' parameter: there "
    "is no default speed",
)
TIME_SLOT = Check(
    "TIME_SLOT",
    "Jours et horaires remplis",
    LIMIT,
    Stage.FIELDS,
    MEASURE,
    MEASURE,
    "measures whose 'Jours et Horaires' text DiaLog cannot express",
)
LANE_PARAMETER = Check(
    "LANE_PARAMETER",
    "Voie concernée remplie",
    LIMIT,
    Stage.FIELDS,
    MEASURE,
    MEASURE,
    "measures narrowed to one lane by 'voie concernée', which DiaLog would apply to the whole "
    "street",
)
UNKNOWN_PARAMETER = Check(
    "UNKNOWN_PARAMETER",
    "Paramètre rempli de nom inconnu",
    DEFECT,
    Stage.FIELDS,
    MEASURE,
    MEASURE,
    "measures carrying a filled parameter of an unknown name, which may narrow them",
)
UNREAD_ACT_TEXT = Check(
    "UNREAD_ACT_TEXT",
    "Texte de l'arrêté non compris (ou d'une de ses mesures non publiées)",
    LIMIT,
    Stage.PREFILTER,
    ACT,
    ACT,
    "regulations whose free text says more than the published fields",
)
UNREAD_MEASURE_TEXT = Check(
    "UNREAD_MEASURE_TEXT",
    "Texte de la mesure non compris",
    LIMIT,
    Stage.TEXTS,
    MEASURE,
    MEASURE,
    "measures whose free text says more than the published fields",
)
UNREAD_LOCATION_TEXT = Check(
    "UNREAD_LOCATION_TEXT",
    "Texte de la localisation non compris",
    LIMIT,
    Stage.TEXTS,
    LOCATION,
    LOCATION,
    "locations whose free text says more than the published fields",
)
UNREAD_TITLE = Check(
    "UNREAD_TITLE",
    "Titre non compris, ou que les champs ne tiennent pas",
    LIMIT,
    Stage.TEXTS,
    ACT,
    ACT,
    "regulations whose title says more than their fields hold",
)
ONE_WAY_LANE = Check(
    "ONE_WAY_LANE",
    "Sens unique qu'une autre mesure de l'arrêté ouvre peut-être dans l'autre sens",
    LIMIT,
    Stage.SIBLINGS,
    LOCATION,
    LOCATION,
    "one-way street locations another measure of the act may open the other way",
)
OUT_OF_MODEL = Check(
    "OUT_OF_MODEL",
    "Mesure sans équivalent DiaLog",
    LIMIT,
    Stage.MODEL,
    MEASURE,
    MEASURE,
    "measures without a DiaLog equivalent",
)
VEHICLE_REJECTION = Check(
    "VEHICLE_REJECTION",
    "Véhicule hors de la liste fermée, ou dérogations trop longues pour l'API",
    LIMIT,
    Stage.MODEL,
    MEASURE,
    MEASURE,
    "measures whose vehicles DiaLog cannot carry",
)
IMPLAUSIBLE_DATES = Check(
    "IMPLAUSIBLE_DATES",
    "Dates incohérentes (début après la fin, année aberrante)",
    DEFECT,
    Stage.MODEL,
    ACT,
    ACT,
    "regulations failing the date controls",
)
BOUND_QUALIFIER = Check(
    "BOUND_QUALIFIER",
    "Borne qui dit plus que l'endroit (côté, sens, complément de numéro)",
    LIMIT,
    Stage.LOCATIONS,
    LOCATION,
    LOCATION,
    "locations whose bound qualifies the measure",
)
LOCATION_DETAIL = Check(
    "LOCATION_DETAIL",
    "Longueur sur une section, ou places sur une rue entière",
    LIMIT,
    Stage.LOCATIONS,
    LOCATION,
    LOCATION,
    "locations whose details narrow the measure",
)
LOCATION_REJECTED = Check(
    "LOCATION_REJECTED",
    "Localisation que DiaLog ne peut pas prendre (rue, borne, arrondissement)",
    LIMIT,
    Stage.LOCATIONS,
    LOCATION,
    LOCATION,
    "locations DiaLog cannot take",
)
ONE_WAY_UNREADABLE = Check(
    "ONE_WAY_UNREADABLE",
    "Sens unique dont les bornes ne disent pas le sens",
    LIMIT,
    Stage.LOCATIONS,
    LOCATION,
    LOCATION,
    "one-way street locations whose bounds do not say which way (no « depuis … vers »)",
)
POINT_BEYOND_PARKING = Check(
    "POINT_BEYOND_PARKING",
    "Point seul sur une autre mesure qu'un stationnement interdit",
    LIMIT,
    Stage.LOCATIONS,
    LOCATION,
    LOCATION,
    "single-point locations of measures other than a parking ban",
)
REFUSED_LOCATION = Check(
    "REFUSED_LOCATION",
    "Adresse que DiaLog refuse de géocoder (liste)",
    LIMIT,
    Stage.LOCATIONS,
    LOCATION,
    LOCATION,
    "locations DiaLog refuses to geocode (blacklist)",
)
COPIED_LOCATION = Check(
    "COPIED_LOCATION",
    "Tronçon recopié d'un ancien arrêté, rue absente du titre",
    DEFECT,
    Stage.COHERENCE,
    LOCATION,
    MEASURE,
    "measures of temporary acts holding a location copied from an older act, its street "
    "absent from the title",
)

CHECKS: tuple[Check, ...] = (
    NO_NUMBER,
    UNKNOWN_REGULATION_TYPE,
    OUT_OF_STATE,
    CITED_BY_ANOTHER_ACT,
    SUPERSEDED_BY_LINK,
    SOME_DAYS_ONLY,
    TITLE_SAYS_REPEALED,
    TITLE_CONDITION,
    PUBLISHED_ELSEWHERE,
    NO_MEASURE,
    UNKNOWN_MEASURE_LABEL,
    UNEXPECTED_OPENING,
    UNKNOWN_OPTION_FORMULA,
    SPEED_UNREADABLE,
    TIME_SLOT,
    LANE_PARAMETER,
    UNKNOWN_PARAMETER,
    UNREAD_ACT_TEXT,
    UNREAD_MEASURE_TEXT,
    UNREAD_LOCATION_TEXT,
    UNREAD_TITLE,
    ONE_WAY_LANE,
    OUT_OF_MODEL,
    VEHICLE_REJECTION,
    IMPLAUSIBLE_DATES,
    BOUND_QUALIFIER,
    LOCATION_DETAIL,
    LOCATION_REJECTED,
    ONE_WAY_UNREADABLE,
    POINT_BEYOND_PARKING,
    REFUSED_LOCATION,
    COPIED_LOCATION,
)
REGISTRY: dict[str, Check] = {check.column: check for check in CHECKS}
RULE_PREFIX = "v_rule_"
PARAMS = "x_params"


def rule_check(rule: Rule) -> Check:
    """The check of a rule, judged on the row (a location) and refusing it alone."""
    return Check(
        code=f"RULE_{rule.code}",
        label=rule.label,
        nature=rule.nature,
        stage=Stage.RULES,
        judged_on=Level.LOCATION,
        refuses=Level.LOCATION,
        logged=f"rows failing the rule {rule.code}",
        reported=False,
    )


def check_of(column: str) -> Check | None:
    """The check a verdict column holds: one of `CHECKS`, or a rule's (`v_rule_<code>`)."""
    if column in REGISTRY:
        return REGISTRY[column]
    if column.startswith(RULE_PREFIX):
        code = column.removeprefix(RULE_PREFIX).upper()
        return Check(
            f"RULE_{code}",
            code,
            Nature.SUSPICIOUS,
            Stage.RULES,
            LOCATION,
            LOCATION,
            f"rows failing the rule {code}",
            reported=False,
        )
    return None


def rule_status(rule: Rule) -> Status:
    return Status.REFUSED if rule.outcome is Outcome.DROP else Status.FLAGGED
