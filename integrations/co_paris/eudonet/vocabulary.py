"""Closed lookup tables between Eudonet catalog labels and the DiaLog model.

Read from `GET /Catalog/{descId}` on 2026-09-08 and cross-checked against the 26 325
measures extracted that day.
"""

import re
from dataclasses import dataclass

from api.dia_log_client.models import (
    MeasureTypeEnum,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodySubject,
)

# `1108` Type arrêté: the catalog has exactly these two values.
TYPE_PERMANENT_LABEL = "Permanent"
TYPE_TEMPORARY_LABEL = "Temporaire"

REGULATION_CATEGORY_BY_LABEL: dict[str, str] = {
    TYPE_PERMANENT_LABEL: PostApiRegulationsAddBodyCategory.PERMANENTREGULATION.value,
    TYPE_TEMPORARY_LABEL: PostApiRegulationsAddBodyCategory.TEMPORARYREGULATION.value,
}

# Catalog `DBValue`s, as `GET /Catalog/{descId}` served them on 2026-09-08: the query asks
# for the codes, the replay of a frozen extract filters on the labels.
TYPE_CODE_BY_LABEL: dict[str, str] = {TYPE_PERMANENT_LABEL: "7", TYPE_TEMPORARY_LABEL: "8"}

# `1107` État (label → `DBValue`), recomputed by Eudonet nightly. A signed permanent may
# carry no date of effect, so it is not known to apply yet.
ACCEPTED_STATES_BY_TYPE: dict[str, dict[str, str]] = {
    TYPE_PERMANENT_LABEL: {"En vigueur": "96"},
    TYPE_TEMPORARY_LABEL: {"En vigueur": "8640", "Publié": "13789", "Signé": "8642"},
}

# Key `1103` of 22 acts of 2005-2008 (2026-11-17): their pedestrian areas are limited to
# 15 km/h, not walking pace (decided 2026-11-19).
PEDESTRIAN_AREA_SPEED_BY_KEY: dict[str, int] = {
    "STATIONNEMENT GENANT- LIMITATION VITESSE 15km/h": 15,
}

# All 32 values of catalog `1202`. `None`: outside the five DiaLog types, dropped and
# counted, never bent into an approaching type.
MEASURE_TYPE_BY_LABEL: dict[str, str | None] = {
    # The producer names the area, so it is published as `noEntry` + `desserteLocale`
    # plus a speed twin, 5 km/h unless a key says otherwise (`add_pedestrian_area_speed`).
    "aire piétonne": MeasureTypeEnum.NOENTRY.value,
    "cédez le passage": None,
    "circulation alternée": MeasureTypeEnum.ALTERNATEROAD.value,
    "circulation interdite": MeasureTypeEnum.NOENTRY.value,
    "feux d'intersection": None,
    "Feux piéton": None,
    "Gratuité de stationnement": None,
    "interdiction d'arrêt": MeasureTypeEnum.PARKINGPROHIBITED.value,
    "interdiction de dépasser": MeasureTypeEnum.NOOVERTAKING.value,
    "interdiction de stationnement": MeasureTypeEnum.PARKINGPROHIBITED.value,
    "interdiction de tourner": None,
    "interdiction double sens pour les cyclistes": None,
    # A gauge or a category limit is not a sixth type: it is `noEntry` restricted to the
    # vehicles the parameters describe.
    "limitation catégorielle": MeasureTypeEnum.NOENTRY.value,
    "limitation de vitesse": MeasureTypeEnum.SPEEDLIMITATION.value,
    "limitation dimensionnelle": MeasureTypeEnum.NOENTRY.value,
    "Mesure libre": None,
    # A dead end closes one end of the street, not the street: as `noEntry` both ways it
    # would read as a full closure (decision of 2026-09-23).
    "mise en impasse": None,
    "obligation d'allumage des feux": None,
    "obligation de mouvement": None,
    "périmètre zone": None,
    "rétablissement double sens": None,
    # « Un sens unique de circulation est institué … depuis A vers B » : no entry from B to A,
    # published only where the bounds say which way (`orient_one_way_streets`).
    "sens interdit (ou sens unique)": MeasureTypeEnum.NOENTRY.value,
    "stationnement réservé": None,
    "stop": None,
    "suppression stationnement réservé": None,
    "suppression voie cyclable": None,
    "suppression voie réservée": None,
    "voie cyclable": None,
    "voie réservée": None,
    "voie verte": None,
    "zone 30": MeasureTypeEnum.SPEEDLIMITATION.value,
    "zone de rencontre": None,
}

# Measures neither fetched (locations, article) nor read: none of them can widen or narrow
# another measure of the act, and their 51 000 locations are two thirds of the base (2026-11-17).
# Every other unpublished measure is read before being dropped, in case it does.
UNREAD_MEASURE_LABELS: dict[str, str] = {
    "stationnement réservé": "a few places kept for some users, no restriction on traffic",
    "suppression stationnement réservé": "the end of such places",
    "feux d'intersection": "priority at a crossing",
    "Feux piéton": "priority at a crossing",
    "cédez le passage": "priority at a crossing",
    "stop": "priority at a crossing",
}
assert set(UNREAD_MEASURE_LABELS) <= {k for k, v in MEASURE_TYPE_BY_LABEL.items() if v is None}

# `1203`, the formula that opens the measure's article: one per measure name on the 10 781
# measures of 2026-11-17. The catalog holds richer ones, never used ("… interdit tous les
# jours, sauf dérogation…"): any other drops the measure (decided 2026-11-18).
OPENING_BY_LABEL: dict[str, str] = {
    "aire piétonne": "Il est institué une aire piétonne constituée par",
    "circulation interdite": "La circulation est interdite",
    "interdiction d'arrêt": "L'arrêt et le stationnement sont interdits",
    "interdiction de stationnement": "Le stationnement est interdit",
    "limitation catégorielle": "La circulation est interdite",
    "limitation de vitesse": "La vitesse maximale autorisée est fixée",
    "limitation dimensionnelle": "La circulation est interdite",
    "sens interdit (ou sens unique)": "Un sens unique de circulation est institué",
    "zone 30": "Les voies constituant la zone 30 sont",
}

# `1236`, `1204`, `1207`, in that order: the formulas the mail merge prints before the
# optional parameters 1 to 3, every value of the 10 781 measures of 2026-11-17. Two say the
# opposite of each other ("ne sont pas applicables" / "sont applicables"): any other drops the
# measure, like an unexpected opening (decided 2026-11-19).
_PENALTY = (
    "Tout stationnement d'un véhicule en infraction avec cette interdiction est considéré comme"
)
_NOT_APPLICABLE = "Toutefois ces dispositions ne sont pas applicables"
_APPLICABLE = "Ces dispositions sont applicables"
_PARKING_ALLOWED = "Le stationnement y est autorisé de manière"
KNOWN_OPTION_FORMULAS: tuple[frozenset[str], ...] = (
    frozenset(
        {
            _PENALTY,
            _NOT_APPLICABLE,
            _APPLICABLE,
            _PARKING_ALLOWED,
            "Le mouvement direct est autorisé",
            "Le mouvement de tourne à droite est autorisé",
        }
    ),
    frozenset({_PENALTY, _NOT_APPLICABLE, _APPLICABLE, _PARKING_ALLOWED}),
    frozenset({_NOT_APPLICABLE}),
)

LABEL_SPEED_LIMIT = "limitation de vitesse"
LABEL_ONE_WAY = "sens interdit (ou sens unique)"
# The number complements that make « depuis A vers B » or « depuis A jusqu'à B »: the act's
# text is these words around the bounds, so traffic goes from the first bound to the second.
ONE_WAY_FROM_COMPLEMENTS = frozenset({"depuis", "dans sa partie comprise entre"})
ONE_WAY_TO_COMPLEMENTS = frozenset({"vers", "jusqu'à"})
# The measures of an act that may open a one-way street the other way (`one_way_exceptions`).
LANE_LABELS = frozenset({"voie réservée", "voie cyclable", "voie verte"})
LABEL_CYCLE_LANE = "voie cyclable"
LABEL_GREENWAY = "voie verte"
LABEL_NO_CONTRAFLOW_CYCLES = "interdiction double sens pour les cyclistes"
LABEL_BOTH_WAYS_RESTORED = "rétablissement double sens"
# `2711` on a lane: its way against the act's general traffic.
LANE_SAME_WAY = "dans le sens de la circulation générale"
LANE_OTHER_WAY = "dans le sens inverse de la circulation générale"
LANE_BOTH_WAYS = "dans les deux sens"
LANE_DIRECTION_PARAMETER = "Direction"
LANE_TWO_WAY_VALUE = "bidirectionnelle"
LANE_USERS_PARAMETER = "véhicule concerné (2)"
# Every value of `véhicule concerné (2)` on the reserved lanes of 2026-11-17, as exemption
# labels (`EXEMPTION_BY_LABEL`) or words published as `other`. Any other drops the one-way street.
LANE_USERS_BY_LABEL: dict[str, tuple[str, ...]] = {
    "des véhicules de transports en commun": ("véhicules de transports en commun",),
    "des véhicules de transports en commun et cycles": (
        "véhicules de transports en commun",
        "aux cycles",
    ),
    "des cycles": ("aux cycles",),
    "des taxis": ("aux taxis",),
    "des riverains": ("des riverains",),
}
LABEL_ZONE_30 = "zone 30"
LABEL_PEDESTRIAN_AREA = "aire piétonne"
LABEL_DIMENSION_LIMIT = "limitation dimensionnelle"
LABEL_CATEGORY_LIMIT = "limitation catégorielle"

IMPLIED_MAX_SPEED_BY_LABEL: dict[str, int] = {LABEL_ZONE_30: 30}

SERVICE_POLICE_PREFECTURE = "Préfecture de Police"

# `1114` Raison. Everything the catalog holds beyond these lands on `other` + free text;
# permanent regulations carry no reason at all and get their own text.
REGULATION_SUBJECT_BY_REASON: dict[str, str] = {
    "Travaux": PostApiRegulationsAddBodySubject.ROADMAINTENANCE.value,
    "Evènement": PostApiRegulationsAddBodySubject.EVENT.value,
    "Manifestation": PostApiRegulationsAddBodySubject.EVENT.value,
    "Cinéma": PostApiRegulationsAddBodySubject.EVENT.value,
}
PERMANENT_SUBJECT_TEXT = "Réglementation permanente"
UNKNOWN_REASON_TEXT = "Motif non précisé"

# A value cell holds several catalog labels joined by "; " ("aux véhicules de secours ;
# aux véhicules des sapeurs pompiers"): 6 such cells on the perimeter of 2026-09-08.
MULTI_VALUE_SEPARATOR = ";"

RESTRICTION_PARAMETERS = frozenset(
    {
        "véhicule concerné (1)",
        "véhicule concerné (2)",
        "véhicule concerné (3)",
        "valeur de la limite",
    }
)

# Not a parameter of Eudonet: the closed list of vehicles a pedestrian area lets in, read in a
# text. It takes the place of the local access exemption (decided 2026-11-18).
PEDESTRIAN_ACCESS_PARAMETER = "desserte de l'aire piétonne"

EXEMPTION_PARAMETERS = frozenset(
    {
        "dérogation pour véhicule",
        "dérogation pour usager",
        "Dérogations véhicules",
        PEDESTRIAN_ACCESS_PARAMETER,
    }
)

# Deliberately ignored. `caractère aggravant` (`gênant` / `très gênant`) is the penal
# qualification of an illegal parking, not a restriction on a vehicle set.
IGNORED_PARAMETERS = frozenset({"caractère aggravant"})

SPEED_PARAMETER = "valeur de la vitesse"

# A filled time slot drops the measure: the only value seen on a mappable measure
# names public holidays, which DiaLog cannot express.
TIME_SLOT_PARAMETERS = frozenset({"Jours et Horaires"})

# A filled lane drops the measure: DiaLog would close the whole street, not the one
# lane named (19 values on 2026-09-08, all a single lane).
LANE_PARAMETERS = frozenset({"voie concernée"})

# Number complements (`2721`, `2731`, `2749`) that only introduce the bound, as seen on the
# published locations of 2026-09-08. Any other value drops the location ("à l'intersection
# avec" and "le" were never seen).
NEUTRAL_FROM_NUMBER_COMPLEMENTS = frozenset(
    {"au droit du", "dans sa partie comprise entre", "entre le", "depuis"}
)
NEUTRAL_TO_NUMBER_COMPLEMENTS = frozenset({"et le", "jusqu'à", "au droit du"})
NEUTRAL_POINT_NUMBER_COMPLEMENTS = frozenset({"au droit du"})
# "en face du n°" puts a parking ban on the other side, which DiaLog cannot carry; a closure
# takes the whole carriageway, so it only places the bound.
FACING_NUMBER_COMPLEMENT = "en vis à vis du"
# "depuis X vers Y" may name a direction: read as the end of a range only where none applies,
# on a parking ban or a pedestrian area.
TOWARDS_NUMBER_COMPLEMENT = "vers"

# The only value that means "no restriction at all".
ALL_VEHICLES_LABELS = frozenset({"à tous les véhicules"})


@dataclass(frozen=True, slots=True)
class VehicleRestriction:
    """One `restrictedTypes` entry, with the weight the label carries — and only that.

    A `None` weight stays `None`: `heavyweightMaxWeight` is never filled from a guess.
    """

    restricted_type: str
    heavyweight_max_weight: float | None = None


# `restrictedTypes` values the DiaLog API accepts.
RESTRICTED_HEAVY_GOODS = "heavyGoodsVehicle"
RESTRICTED_HAZARDOUS = "hazardousMaterials"
RESTRICTED_OTHER = "other"

# `exemptedTypes` values the DiaLog API accepts.
EXEMPTED_EMERGENCY = "emergencyServices"
EXEMPTED_LOCAL_RESIDENT = "localResident"
EXEMPTED_ROAD_WORKS = "roadMaintenanceOrConstruction"
EXEMPTED_BICYCLE = "bicycle"
EXEMPTED_COMMERCIAL = "commercial"
EXEMPTED_TAXI = "taxi"
EXEMPTED_OTHER = "other"

# Published as "other" + the label (closed list, 2026-10-26). Any other unmapped value drops
# its measure.
RESTRICTIONS_KEPT_AS_OTHER = frozenset(
    {
        "aux véhicules de plus de 2 mètres",
        "aux véhicules de plus de 10 mètres",
        "aux autocars",
    }
)

RESTRICTION_BY_LABEL: dict[str, VehicleRestriction] = {
    "aux véhicules de plus de 3,5 T": VehicleRestriction(RESTRICTED_HEAVY_GOODS, 3.5),
    "aux véhicules de plus de 7,5 T": VehicleRestriction(RESTRICTED_HEAVY_GOODS, 7.5),
    "aux véhicules de plus de 19 T": VehicleRestriction(RESTRICTED_HEAVY_GOODS, 19.0),
    "aux véhicules transportant des matières dangereuses": VehicleRestriction(RESTRICTED_HAZARDOUS),
}

EXEMPTION_BY_LABEL: dict[str, str] = {
    "aux véhicules de secours": EXEMPTED_EMERGENCY,
    "aux véhicules des sapeurs pompiers": EXEMPTED_EMERGENCY,
    "aux véhicules d'intérêt général prioritaire": EXEMPTED_EMERGENCY,
    "aux véhicules d'intérêt général bénéficiant de facilités de passage": EXEMPTED_EMERGENCY,
    "véhicules d'intérêt général prioritaires ou bénéficiant de facilités de passage": (
        EXEMPTED_EMERGENCY
    ),
    "aux véhicules des riverains": EXEMPTED_LOCAL_RESIDENT,
    "des riverains": EXEMPTED_LOCAL_RESIDENT,
    "aux véhicules de nettoiement": EXEMPTED_ROAD_WORKS,
    "aux véhicules de chantiers": EXEMPTED_ROAD_WORKS,
    "des véhicules de chantiers": EXEMPTED_ROAD_WORKS,
    "aux cycles": EXEMPTED_BICYCLE,
    "cycles": EXEMPTED_BICYCLE,
    "des cycles": EXEMPTED_BICYCLE,
    "aux véhicules de livraison": EXEMPTED_COMMERCIAL,
    "des véhicules de livraison": EXEMPTED_COMMERCIAL,
    "aux véhicules d'approvisionnement de marchés": EXEMPTED_COMMERCIAL,
    "des véhicules approvisionnement des marchés": EXEMPTED_COMMERCIAL,
    "aux taxis": EXEMPTED_TAXI,
    "des taxis": EXEMPTED_TAXI,
    # Spellings of the pedestrian areas' lists (2026-11-17): learnt when they have a true DiaLog
    # value and are used more than ten times; the others go as `other` with their words.
    "aux véhicules d'intervention urgente et de secours": EXEMPTED_EMERGENCY,
    "aux véhicules d'urgence et de secours": EXEMPTED_EMERGENCY,
    "aux véhicules de secours et de sécurité": EXEMPTED_EMERGENCY,
}

# One category standing for two exemptions: cycles, and scooters as `other` with their words.
_CYCLES_AND_SCOOTERS = re.compile(
    r"^cycles et engins de d[ée]placements? personnels?( motoris[ée]s)?$", re.IGNORECASE
)
SCOOTERS_LABEL = "engins de déplacement personnels"

# Exemptions a free text may name ("sauf aux véhicules de secours"), published as the
# parameter "dérogation pour véhicule" would be.
PARAGRAPH_EXEMPTION_PARAMETER = "dérogation pour véhicule"
CYCLES_EXEMPTION_LABEL = "aux cycles"
PARAGRAPH_EXEMPTION_LABELS = frozenset(
    label for label, exempted in EXEMPTION_BY_LABEL.items() if exempted == EXEMPTED_EMERGENCY
)


def exemption_label(text: str) -> str:
    """The catalog label a free text names ("véhicules des riverains" → "aux véhicules des
    riverains"), or the text itself, which goes out as `other` with its wording."""
    plain = " ".join(text.replace("’", "'").split())
    for candidate in (plain, plain.lower(), f"aux {plain.lower()}"):
        if candidate in EXEMPTION_BY_LABEL:
            return candidate
    return plain


def exemption_labels(text: str) -> list[str]:
    """`exemption_label`, split when one category names two kinds of vehicles."""
    plain = " ".join(text.replace("’", "'").split())
    if _CYCLES_AND_SCOOTERS.match(plain):
        return ["aux cycles", SCOOTERS_LABEL]
    return [exemption_label(plain)]


# Every parameter name the code reads; a filled one outside drops its measure.
KNOWN_PARAMETERS = (
    RESTRICTION_PARAMETERS
    | EXEMPTION_PARAMETERS
    | IGNORED_PARAMETERS
    | TIME_SLOT_PARAMETERS
    | LANE_PARAMETERS
    | {SPEED_PARAMETER}
)
