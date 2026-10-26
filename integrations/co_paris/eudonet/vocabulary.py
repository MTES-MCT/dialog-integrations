"""Paris / Eudonet: the closed lookup tables between Eudonet catalogs and the DiaLog model.

Eudonet describes a measure with catalogued labels, not codes we can rely on: a measure
name (catalog `1202`), and up to nine (parameter name, parameter value) pairs whose names
come from catalogs `1218`/`1221` and whose values come from `1219`/`1222`. Everything this
module holds was read from the catalogs as the API served them on 2026-09-08
(`GET /Catalog/{descId}`), and cross-checked against the 26 325 measures extracted that day.
"""

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

# `1107` État (label → `DBValue`), by `1108` Type, recomputed by Eudonet every night at
# 00:30. A permanent is kept only "En vigueur": "Signé" may carry no date of effect, so it is
# not known to apply yet.
ACCEPTED_STATES_BY_TYPE: dict[str, dict[str, str]] = {
    TYPE_PERMANENT_LABEL: {"En vigueur": "96"},
    TYPE_TEMPORARY_LABEL: {"En vigueur": "8640", "Publié": "13789", "Signé": "8642"},
}

# `1202` Nom de la mesure — the 32 values of the catalog, none omitted.
# `None` means "outside the five DiaLog types" (R-30): the measure is dropped and counted,
# never bent into an approaching type (R-02).
MEASURE_TYPE_BY_LABEL: dict[str, str | None] = {
    # R-71: the producer names the area, so it is published as `noEntry` + `desserteLocale`
    # plus a 5 km/h twin (`add_pedestrian_area_speed`).
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
    # vehicles the parameters describe (R-33).
    "limitation catégorielle": MeasureTypeEnum.NOENTRY.value,
    "limitation de vitesse": MeasureTypeEnum.SPEEDLIMITATION.value,
    "limitation dimensionnelle": MeasureTypeEnum.NOENTRY.value,
    "Mesure libre": None,
    # A dead end closes one end of the street, not the street: as `noEntry` both ways it
    # would read as a full closure (R-02, decision of 2026-09-23).
    "mise en impasse": None,
    "obligation d'allumage des feux": None,
    "obligation de mouvement": None,
    "périmètre zone": None,
    "rétablissement double sens": None,
    # R-32 freeze: no one-direction measure until DiaLog carries the side of the road.
    "sens interdit (ou sens unique)": None,
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

LABEL_SPEED_LIMIT = "limitation de vitesse"
LABEL_ZONE_30 = "zone 30"
LABEL_PEDESTRIAN_AREA = "aire piétonne"
LABEL_DIMENSION_LIMIT = "limitation dimensionnelle"
LABEL_CATEGORY_LIMIT = "limitation catégorielle"

IMPLIED_MAX_SPEED_BY_LABEL: dict[str, int] = {LABEL_ZONE_30: 30}

PEDESTRIAN_AREA_SPEED = 5

SERVICE_POLICE_PREFECTURE = "Préfecture de Police"

# `1114` Raison. Everything the catalog holds beyond these lands on `other` + free text
# (R-27); permanent regulations carry no reason at all and get their own text.
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

EXEMPTION_PARAMETERS = frozenset(
    {
        "dérogation pour véhicule",
        "dérogation pour usager",
        "Dérogations véhicules",
    }
)

# Deliberately ignored. `caractère aggravant` (`gênant` / `très gênant`) is the penal
# qualification of an illegal parking, not a restriction on a vehicle set.
IGNORED_PARAMETERS = frozenset({"caractère aggravant"})

SPEED_PARAMETER = "valeur de la vitesse"

# Time slots, as free text. A filled one drops the measure (R-78): the only value seen on a
# mappable measure, "de 20h00 à 7h00 ainsi que les dimanches et jours fériés", names public
# holidays, which DiaLog cannot express.
TIME_SLOT_PARAMETERS = frozenset({"Jours et Horaires"})

# The lane a measure applies to. A filled one drops the measure (R-78): it narrows the
# measure to one lane of the street ("sur la voie unidirectionnelle réservée aux véhicules
# de transports en commun"), and DiaLog would close the whole street. 19 filled values on
# the extraction of 2026-09-08, all naming a single lane.
LANE_PARAMETERS = frozenset({"voie concernée"})

WHOLE_LOCATION_COMPLEMENTS = frozenset(
    {
        "sur toute la longueur",
        "sur toute la longueur de la voie",
        "sur toutes les places",
        "sur tout le stationnement",
        "sur l'intégralité de la voie",
    }
)
COMPLEMENT_CONNECTORS = frozenset(
    {
        *("et", "à", "a", "au", "aux", "du", "des", "le", "la", "les", "jusqu'à", "jusqu'au"),
        *("n", "n°", "no", "bis", "ter", "quater"),
    }
)

# The only value that means "no restriction at all" (R-34).
ALL_VEHICLES_LABELS = frozenset({"à tous les véhicules"})


@dataclass(frozen=True, slots=True)
class VehicleRestriction:
    """One `restrictedTypes` entry, with the weight the label carries — and only that.

    A `None` weight stays `None`: `heavyweightMaxWeight` is never filled from a guess (R-35).
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

# Restriction values with no DiaLog equivalent, published as "other" + the label: the
# closed list decided on 2026-10-26 (Q-58). Any other value drops its measure until a human
# qualifies it, "cycles" included (R-34: pedestrians and cycles are never restricted).
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
}

# Every parameter name the code reads; a filled one outside drops its measure (R-81).
KNOWN_PARAMETERS = (
    RESTRICTION_PARAMETERS
    | EXEMPTION_PARAMETERS
    | IGNORED_PARAMETERS
    | TIME_SLOT_PARAMETERS
    | LANE_PARAMETERS
    | {SPEED_PARAMETER}
)
