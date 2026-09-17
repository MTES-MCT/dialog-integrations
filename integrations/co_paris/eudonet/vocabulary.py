"""Paris / Eudonet: the closed lookup tables between Eudonet catalogs and the DiaLog model.

Eudonet describes a measure with catalogued labels, not codes we can rely on: a measure
name (catalog `1202`), and up to nine (parameter name, parameter value) pairs whose names
come from catalogs `1218`/`1221` and whose values come from `1219`/`1222`. Everything this
module holds was read from `explorations/co_paris/data/eudonet_2026-09-08/catalogs.json`
and cross-checked against the 26 325 measures of the same dump.

Two different contracts live here, on purpose:

* `MEASURE_TYPE_BY_LABEL` is **strictly closed**. The 32 values of catalog `1202` are all
  listed; a label that is not in it means Paris enriched the catalog and the mapping has to
  be decided by a human, not guessed (R-02, Q-05). `compute_measure_fields` raises.
* the vehicle tables are closed on what the catalogs contain today, with a **documented
  fallback**: an unknown vehicle label becomes `other` + the label itself as free text
  (plan §4.2). That is lossless for the reader and invents no threshold (R-35).
"""

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

# `1107` État. Eudonet recomputes it every night at 00:30. An act that is not signed is not
# enforceable; `Périmé` and `Abrogé` have left the perimeter (plan §1).
ACCEPTED_STATE_LABELS = ("En vigueur", "Publié", "Signé")

# `1202` Nom de la mesure — the 32 values of the catalog, none omitted.
# `None` means "outside the five DiaLog types" (R-30): the measure is dropped and counted,
# never bent into an approaching type (R-02).
MEASURE_TYPE_BY_LABEL: dict[str, str | None] = {
    "aire piétonne": None,
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
    "mise en impasse": MeasureTypeEnum.NOENTRY.value,
    "obligation d'allumage des feux": None,
    "obligation de mouvement": None,
    "périmètre zone": None,
    "rétablissement double sens": None,
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

# Measure labels the transformations single out by name.
LABEL_SPEED_LIMIT = "limitation de vitesse"
LABEL_ZONE_30 = "zone 30"
LABEL_ONE_WAY = "sens interdit (ou sens unique)"
LABEL_DIMENSION_LIMIT = "limitation dimensionnelle"
LABEL_CATEGORY_LIMIT = "limitation catégorielle"

# `zone 30` carries its speed in its own name; nothing else does.
IMPLIED_MAX_SPEED_BY_LABEL: dict[str, int] = {LABEL_ZONE_30: 30}

# `2711` Sens. Only the three that name a direction along the segment can be turned into a
# DiaLog direction; "dans le sens (inverse) de la circulation générale" says nothing about
# how the segment itself is oriented, so a one-way built on it would be a coin toss (R-32).
DIRECTIONAL_LOCATION_LABELS = (
    "du début vers la fin du segment",
    "de la fin vers le début du segment",
    "dans les deux sens",
)

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

# ---------------------------------------------------------------------------
# Vehicles (plan §4.2)
# ---------------------------------------------------------------------------

# A value cell holds several catalog labels joined by "; " ("aux véhicules de secours ;
# aux véhicules des sapeurs pompiers"): 6 such cells on the perimeter of 2026-09-08.
MULTI_VALUE_SEPARATOR = ";"

# Parameter names (catalogs `1218`/`1221`) that describe **who the measure applies to**.
RESTRICTION_PARAMETERS = frozenset(
    {
        "véhicule concerné (1)",
        "véhicule concerné (2)",
        "véhicule concerné (3)",
        "valeur de la limite",
    }
)

# Parameter names that describe **who escapes it**.
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

# Time slots. The pivot has no `timeSlots` / `dailyRange`, so the parameter is dropped —
# but counted, because dropping it broadens the measure to the whole day (plan §4.3).
TIME_SLOT_PARAMETERS = frozenset({"Jours et Horaires"})

# The only value that means "no restriction at all" (R-34).
ALL_VEHICLES_LABELS = frozenset({"à tous les véhicules"})


class VehicleRestriction:
    """One `restrictedTypes` entry, with the threshold the label carries — and only that.

    A `None` threshold stays `None`: `heavyweightMaxWeight` / `maxLength` / `maxHeight` /
    `maxWidth` are never filled from a guess (R-35).
    """

    __slots__ = (
        "restricted_type",
        "heavyweight_max_weight",
        "max_length",
        "max_height",
        "max_width",
    )

    def __init__(
        self,
        restricted_type: str,
        heavyweight_max_weight: float | None = None,
        max_length: float | None = None,
        max_height: float | None = None,
        max_width: float | None = None,
    ):
        self.restricted_type = restricted_type
        self.heavyweight_max_weight = heavyweight_max_weight
        self.max_length = max_length
        self.max_height = max_height
        self.max_width = max_width


# `restrictedTypes` values the API accepts (output-api.md § vehicleSet.restrictedTypes).
RESTRICTED_HEAVY_GOODS = "heavyGoodsVehicle"
RESTRICTED_DIMENSIONS = "dimensions"
RESTRICTED_HAZARDOUS = "hazardousMaterials"
RESTRICTED_OTHER = "other"

# `exemptedTypes` values the API accepts (output-api.md § vehicleSet.exemptedTypes).
EXEMPTED_EMERGENCY = "emergencyServices"
EXEMPTED_LOCAL_RESIDENT = "localResident"
EXEMPTED_ROAD_WORKS = "roadMaintenanceOrConstruction"
EXEMPTED_BICYCLE = "bicycle"
EXEMPTED_COMMERCIAL = "commercial"
EXEMPTED_TAXI = "taxi"
EXEMPTED_OTHER = "other"

# Values of catalogs `1219`/`1222` seen under a restriction parameter.
#
# ⚠️ "aux véhicules de plus de N mètres" — the label does **not** say which dimension.
# Checked on the whole 2026-09-08 dump: not one of the 26 325 measures carries a
# `longueur` / `hauteur` / `largeur` value alongside it, even though catalogs `1219` and
# `1222` define those three labels. There is therefore nothing to disambiguate it with, and
# picking `maxLength` would be inventing the dimension. They map to `other` + the label,
# which keeps the metres readable and restricts nobody by mistake (R-35, plan question 6).
RESTRICTION_BY_LABEL: dict[str, VehicleRestriction] = {
    "aux véhicules de plus de 3,5 T": VehicleRestriction(RESTRICTED_HEAVY_GOODS, 3.5),
    "aux véhicules de plus de 7,5 T": VehicleRestriction(RESTRICTED_HEAVY_GOODS, 7.5),
    "aux véhicules de plus de 19 T": VehicleRestriction(RESTRICTED_HEAVY_GOODS, 19.0),
    "aux véhicules transportant des matières dangereuses": VehicleRestriction(RESTRICTED_HAZARDOUS),
}

# Values of catalogs `1219`/`1222` seen under an exemption parameter.
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
