from datetime import datetime
from typing import TypedDict

from api.dia_log_client.models import (
    MeasureTypeEnum,
)
from integrations.shared import typed_dict_to_polars_schema

DESCRIPTION_CONFIG = {
    # Limitations de vitesse
    "Limitation Vitesse": {
        "measure_type": MeasureTypeEnum.SPEEDLIMITATION,
    },
    # Stationnement
    "Stationnement interdit": {
        "measure_type": MeasureTypeEnum.PARKINGPROHIBITED,
    },
    "Arrêt interdit": {
        "measure_type": MeasureTypeEnum.PARKINGPROHIBITED,
    },
    "Stationnement gênant": {
        "measure_type": MeasureTypeEnum.PARKINGPROHIBITED,
    },
    "Stationnement interdit aux poids-lourds": {
        "measure_type": MeasureTypeEnum.PARKINGPROHIBITED,
    },
    # noEntry – limitations dimensionnelles (poids / hauteur)
    "Limitation Poids": {
        "measure_type": MeasureTypeEnum.NOENTRY,
    },
    "Limitation Hauteur": {
        "measure_type": MeasureTypeEnum.NOENTRY,
    },
    "Interdit aux transports de marchandises": {
        "measure_type": MeasureTypeEnum.NOENTRY,
    },
    # noEntry – catégories particulières`
    "Interdit dans les 2 sens": {
        "measure_type": MeasureTypeEnum.NOENTRY,
    },
    "Interdit à  tous véhicules à moteur": {
        "measure_type": MeasureTypeEnum.NOENTRY,
        "exempted_types": ["bicycle", "pedestrians"],
    },
    "Interdit aux véhicules à moteur sauf cyclos": {
        # motorisés interdits, sauf cyclomoteurs (et vélos + piétons)
        "measure_type": MeasureTypeEnum.NOENTRY,
        "exempted_types": ["bicycle", "pedestrians", "other"],
    },
    "Limitation Largeur": {
        "measure_type": MeasureTypeEnum.NOENTRY,
    },
    "Sens interdit / Sens unique": {
        "measure_type": MeasureTypeEnum.NOENTRY,
        "filter": lambda df: df["SENS"] == 0,
    },
}


class BrestMeasure(TypedDict):
    NOARR: str
    DESCRIPTIF: str
    LIBRU: str
    LIBCO: str
    geometry: str
    SENS: int
    VELO: bool
    CYCLO: bool
    VITEMAX: int | None
    POIDS: float
    HAUTEUR: float
    LARGEUR: float
    DT_MAT: datetime | None


BREST_SCHEMA = typed_dict_to_polars_schema(BrestMeasure)
