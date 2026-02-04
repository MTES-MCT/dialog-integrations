from datetime import datetime
from typing import TypedDict

import pandera.polars as pa

from api.dia_log_client.models import (
    MeasureTypeEnum,
)
from integrations.shared import typed_dict_to_polars_schema


class BrestRawDataSchema(pa.DataFrameModel):
    """Schema for raw data from Brest shapefile after minimal casting
    - only columns we actually use."""

    NOARR: str | None = pa.Field(nullable=True)
    DESCRIPTIF: str | None = pa.Field(nullable=True)
    LIBRU: str | None = pa.Field(nullable=True)
    LIBCO: str | None = pa.Field(nullable=True)
    geometry: str | None = pa.Field(nullable=True)
    SENS: int | None = pa.Field(nullable=True)
    VELO: bool  # After boolean casting
    CYCLO: bool  # After boolean casting
    VITEMAX: int | None = pa.Field(nullable=True)
    POIDS: float | None = pa.Field(nullable=True)
    HAUTEUR: float | None = pa.Field(nullable=True)
    LARGEUR: float | None = pa.Field(nullable=True)
    DT_MAT: datetime | None = pa.Field(nullable=True)

    class Config:
        strict = False  # Allow extra columns
        coerce = True  # Allow type coercion during validation


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
    # Period fields (prefixed with period_)
    period_start_date: str | None
    period_end_date: str | None
    period_start_time: str | None
    period_end_time: str | None
    period_recurrence_type: str | None
    period_is_permanent: bool | None


BREST_SCHEMA = typed_dict_to_polars_schema(BrestMeasure)
