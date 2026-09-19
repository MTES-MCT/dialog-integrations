"""Tests for the Issy-les-Moulineaux integration (travaux de voirie).

The fixture reproduces the types the Opendatasoft API actually returns (multivalued
`mesure_titre` / `mesures`, dates as strings, `geolocalisation` as a lon/lat struct):
past regressions came from them.
"""

import importlib.util
import json
import sys

import polars as pl
import pytest

from api.dia_log_client.models import MeasureTypeEnum, PostApiRegulationsAddBodyStatus
from integrations.base_integration import BaseIntegration

MODULE = "integrations.co_issy-les-moulineaux.travaux_voirie.data_source_integration"


def _load_module():
    """The package name holds a hyphen: a plain import is impossible."""
    spec = importlib.util.find_spec(MODULE)
    module = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    sys.modules[spec.name] = module  # type: ignore[union-attr]
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


RAW_RECORDS = [
    # One arrete over two streets. Each street carries several measures: one publishable
    # (parking), one understood but reserved to segments, one with no equivalent.
    {
        "reference": "ACP/2026/001",
        "type_travaux": "Arrêté provisoire de restriction de la circulation",
        "mesure_titre": ["Limitation vitesse", "Stationnement gênant"],
        "mesures": [
            "Un rappel de la limitation de vitesse à 30 km/h devra être matérialisé.",
            "Un stationnement gênant sur toute la voie.",
        ],
        "rue_principal": "RUE ERNEST RENAN",
        "commune": "Issy-les-Moulineaux",
        "description": "travaux de réfection",
        "date_debut": "2026-04-27",
        "date_fin": "2026-10-02",
        "geolocalisation": {"lon": 2.249717, "lat": 48.81946},
        "url": None,
    },
    {
        "reference": "ACP/2026/001",
        "type_travaux": "Arrêté provisoire de restriction de la circulation",
        "mesure_titre": ["Barrage de voie", "Stationnement gênant", "Rétrecissement de chaussée"],
        "mesures": [
            "Un barrage de voie, de 09h00 à 16h30.",
            "Un stationnement gênant au droit du numéro 2.",
            "Un rétrécissement de la chaussée.",
        ],
        "rue_principal": "RUE SEVERINE",
        "commune": "Issy-les-Moulineaux",
        "description": "travaux de réfection",
        "date_debut": "2026-04-27",
        "date_fin": "2026-10-02",
        "geolocalisation": {"lon": 2.25, "lat": 48.82},
        "url": None,
    },
    # No geolocation: must be dropped.
    {
        "reference": "ACP/2026/002",
        "type_travaux": "Arrêté provisoire de restriction de la circulation",
        "mesure_titre": ["Stationnement gênant"],
        "mesures": ["Un stationnement gênant."],
        "rue_principal": "BOULEVARD VOLTAIRE",
        "commune": "Issy-les-Moulineaux",
        "description": "travaux divers",
        "date_debut": "2026-05-01",
        "date_fin": "2026-05-10",
        "geolocalisation": {"lon": None, "lat": None},
        "url": None,
    },
    # No end date: must be dropped, or the measure would stay active forever.
    # Publishable otherwise (parking + point).
    {
        "reference": "ACP/2026/004",
        "type_travaux": "Arrêté provisoire de restriction de la circulation",
        "mesure_titre": ["Stationnement gênant"],
        "mesures": ["Un stationnement gênant jusqu'à nouvel ordre."],
        "rue_principal": "RUE DU GENERAL LECLERC",
        "commune": "Issy-les-Moulineaux",
        "description": "travaux sans terme",
        "date_debut": "2026-06-01",
        "date_fin": None,
        "geolocalisation": {"lon": 2.27, "lat": 48.84},
        "url": None,
    },
    # No measure: must be dropped.
    {
        "reference": "ACP/2026/003",
        "type_travaux": "Arrêté provisoire de restriction de la circulation",
        "mesure_titre": None,
        "mesures": None,
        "rue_principal": "RUE MICHELET",
        "commune": "Issy-les-Moulineaux",
        "description": "travaux divers",
        "date_debut": "2026-05-01",
        "date_fin": "2026-05-10",
        "geolocalisation": {"lon": 2.26, "lat": 48.83},
        "url": None,
    },
]


@pytest.fixture
def clean_data():
    module = _load_module()
    data_source = module.DataSourceIntegration(None, None)
    data_source.fetch_raw_data = lambda: pl.DataFrame(RAW_RECORDS)
    return data_source.compute_data_regulations()


def test_une_ligne_par_mesure_publiable(clean_data):
    """Multivalued labels are exploded: one row per measure actually published."""
    assert clean_data.shape[0] == 2
    assert clean_data["measure_type_"].to_list() == [
        MeasureTypeEnum.PARKINGPROHIBITED.value,
        MeasureTypeEnum.PARKINGPROHIBITED.value,
    ]


def test_types_reserves_aux_troncons_ecartes(clean_data):
    """The source gives a point only: parking prohibition is the only type published.

    "Limitation vitesse" and "Barrage de voie" are understood by MEASURE_TYPE_BY_LABEL
    but dropped: a speed limit or a closure applies to a segment, not a point.
    """
    module = _load_module()
    assert module.PUBLISHED_MEASURE_TYPES == [MeasureTypeEnum.PARKINGPROHIBITED.value]

    publies = set(clean_data["measure_type_"])
    assert MeasureTypeEnum.SPEEDLIMITATION.value not in publies
    assert MeasureTypeEnum.NOENTRY.value not in publies


def test_mesure_sans_date_de_fin_ecartee(clean_data):
    """A measure with no end would stay active forever: nothing closes it afterwards.

    The arrete is otherwise publishable (parking + point): only the missing `date_fin`
    drops it.
    """
    assert "ACP/2026/004" not in set(clean_data["regulation_identifier"])
    assert None not in set(clean_data["period_end_date"])


def test_measure_type_survit_au_pivot(clean_data):
    """Guard: RegulationMeasure silently drops any column it does not declare."""
    assert "measure_type_" in clean_data.columns
    assert "measure_max_speed" in clean_data.columns


def test_aucune_vitesse_portee_par_une_mesure_de_stationnement(clean_data):
    """A speed read from the text must not follow a measure of another type.

    Regression: the measures' texts used to be concatenated per arrete, and a parking
    measure left with the 30 km/h of the neighbouring limitation.
    """
    assert clean_data["measure_max_speed"].to_list() == [None, None]


def test_geometrie_point_en_lon_lat(clean_data):
    ligne = clean_data.filter(pl.col("location_label") == "RUE ERNEST RENAN - Issy-les-Moulineaux")
    geometry = json.loads(ligne["location_geometry"][0])
    assert geometry["type"] == "Point"
    assert geometry["coordinates"] == [2.249717, 48.81946]


@pytest.mark.parametrize(
    "longueur, doit_etre_tronquee",
    [(20, False), (255, False), (256, True), (323, True)],
)
def test_titre_tronque_a_la_limite_de_l_api(longueur, doit_etre_tronquee):
    """The API caps `title` at 255 characters and the client does not check it."""
    module = _load_module()
    description = "a" * longueur

    titre = module.compute_regulation_fields(
        pl.DataFrame(
            {
                "reference": ["ACP/2026/009"],
                "description": [description],
                "type_travaux": ["Arrêté provisoire"],
                "url": [None],
            },
            schema_overrides={"url": pl.Utf8},
        )
    )["regulation_title"][0]

    assert len(titre) <= module.TITLE_MAX_LENGTH
    if doit_etre_tronquee:
        assert len(titre) == module.TITLE_MAX_LENGTH
        assert titre.endswith(module.TITLE_ELLIPSIS)
    else:
        assert titre == description


def test_regroupement_en_un_arrete_multi_mesures(clean_data):
    class _Integration(BaseIntegration):
        def __init__(self):
            self.status = PostApiRegulationsAddBodyStatus.DRAFT

    regulations = _Integration().create_regulations(clean_data)

    assert len(regulations) == 1
    regulation = regulations[0]
    assert regulation.identifier == "ACP/2026/001"
    measures = list(regulation.measures or [])
    assert len(measures) == 2
    # Both streets of the arrete are kept, one per measure.
    labels = {
        measure.locations[0].raw_geo_json.label  # type: ignore[union-attr]
        for measure in measures
    }
    assert labels == {
        "RUE ERNEST RENAN - Issy-les-Moulineaux",
        "RUE SEVERINE - Issy-les-Moulineaux",
    }
