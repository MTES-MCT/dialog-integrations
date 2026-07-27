import pickle
from pathlib import Path
from typing import Dict

import polars as pl
from loguru import logger

from api.dia_log_client.models import (
    DirectionEnum,
    MeasureTypeEnum,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
)
from integrations.base_data_source_integration import BaseDataSourceIntegration

from .schema import ParisEudonetSchema

EUDONET_PARIS_URL = "https://eudonet-partage.apps.paris.fr/eudoapi/eudoapi/records/search"
PAGE_SIZE = 1000


class ParisEudonetDataSourceIntegration(BaseDataSourceIntegration):
    name = "temporaires_paris"
    raw_data_schema = ParisEudonetSchema
    mode = "raw"

    def fetch_raw_data(self):
        pickle_path = None
        if pickle_path is None:
            default_path = (
                Path(__file__).resolve().parents[3]
                / "explorations"
                / "co_paris"
                / "data"
                / "permanent.pickle"
            )
            pickle_path = default_path
        if isinstance(pickle_path, str):
            pickle_path = Path(pickle_path)
        if not pickle_path.exists():
            raise FileNotFoundError(...)
        with pickle_path.open("rb") as fh:
            raw_data = pickle.load(fh)
        logger.info(f"Loaded {len(raw_data)} Eudonet regulations from {pickle_path}")
        return raw_data

    def compute_clean_data(self, raw_data: list[Dict]):  # type: ignore
        records = []
        for regulation in raw_data or []:
            fields = regulation.get("fields", {})
            regulation_id = fields.get(1101)
            for measure in regulation.get("measures", []):
                measure_fields = measure.get("fields", {})
                for location in measure.get("locations", []):
                    location_fields = location.get("fields", {})
                    records.append(
                        {
                            "arrete_num": str(regulation_id),
                            "arrete_libelle": _pick_first(fields, [1102, 1101, 1108]),
                            "arrete_start_date": fields.get(1109) or "01/01/2001",
                            "measure_num": measure.get("fileId"),
                            "measure_name": measure_fields.get(1202),
                            "location_num": location.get("fileId"),
                            "type_section": location_fields.get(2705),
                            "arrondissement": location_fields.get(2708),
                            "voie": location_fields.get(2710),
                            "intersection_deb": location_fields.get(2730) or None,
                            "intersection_fin": location_fields.get(2740) or None,
                            "numero_debut": location_fields.get(2720) or None,
                            "numero_fin": location_fields.get(2737) or None,
                        }
                    )
        if not records:
            return pl.DataFrame()

        df = pl.from_dicts(records, infer_schema_length=50000)

        return (
            df.pipe(compute_measure_fields)
            .pipe(compute_period_fields)
            .pipe(compute_location_fields)
            .pipe(compute_regulation_fields)
            .pipe(compute_vehicle_fields)
        )
        # .limit(20).pipe(_debug)


def _debug(df):
    breakpoint()
    return df


def compute_measure_fields(df: pl.DataFrame) -> pl.DataFrame:

    df = df.with_columns(
        [
            pl.when(pl.col("measure_name") == "interdiction de stationnement")
            .then(pl.lit(MeasureTypeEnum.PARKINGPROHIBITED))
            .when(pl.col("measure_name") == "interdiction d'arrêt")
            .then(pl.lit(MeasureTypeEnum.PARKINGPROHIBITED))
            .when(pl.col("measure_name") == "zone 30")
            .then(pl.lit(MeasureTypeEnum.SPEEDLIMITATION))
            .when(pl.col("measure_name") == "circulation interdite")
            .then(pl.lit(MeasureTypeEnum.NOENTRY.value))
            .otherwise(pl.lit(None))
            .alias("measure_type_"),
        ]
    )

    df = df.with_columns(
        pl.when(pl.col("measure_name") == "zone 30")
        .then(30.0)
        .otherwise(None)
        .alias("measure_max_speed")
    )

    null_measure_type = df.select(pl.col("measure_type_").is_null().sum()).item()
    logger.warning(f"Dropping {null_measure_type} rows due to unable to infer restriction type")
    df = df.filter(pl.col("measure_type_").is_not_null())

    return df


def compute_period_fields(df: pl.DataFrame) -> pl.DataFrame:
    """
    Compute all period fields for SavePeriodDTO.
    - period_start_date: arrete_start_date (timestamp)
    - period_end_date: arrete_end_date (timestamp)
    - period_start_time: arrete_start_date (timestamp)
    - period_end_time: arrete_end_date (timestamp)
    - period_recurrence_type: everyDay
    - period_is_permanent: False
    """

    return df.with_columns(
        [
            pl.when(pl.col("arrete_start_date") != "")
            .then(
                pl.col("arrete_start_date")
                .str.to_datetime("%d/%m/%Y")
                .dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            )
            .otherwise(pl.lit("2001-01-01T00:00:00"))
            .alias("period_start_date"),
            pl.lit(None).alias("period_end_date"),
            pl.lit(None).alias("period_start_time"),
            pl.lit(None).alias("period_end_time"),
            pl.lit("everyDay").alias("period_recurrence_type"),
            pl.lit(True).alias("period_is_permanent"),
        ]
    )


def compute_location_fields(df: pl.DataFrame) -> pl.DataFrame:

    cannot_locate = ~(
        (pl.col("arrondissement") != "")
        & (pl.col("voie") != "")
        & (
            (
                ((pl.col("intersection_deb") != "") | (pl.col("numero_debut") != ""))
                & ((pl.col("intersection_fin") != "") | (pl.col("numero_fin") != ""))
            )
            | (pl.col("type_section") == "La totalité de la voie")
        )
    )

    num_dropped = df.select(cannot_locate.sum()).item()
    logger.warning(f"Dropping {num_dropped} rows where location cannot be infered")
    df = df.filter(~cannot_locate)

    df = df.with_columns(
        [
            pl.lit(RoadTypeEnum.LANE.value).alias("location_road_type"),
            pl.col("arrondissement")
            .map_elements(_arrondissement_insee)
            .cast(pl.Utf8)
            .alias("location_city_code"),
            (pl.lit("Paris – ") + pl.col("arrondissement")).alias("location_city_label"),
            pl.col("voie").alias("location_road_name"),
            pl.when(pl.col("type_section") == "La totalité de la voie")
            .then(pl.lit(True))
            .otherwise(pl.lit(None))
            .alias("location_is_entire_street"),
            pl.when(pl.col("intersection_deb").is_not_null())
            .then(pl.lit("intersection"))
            .when(pl.col("numero_debut").is_not_null())
            .then(pl.lit("houseNumber"))
            .otherwise(pl.lit(None))
            .alias("location_from_point_type"),
            pl.when(pl.col("intersection_fin").is_not_null())
            .then(pl.lit("intersection"))
            .when(pl.col("numero_fin").is_not_null())
            .then(pl.lit("houseNumber"))
            .otherwise(pl.lit(None))
            .alias("location_to_point_type"),
            pl.when(pl.col("intersection_deb").is_not_null())
            .then(pl.col("intersection_deb"))
            .otherwise(pl.lit(None))
            .alias("location_from_road_name"),
            pl.when(pl.col("intersection_fin").is_not_null())
            .then(pl.col("intersection_fin"))
            .otherwise(pl.lit(None))
            .alias("location_to_road_name"),
            pl.when(pl.col("numero_debut").is_not_null())
            .then(pl.col("numero_debut"))
            .otherwise(pl.lit(None))
            .alias("location_from_house_number"),
            pl.when(pl.col("numero_fin").is_not_null())
            .then(pl.col("numero_fin"))
            .otherwise(pl.lit(None))
            .alias("location_to_house_number"),
            pl.lit(DirectionEnum.BOTH.value).alias("location_direction"),
        ]
    )
    return df


def _arrondissement_insee(litteral):
    if litteral == "1er arrondissement":
        return 75101
    elif litteral == "2ème arrondissement":
        return 75102
    elif litteral == "3ème arrondissement":
        return 75103
    elif litteral == "4ème arrondissement":
        return 75104
    elif litteral == "5ème arrondissement":
        return 75105
    elif litteral == "6ème arrondissement":
        return 75106
    elif litteral == "7ème arrondissement":
        return 75107
    elif litteral == "8ème arrondissement":
        return 75108
    elif litteral == "9ème arrondissement":
        return 75109
    elif litteral == "10ème arrondissement":
        return 75110
    elif litteral == "11ème arrondissement":
        return 75111
    elif litteral == "12ème arrondissement":
        return 75112
    elif litteral == "13ème arrondissement":
        return 75113
    elif litteral == "14ème arrondissement":
        return 75114
    elif litteral == "15ème arrondissement":
        return 75115
    elif litteral == "16ème arrondissement":
        return 75116
    elif litteral == "17ème arrondissement":
        return 75117
    elif litteral == "18ème arrondissement":
        return 75118
    elif litteral == "19ème arrondissement":
        return 75119
    elif litteral == "20ème arrondissement":
        return 75120
    else:
        return 75056  # Departement de Paris, la fonction ne peiut pas retourner None


def compute_regulation_fields(df: pl.DataFrame) -> pl.DataFrame:
    df = df.with_columns(
        [
            (pl.lit("PARIS/PERMANENTS/") + pl.col("arrete_num").cast(pl.Utf8)).alias(
                "regulation_identifier"
            ),
            pl.lit(PostApiRegulationsAddBodyCategory.PERMANENTREGULATION.value).alias(
                "regulation_category"
            ),
            pl.lit(PostApiRegulationsAddBodySubject.OTHER.value).alias("regulation_subject"),
            (pl.col("arrete_libelle").fill_null("").cast(pl.Utf8)).alias("regulation_title"),
            pl.lit("Circulation").alias("regulation_other_category_text"),
        ]
    )

    libelle_is_html = pl.col("regulation_title").str.starts_with("<")
    num_dropped = df.select(libelle_is_html.sum()).item()
    logger.warning(f"Dropping {num_dropped} rows where libelle is HTML/XML")
    df = df.filter(~libelle_is_html)
    return df


def compute_vehicle_fields(df: pl.DataFrame):
    """
    Compute all vehicle fields for SaveVehicleSetDTO.
    - vehicle_all_vehicles: true
    """

    return df.with_columns([pl.lit(True).alias("vehicle_all_vehicles")])


def _pick_first(d, fs):
    r = None
    for f in fs:
        r = d.get(f, None)
        if r is not None:
            break
    return r
