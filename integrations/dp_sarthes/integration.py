import io
from typing import cast

import polars as pl
import requests
from loguru import logger

from api.dia_log_client.models import (
    MeasureTypeEnum,
    PostApiRegulationsAddBody,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodyStatus,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
    SaveLocationDTO,
    SaveMeasureDTO,
    SaveRawGeoJSONDTO,
    SaveVehicleSetDTO,
)
from integrations.dp_sarthes.schema import SarthesMeasure, SarthesRawDataSchema
from integrations.shared import DialogIntegration

URL = (
    "https://data.sarthe.fr"
    "/api/explore/v2.1/catalog/datasets/227200029_limitations-vitesse/exports/csv"
    "?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"
)


class Integration(DialogIntegration):
    draft = True
    raw_data_schema = SarthesRawDataSchema

    def fetch_raw_data(self) -> pl.DataFrame:
        # download
        logger.info(f"Downloading data from {URL}")
        r = requests.get(URL)
        r.raise_for_status()

        # read CSV into Polars
        return pl.read_csv(
            io.BytesIO(r.content), separator=";", encoding="utf8", ignore_errors=True
        )

    def compute_clean_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        return (
            raw_data.pipe(compute_vitesse)
            .pipe(build_id_and_drop_duplicates)
            .pipe(compute_title)
            .pipe(compute_start_date)
            .pipe(compute_location_label)
            .pipe(compute_geometry)
            .filter(pl.col("geometry").is_not_null())
            .select(
                [
                    pl.col("id"),
                    pl.col("title"),
                    pl.col("VITESSE").alias("max_speed"),
                    pl.col("geometry"),
                    pl.col("label"),
                    # Period fields
                    pl.col("period_start_date"),
                    pl.col("period_end_date"),
                    pl.col("period_start_time"),
                    pl.col("period_end_time"),
                    pl.col("period_recurrence_type"),
                    pl.col("period_is_permanent"),
                ]
            )
        )

    def create_regulations(self, clean_data: pl.DataFrame) -> list[PostApiRegulationsAddBody]:
        regulations = []

        for row in clean_data.iter_rows(named=True):
            try:
                row = cast(SarthesMeasure, row)
                measure = self.create_measure(row)

                status = (
                    PostApiRegulationsAddBodyStatus.DRAFT
                    if self.draft
                    else PostApiRegulationsAddBodyStatus.PUBLISHED
                )

                regulations.append(
                    PostApiRegulationsAddBody(
                        identifier=row["id"],
                        category=PostApiRegulationsAddBodyCategory.PERMANENTREGULATION,
                        status=status,
                        subject=PostApiRegulationsAddBodySubject.OTHER,
                        title=row["title"],
                        other_category_text="Limitation de vitesse",
                        measures=[measure],  # type: ignore
                    )
                )
            except Exception as e:
                logger.error(f"Error creating regulation for id {row.get('id')}: {e}")

        return regulations

    def create_measure(self, measure: SarthesMeasure) -> SaveMeasureDTO:
        return SaveMeasureDTO(
            type_=MeasureTypeEnum.SPEEDLIMITATION,
            max_speed=int(measure["max_speed"]),
            periods=[self.create_save_period_dto(measure)],  # type: ignore
            locations=[self.create_save_location_dto(measure)],
            vehicle_set=SaveVehicleSetDTO(all_vehicles=True),
        )

    def create_save_location_dto(self, measure: SarthesMeasure) -> SaveLocationDTO:
        return SaveLocationDTO(
            road_type=RoadTypeEnum.RAWGEOJSON,
            raw_geo_json=SaveRawGeoJSONDTO(
                label=measure["label"],
                geometry=measure["geometry"],
            ),
        )


def compute_vitesse(df: pl.DataFrame) -> pl.DataFrame:
    """
    Cast VITESSE to int and drop rows where VITESSE is null or 0.
    """
    df = df.with_columns(pl.col("VITESSE").cast(pl.Int64))

    invalid = pl.col("VITESSE").is_null() | (pl.col("VITESSE") <= 0) | (pl.col("VITESSE") > 130)
    n_removed = df.select(invalid.sum()).item()

    if n_removed:
        logger.info(f"Removing {n_removed} rows with invalid VITESSE")

    return df.filter(~invalid)


def build_id_and_drop_duplicates(df: pl.DataFrame) -> pl.DataFrame:
    """
    Use `infobulle` as id when present.
    Otherwise build a deterministic 32-char hash from (loc_txt, VITESSE, longueur).
    Drop ALL rows involved in duplicated fallback hashes.
    """

    df = df.with_columns(
        pl.concat_str(
            [
                pl.col("loc_txt"),
                pl.col("VITESSE").cast(pl.Utf8),
                pl.col("longueur").cast(pl.Utf8),
            ],
            separator="|",
        )
        .hash(seed=0)
        .cast(pl.Utf8)
        .str.slice(0, 32)
        .alias("id")
    )

    # find duplicated hashes ONLY among fallback-generated ids
    dup_ids = df.group_by("id").len().filter(pl.col("len") > 1).select("id")

    if dup_ids.height > 0:
        logger.warning(
            "Found %d duplicated fallback ids, dropping ALL corresponding rows",
            dup_ids.height,
        )
        logger.debug("Duplicated ids: %s", dup_ids["id"].to_list())

    return df.join(dup_ids, on="id", how="anti")


def compute_title(df: pl.DataFrame) -> pl.DataFrame:
    """
    Create title from infobulle field, use "Inconnu" if empty or null.
    """
    return df.with_columns(
        pl.when(pl.col("infobulle").is_null() | (pl.col("infobulle") == ""))
        .then(pl.lit("Inconnu"))
        .otherwise(pl.col("infobulle"))
        .alias("title")
    )


def compute_start_date(df: pl.DataFrame) -> pl.DataFrame:
    """
    Compute all period fields for SavePeriodDTO.
    - period_start_date: from annee (Jan 1st) or date_modif as fallback
    - period_end_date, period_start_time, period_end_time: None
    - period_recurrence_type: EVERYDAY
    - period_is_permanent: True
    """
    # Log how many rows are using fallback date
    n_missing_annee = df.select(pl.col("annee").is_null().sum()).item()
    if n_missing_annee > 0:
        logger.info(f"Using date_modif as fallback for {n_missing_annee} rows with missing annee")

    return df.with_columns(
        [
            # Start date from annee or date_modif
            pl.when(pl.col("annee").is_not_null())
            .then(pl.col("annee").cast(pl.Int64).cast(pl.Utf8) + pl.lit("-01-01T00:00:00Z"))
            .otherwise(pl.col("date_modif"))
            .alias("period_start_date"),
            # Other period fields
            pl.lit(None).alias("period_end_date"),
            pl.lit(None).alias("period_start_time"),
            pl.lit(None).alias("period_end_time"),
            pl.lit("everyDay").alias("period_recurrence_type"),
            pl.lit(True).alias("period_is_permanent"),
        ]
    )


def compute_location_label(df: pl.DataFrame) -> pl.DataFrame:
    """
    Create label for location from loc_txt field, fallback to title if not available.
    """
    return df.with_columns(
        pl.when(pl.col("loc_txt").is_not_null() & (pl.col("loc_txt") != ""))
        .then(pl.col("loc_txt"))
        .otherwise(pl.col("title"))
        .alias("label")
    )


def compute_geometry(df: pl.DataFrame) -> pl.DataFrame:
    """
    Rename geo_shape to geometry (it's already in GeoJSON format).
    """
    return df.rename({"geo_shape": "geometry"})
