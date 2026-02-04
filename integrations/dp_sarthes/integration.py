import io

import polars as pl
import requests
from loguru import logger

from api.dia_log_client.models import (
    MeasureTypeEnum,
    PeriodRecurrenceTypeEnum,
    PostApiRegulationsAddBody,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodyStatus,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
    SaveLocationDTO,
    SaveMeasureDTO,
    SavePeriodDTO,
    SaveRawGeoJSONDTO,
    SaveVehicleSetDTO,
)
from integrations.shared import DialogIntegration

URL = (
    "https://data.sarthe.fr"
    "/api/explore/v2.1/catalog/datasets/227200029_limitations-vitesse/exports/csv"
    "?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"
)


class Integration(DialogIntegration):
    draft = True

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
                    pl.col("start_date"),
                    pl.col("geometry"),
                    pl.col("label"),
                ]
            )
        )

    def create_regulations(self, clean_data: pl.DataFrame) -> list[PostApiRegulationsAddBody]:
        regulations = []

        for row in clean_data.iter_rows(named=True):
            try:
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

    def create_measure(self, row: dict) -> SaveMeasureDTO:
        return SaveMeasureDTO(
            type_=MeasureTypeEnum.SPEEDLIMITATION,
            max_speed=int(row["max_speed"]),
            periods=[self.create_save_period_dto(row)],
            locations=[self.create_save_location_dto(row)],
            vehicle_set=SaveVehicleSetDTO(all_vehicles=True),
        )

    def create_save_period_dto(self, row: dict) -> SavePeriodDTO:
        start_date = row.get("start_date")

        return SavePeriodDTO(
            start_date=start_date,
            end_date=None,
            start_time=None,
            end_time=None,
            recurrence_type=PeriodRecurrenceTypeEnum.EVERYDAY,
            is_permanent=True,
            time_slots=None,
            daily_range=None,
        )

    def create_save_location_dto(self, row: dict) -> SaveLocationDTO:
        return SaveLocationDTO(
            road_type=RoadTypeEnum.RAWGEOJSON,
            raw_geo_json=SaveRawGeoJSONDTO(
                label=row["label"],
                geometry=row["geometry"],
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
    Create start_date from annee column (January 1st of the year).
    Falls back to date_modif if annee is not present.
    """
    return df.with_columns(
        pl.when(pl.col("annee").is_not_null())
        .then(pl.col("annee").cast(pl.Int64).cast(pl.Utf8) + pl.lit("-01-01T00:00:00Z"))
        .otherwise(pl.col("date_modif"))
        .alias("start_date")
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
