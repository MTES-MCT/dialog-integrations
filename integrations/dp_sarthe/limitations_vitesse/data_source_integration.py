import hashlib
import io

import polars as pl
import requests
from loguru import logger

from api.dia_log_client.models import (
    MeasureTypeEnum,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
)
from integrations.base_data_source_integration import BaseDataSourceIntegration
from integrations.dp_sarthe.limitations_vitesse.schema import SartheRawDataSchema
from integrations.shared.local_time import start_of_local_day

URL = (
    "https://data.sarthe.fr"
    "/api/explore/v2.1/catalog/datasets/227200029_limitations-vitesse/exports/csv"
    "?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"
)


class DataSourceIntegration(BaseDataSourceIntegration):
    """Data source for Sarthe limitations vitesse CSV data."""

    raw_data_schema = SartheRawDataSchema
    name = "limitations_vitesse"

    def fetch_raw_data(self) -> pl.DataFrame:
        logger.info(f"Downloading data from {URL}")
        r = requests.get(URL)
        r.raise_for_status()

        return pl.read_csv(
            io.BytesIO(r.content), separator=";", encoding="utf8", ignore_errors=True
        )

    def compute_clean_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        return (
            raw_data.pipe(compute_measure_fields)
            .pipe(compute_title)
            .pipe(compute_start_date)
            .pipe(compute_location_fields)
            .pipe(self.compute_regulation_fields)
            .pipe(compute_vehicle_fields)
        )

    def compute_regulation_fields(self, df: pl.DataFrame) -> pl.DataFrame:
        """Identify each row by md5(loc_txt | speed | longueur); rows sharing a hash are
        all dropped (D-07)."""

        def deterministic_hash(s: str) -> str:
            return hashlib.md5(s.encode()).hexdigest()

        df = df.with_columns(
            pl.concat_str(
                [
                    pl.col("loc_txt"),
                    pl.col("measure_max_speed").cast(pl.Utf8),
                    pl.col("longueur").cast(pl.Utf8),
                ],
                separator="|",
            )
            .map_elements(deterministic_hash, return_dtype=pl.Utf8)
            .alias("id")
        )

        dup_ids = df.group_by("id").len().filter(pl.col("len") > 1).select("id")

        if dup_ids.height > 0:
            logger.warning(
                "Found %d duplicated fallback ids, dropping ALL corresponding rows",
                dup_ids.height,
            )
            logger.debug("Duplicated ids: %s", dup_ids["id"].to_list())

        df = df.join(dup_ids, on="id", how="anti")

        return df.with_columns(
            [
                pl.col("id").alias("regulation_identifier"),
                pl.lit(PostApiRegulationsAddBodyCategory.PERMANENTREGULATION.value).alias(
                    "regulation_category"
                ),
                pl.lit(PostApiRegulationsAddBodySubject.OTHER.value).alias("regulation_subject"),
                pl.col("title").alias("regulation_title"),
                pl.lit("Limitation de vitesse").alias("regulation_other_category_text"),
            ]
        )


def compute_measure_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Speed limitation from VITESSE; rows outside ]0, 130] are dropped."""
    df = df.with_columns(pl.col("VITESSE").cast(pl.Int64))

    invalid = pl.col("VITESSE").is_null() | (pl.col("VITESSE") <= 0) | (pl.col("VITESSE") > 130)
    n_removed = df.select(invalid.sum()).item()

    if n_removed:
        logger.info(f"Removing {n_removed} rows with invalid VITESSE")

    df = df.filter(~invalid)

    return df.rename({"VITESSE": "measure_max_speed"}).with_columns(
        pl.lit(MeasureTypeEnum.SPEEDLIMITATION.value).alias("measure_type_")
    )


def compute_title(df: pl.DataFrame) -> pl.DataFrame:
    """Title from `infobulle`, "Inconnu" when empty."""
    return df.with_columns(
        pl.when(pl.col("infobulle").is_null() | (pl.col("infobulle") == ""))
        .then(pl.lit("Inconnu"))
        .otherwise(pl.col("infobulle"))
        .alias("title")
    )


def compute_start_date(df: pl.DataFrame) -> pl.DataFrame:
    """Permanent period starting on Jan 1st of `annee`, or on `date_modif` when missing."""
    n_missing_annee = df.select(pl.col("annee").is_null().sum()).item()
    if n_missing_annee > 0:
        logger.info(f"Using date_modif as fallback for {n_missing_annee} rows with missing annee")

    # Resolved first so start_of_local_day can read the column's dtype.
    df = df.with_columns(
        pl.when(pl.col("annee").is_not_null())
        .then(pl.col("annee").cast(pl.Int64).cast(pl.Utf8) + pl.lit("-01-01T00:00:00Z"))
        .otherwise(pl.col("date_modif"))
        .alias("_raw_start_date")
    )

    return df.with_columns(
        [
            start_of_local_day(df, "_raw_start_date").alias("period_start_date"),
            pl.lit(None).alias("period_end_date"),
            pl.lit("everyDay").alias("period_recurrence_type"),
            pl.lit(True).alias("period_is_permanent"),
        ]
    ).drop("_raw_start_date")


def compute_location_fields(df: pl.DataFrame) -> pl.DataFrame:
    """rawGeoJSON from `geo_shape` (already GeoJSON); rows without geometry are dropped."""
    n_null_geometry = df.select(pl.col("geo_shape").is_null().sum()).item()
    if n_null_geometry > 0:
        logger.warning(
            f"Dropping {n_null_geometry} rows with null geo_shape (no geometry available)"
        )

    df = df.filter(pl.col("geo_shape").is_not_null())

    return df.with_columns(
        [
            pl.lit(RoadTypeEnum.RAWGEOJSON.value).alias("location_road_type"),
            pl.when(pl.col("loc_txt").is_not_null() & (pl.col("loc_txt") != ""))
            .then(pl.col("loc_txt"))
            .otherwise(pl.col("title"))
            .alias("location_label"),
            pl.col("geo_shape").alias("location_geometry"),
        ]
    )


def compute_vehicle_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Every limit applies to all vehicles."""
    return df.with_columns(
        [
            pl.lit(True).alias("vehicle_all_vehicles"),
            pl.lit(None).cast(pl.Float64).alias("vehicle_heavyweight_max_weight"),
            pl.lit(None).cast(pl.Float64).alias("vehicle_max_height"),
            pl.lit(None).cast(pl.Float64).alias("vehicle_max_width"),
            pl.lit(None).cast(pl.List(pl.Utf8)).alias("vehicle_exempted_types"),
            pl.lit(None).cast(pl.List(pl.Utf8)).alias("vehicle_restricted_types"),
            pl.lit(None).cast(pl.Utf8).alias("vehicle_other_exempted_type_text"),
        ]
    )
