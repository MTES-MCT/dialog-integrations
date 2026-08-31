"""Data source integration for Aveyron : limitations-de-vitesse-du-departement"""

import io

import polars as pl
import requests
from loguru import logger

from api.dia_log_client.models import (
    DirectionEnum,
    MeasureTypeEnum,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
)
from integrations.base_data_source_integration import BaseDataSourceIntegration
from integrations.dp_aveyron.limitations_vitesse.schema import (
    AveyronLimitationsVitesseRawDataSchema,
)

URL = "https://opendata.aveyron.fr/api/explore/v2.1/catalog/datasets/limitations-de-vitesse-du-departement-aveyron/exports/parquet"


class DataSourceIntegration(BaseDataSourceIntegration):
    """Data source for Limitations de vitesse du département de l'Aveyron"""

    raw_data_schema = AveyronLimitationsVitesseRawDataSchema
    name = "limitation_vitesse"

    def fetch_raw_data(self):
        logger.info(f"Downloading data from {URL}")

        r = requests.get(URL)
        r.raise_for_status()

        df = pl.read_parquet(io.BytesIO(r.content))
        return df

    def preprocess_raw_data(self, raw_data):
        """Drop rows whose milestones are not numeric.

        The export carries a stray header line ("Début" in prd). Left in place it makes the
        schema coercion fail, and with it the whole integration.
        """
        numeric = pl.all_horizontal(
            [
                pl.col(c).cast(pl.Float64, strict=False).is_not_null()
                for c in ("prd", "prf", "abd", "abf")
            ]
        )
        n_dropped = raw_data.select((~numeric).sum()).item()
        if n_dropped > 0:
            logger.warning(f"Dropping {n_dropped}/{raw_data.height} rows with a non-numeric PR")
        return raw_data.filter(numeric)

    def compute_clean_data(self, raw_data):
        return (
            raw_data.pipe(compute_measure_fields)
            .pipe(compute_period_fields)
            .pipe(compute_location_fields)
            .pipe(compute_regulation_fields)
            .pipe(compute_vehicle_fields)
        )


def compute_measure_fields(df: pl.DataFrame):
    return df.with_columns(
        [
            pl.lit(MeasureTypeEnum.SPEEDLIMITATION.value).alias("measure_type_"),
            pl.col("limit").alias("measure_max_speed"),
        ]
    )


def compute_period_fields(df: pl.DataFrame):
    """
    Compute all period fields for SavePeriodDTO.
    - period_start_date: 2024-08-12, last update of the file.
    - period_end_date / : None
    - period_recurrence_type: everyDay
    - period_is_permanent: True
    """
    return df.with_columns(
        [
            pl.lit("2024-08-12T00:00:00").alias("period_start_date"),
            pl.lit(None).alias("period_end_date"),
            pl.lit("everyDay").alias("period_recurrence_type"),
            pl.lit(True).alias("period_is_permanent"),
        ]
    )


# One measure per stretch of road and speed limit. `cote` is deliberately absent: it holds
# the direction of travel, which becomes `location_direction` rather than part of the key,
# so a stretch limited alike both ways is one measure and not two.
SEGMENT_KEY = [
    "location_road_number",
    "location_from_point_number",
    "location_from_abscissa",
    "location_to_point_number",
    "location_to_abscissa",
    "measure_max_speed",
]


def compute_direction() -> pl.Expr:
    """Read the direction a limit applies to from the sides it is signposted on.

    `cote` is the side of the roadway the sign stands on, so it names the traffic it
    faces: `droite` is the right-hand side going towards increasing PR, which is the
    A_TO_B of the location we send. Checked two ways on 2026-08-31: the `droite` geometry
    lies to the right of A_TO_B on 2681 of 2684 stretches (median offset 10.2 m), and the
    D920 at PR 39+719, limited to 90 on `droite` and 50 on `gauche`, does carry its 50
    from B to A on the ground.

    A stretch signposted on both sides applies both ways. `centre` and a missing side fall
    back to BOTH, which overstates the restriction rather than pointing it the wrong way.
    """
    signposted_right = (pl.col("cote") == "droite").any().over(SEGMENT_KEY)
    signposted_left = (pl.col("cote") == "gauche").any().over(SEGMENT_KEY)
    return (
        pl.when(signposted_right & signposted_left)
        .then(pl.lit(DirectionEnum.BOTH.value))
        .when(signposted_right)
        .then(pl.lit(DirectionEnum.A_TO_B.value))
        .when(signposted_left)
        .then(pl.lit(DirectionEnum.B_TO_A.value))
        .otherwise(pl.lit(DirectionEnum.BOTH.value))
    )


def compute_location_fields(df: pl.DataFrame):
    """
    Compute all location fields for SaveLocationDTO.
    - location_administrator: "Aveyron"
    - location_road_type: RoadTypeEnum.DEPARTMENTALROAD
    - location_road_number: from route, e.g. 12_D98 -> D98
    - location_from_department_code: 12
    - location_from_point_number: from prd
    - location_from_abscissa: from abd
    - location_from_side: "U"
    - location_to_department_code: 12
    - location_to_point_number: from prf
    - location_to_abscissa: from abf
    - location_to_side: "U"
    - location_direction: from cote, see compute_direction
    #NOT TRANSMITTTED- location_geometry: from geo_shape
    """

    return df.with_columns(
        [
            pl.lit("Aveyron").alias("location_administrator"),
            pl.lit(RoadTypeEnum.DEPARTMENTALROAD.value).alias("location_road_type"),
            pl.col("route").str.split("_").list.last().alias("location_road_number"),
            pl.lit("12").alias("location_from_department_code"),
            pl.col("prd").cast(pl.Utf8).alias("location_from_point_number"),
            pl.col("abd").round().cast(pl.Int64).alias("location_from_abscissa"),
            pl.lit("U").alias("location_from_side"),
            pl.lit("12").alias("location_to_department_code"),
            pl.col("prf").cast(pl.Utf8).alias("location_to_point_number"),
            pl.col("abf").round().cast(pl.Int64).alias("location_to_abscissa"),
            pl.lit("U").alias("location_to_side"),
        ]
    ).with_columns(compute_direction().alias("location_direction"))


MAX_IDENTIFIER_LENGTH = 60  # API contract


def normalize_reference(reference: pl.Expr) -> pl.Expr:
    """Reduce a producer reference to what survives a URL path unescaped.

    Identifiers travel in the path of DELETE /api/regulations/{identifier} and of the
    publish endpoint. Aveyron writes "143/2025 Conques" and "A21R0212 - A21R0213", so
    anything that is not a letter or a digit becomes a single hyphen.
    """
    return reference.str.replace_all(r"[^A-Za-z0-9]+", "-").str.strip_chars("-")


def compute_regulation_fields(df: pl.DataFrame):
    """
    Compute all regulation fields for PostApiRegulationsAddBody.
    - regulation_identifier: "lim-vitesse-{num_arrete}" when the producer gives an arrete
      number. It leaves that column empty on most rows, so the rest is identified by the
      stretch itself, "lim-vitesse-{road}-de-{fromPR}+{fromAbscissa}-a-{toPR}+{toAbscissa}"
      The speed is deliberately out of the key:
      Aveyron changing a limit must update the regulation, not orphan it and create a
      second one. Both forms stay under the API's 60-character cap.
    - regulation_category: PERMANENTREGULATION
    - regulation_subject: OTHER
    - regulation_title: arrete number + roads + section count, or the stretch itself
    - regulation_other_category_text: "Limitation de vitesse"

    Rows with no arrete number carrying a nationwide default are dropped, because they
    restate the law instead of a local decision: 50 km/h inside an agglomeration and
    90 km/h outside one. The test is per row, so a 90 inside an agglomeration or a 50
    outside one is kept.

    Everything left is then deduplicated on SEGMENT_KEY. A stretch limited to the same
    speed in both directions is one measure, not two; a stretch limited differently each
    way stays two measures under one regulation.
    """
    df = df.with_columns(normalize_reference(pl.col("num_arrete")).alias("num_arrete"))

    has_arrete = pl.col("num_arrete").is_not_null() & (pl.col("num_arrete") != "")
    # fill_null keeps a row with no readable speed instead of letting a null predicate
    # drop it silently: it must surface, not vanish.
    is_default_limit = (
        ((pl.col("measure_max_speed") == 50) & pl.col("agglo").is_not_null())
        | ((pl.col("measure_max_speed") == 90) & pl.col("agglo").is_null())
    ).fill_null(False)

    n_default = df.select((~has_arrete & is_default_limit).sum()).item()
    if n_default > 0:
        logger.warning(
            f"Dropping {n_default}/{df.height} rows with no num_arrete carrying a default "
            f"limit (50 km/h in agglomeration, 90 km/h outside)"
        )
    df = df.filter(has_arrete | ~is_default_limit)

    # Keep the row carrying an arrete number when the same measure appears with and without,
    # and order the rest so the survivor does not depend on the order of the export.
    n_duplicates = df.height - df.unique(subset=SEGMENT_KEY).height
    if n_duplicates > 0:
        logger.warning(f"Merging {n_duplicates} rows describing an already covered measure")
    df = (
        df.with_columns((~has_arrete).alias("_no_arrete"))
        .sort(["_no_arrete", "num_arrete"], nulls_last=True)
        .unique(subset=SEGMENT_KEY, keep="first", maintain_order=True)
        .drop("_no_arrete")
    )

    stretch = (
        pl.col("location_road_number")
        + pl.lit("-de-")
        + pl.col("location_from_point_number")
        + pl.lit("+")
        + pl.col("location_from_abscissa").cast(pl.Utf8)
        + pl.lit("-a-")
        + pl.col("location_to_point_number")
        + pl.lit("+")
        + pl.col("location_to_abscissa").cast(pl.Utf8)
    )
    df = df.with_columns(
        (
            pl.lit("lim-vitesse-")
            + pl.when(has_arrete).then(pl.col("num_arrete")).otherwise(stretch)
        ).alias("regulation_identifier")
    )

    n_too_long = df.select(
        (pl.col("regulation_identifier").str.len_chars() > MAX_IDENTIFIER_LENGTH).sum()
    ).item()
    if n_too_long > 0:
        logger.error(f"{n_too_long} identifiers exceed {MAX_IDENTIFIER_LENGTH} characters")

    # A regulation groups every measure sharing an identifier, and the API keeps the title
    # of the first row only, so both titles are computed over that group.
    roads = pl.col("location_road_number").unique().sort().str.join(", ")
    sections = pl.len().cast(pl.Utf8)
    speeds = pl.col("measure_max_speed").unique().sort().cast(pl.Utf8).str.join(", ")

    return df.with_columns(
        [
            pl.lit(PostApiRegulationsAddBodyCategory.PERMANENTREGULATION.value).alias(
                "regulation_category"
            ),
            pl.lit(PostApiRegulationsAddBodySubject.OTHER.value).alias("regulation_subject"),
            pl.when(has_arrete)
            .then(
                pl.col("num_arrete")
                + pl.lit(" - Limitation de vitesse - ")
                + roads.over("regulation_identifier")
                + pl.lit(" (")
                + sections.over("regulation_identifier")
                + pl.lit(" sections)")
            )
            .otherwise(
                pl.lit("Limitation de vitesse ")
                + speeds.over("regulation_identifier")
                + pl.lit(" km/h - ")
                + pl.col("location_road_number")
                + pl.lit(", du PR ")
                + pl.col("location_from_point_number")
                + pl.lit("+")
                + pl.col("location_from_abscissa").cast(pl.Utf8)
                + pl.lit(" au PR ")
                + pl.col("location_to_point_number")
                + pl.lit("+")
                + pl.col("location_to_abscissa").cast(pl.Utf8)
            )
            .alias("regulation_title"),
            pl.lit("Limitation de vitesse").alias("regulation_other_category_text"),
        ]
    )


def compute_vehicle_fields(df: pl.DataFrame):
    """
    Compute all vehicle fields for SaveVehicleSetDTO.
    - vehicle_all_vehicles: true
    """
    return df.with_columns([pl.lit(True).alias("vehicle_all_vehicles")])
