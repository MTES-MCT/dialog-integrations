"""Aveyron: limitations-de-vitesse-du-departement."""

import polars as pl
from loguru import logger

from api.dia_log_client.models import (
    DirectionEnum,
    MeasureTypeEnum,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
)
from canonical.source import BaseDataSourceIntegration
from integrations.dp_aveyron.limitations_vitesse.schema import (
    AveyronLimitationsVitesseRawDataSchema,
)
from toolkit.connectors import opendatasoft

URL = "https://opendata.aveyron.fr/api/explore/v2.1/catalog/datasets/limitations-de-vitesse-du-departement-aveyron/exports/parquet"


# DiaLog geocodes each milestone stretch (kilometres long) and the router cuts at 60 s:
# 100 locations per POST timed out on the staging, 50 stays well under.
MAX_LOCATIONS_PER_REGULATION = 50


class DataSourceIntegration(BaseDataSourceIntegration):
    raw_data_schema = AveyronLimitationsVitesseRawDataSchema
    name = "limitation_vitesse"
    identifier_prefix = "AV-LV-"

    # R-28: one measure per signature, carrying every stretch. The grouped fallbacks of the
    # default limits (R-70) carry thousands, hence the ceiling.
    group_locations_by_measure = True
    max_locations_per_regulation = MAX_LOCATIONS_PER_REGULATION

    def fetch_raw_data(self) -> pl.DataFrame:
        return opendatasoft.read_export(URL)

    def preprocess_raw_data(self, raw_data):
        """Drop rows whose milestones are not numeric.

        The export carries a stray header line ("Début" in prd) that would fail the schema
        coercion, and with it the whole integration.
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
            # Before the direction is read: a side left with no conflicting limit must
            # stand alone, not keep the "both ways" its discarded twin gave it (D840).
            .pipe(discard_vehicle_class_limits)
            .pipe(compute_location_fields)
            # Production ignores `direction`: a one-way limit would be broadcast both ways.
            # Remove this line once the back end reads it.
            .pipe(discard_directional_stretches)
            # After the locations: the blocklist names stretches by their milestones.
            .pipe(discard_refused_segments)
            # Before the rows are grouped into regulations.
            .pipe(self.apply_rules)
            # Before duplicates are merged: the rows fetched count them too.
            .pipe(self.count_retained_restrictions)
            .pipe(compute_regulation_fields)
            .pipe(compute_vehicle_fields)
            .pipe(compute_split_order)
        )


# Stretches the API refuses to geolocate, found by probing. One refused stretch sinks its
# whole regulation. Keyed on milestones since the source has no line identifier.
# Not a rule: PR 999 is accepted on 17 other stretches, and D888/D911 refuse 2 of 71 and 44.
REFUSED_SEGMENTS: frozenset[str] = frozenset(
    {
        # 2026-09-07
        "D1088B1-de-0+0-a-999+0",
        "D888-de-84+468-a-85+945",
        "D888-de-85+945-a-86+286",
        "D911-de-15+52-a-16+242",
        "D911-de-6+636-a-15+52",
        "D920AB1-de-0+0-a-999+0",
        "D920AB2-de-0+0-a-999+0",
        # 2026-09-18
        "D259-de-1+235-a-2+0",
        "D259-de-2+0-a-999+0",
        "D33-de-18+878-a-18+1017",
        "D568-de-1+924-a-2+239",
    }
)


# A vehicle-class limit (`50 PL>12t`, 62 of 5 814 rows on 2026-09-22) makes "every vehicle at
# 90" false and names no usable tonnage (R-35), so the stretch is dropped. A condition
# (`70 chaussee mouillee`, 11 rows) leaves the general limit true: kept (decided 2026-09-22).
# Deliberately wider than the values observed.
VEHICLE_CLASS_PATTERN = r"(?i)\bPL\b|poids.lourd|camion|autocar|\bbus\b|\bcar\b|>\s*\d+\s*t\b"


def discard_vehicle_class_limits(df: pl.DataFrame) -> pl.DataFrame:
    """Drop the stretches whose `limit_spec` sets another limit for a vehicle class."""
    spec = pl.col("limit_spec").fill_null("").str.strip_chars()
    specified = spec != ""
    conflicting = specified & spec.str.contains(VEHICLE_CLASS_PATTERN)

    n_conflicting = df.select(conflicting.sum()).item()
    if n_conflicting:
        values = sorted(df.filter(conflicting)["limit_spec"].unique().to_list())
        logger.warning(
            f"Discarding {n_conflicting}/{df.height} stretches whose limit differs for a "
            f"vehicle class we cannot publish (R-35): {values}"
        )
    n_conditional = df.select((specified & ~conflicting).sum()).item()
    if n_conditional:
        values = sorted(df.filter(specified & ~conflicting)["limit_spec"].unique().to_list())
        logger.info(
            f"Keeping {n_conditional} stretches under a condition DiaLog cannot express; "
            f"their general limit is published alone: {values}"
        )
    return df.filter(~conflicting)


def discard_directional_stretches(df: pl.DataFrame) -> pl.DataFrame:
    """Drop the stretches whose limit depends on the direction of travel.

    A stopgap: DiaLog's production ignores the direction of a numbered-road location, so the
    D920 at PR 39+719 (90 one way, 50 the other) would get two contradictory limits both ways.
    """
    both_ways = pl.col("location_direction") == DirectionEnum.BOTH.value
    n_directional = df.select((~both_ways).sum()).item()
    if n_directional:
        logger.warning(
            f"Discarding {n_directional}/{df.height} stretches whose limit depends on the "
            "direction: production ignores it (D-19)"
        )
    return df.filter(both_ways)


def compute_split_order(df: pl.DataFrame) -> pl.DataFrame:
    """Rank the rows by road and milestone, so each slice of a split regulation covers
    consecutive roads rather than a random sample of the department."""
    return (
        df.sort(
            [
                "location_road_number",
                pl.col("location_from_point_number").cast(pl.Int64, strict=False),
                "location_from_abscissa",
            ],
            nulls_last=True,
        )
        .with_row_index("regulation_split_order")
        .with_columns(pl.col("regulation_split_order").cast(pl.Int64))
    )


def segment_key() -> pl.Expr:
    """`D920-de-39+719-a-39+880`: the only key of a row, the source ships none."""
    return (
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


def discard_refused_segments(df: pl.DataFrame):
    """Drop the stretches the API cannot geolocate, before they sink their regulation."""
    refused = segment_key().is_in(list(REFUSED_SEGMENTS))
    n_refused = df.select(refused.sum()).item()
    if n_refused:
        logger.info(
            f"Discarding {n_refused} stretches the API cannot geolocate "
            f"(blocklist of {len(REFUSED_SEGMENTS)} entries, measured on the preprod)"
        )
    return df.filter(~refused)


def measure_group_key() -> pl.Expr:
    """`V70`: rows sharing it in one regulation collapse into one measure (R-28).

    The speed is enough: every measure here applies to all vehicles, and the direction
    travels on each location.
    """
    return pl.format("V{}", pl.col("measure_max_speed"))


def compute_measure_fields(df: pl.DataFrame):
    """Measure type, speed and group key; a row with no readable speed is dropped.

    The column has always been full: the guard turns an upstream change into a count rather
    than a null identifier.
    """
    df = df.with_columns(
        [
            pl.lit(MeasureTypeEnum.SPEEDLIMITATION.value).alias("measure_type_"),
            pl.col("limit").alias("measure_max_speed"),
        ]
    )

    speechless = pl.col("measure_max_speed").is_null()
    n_speechless = df.select(speechless.sum()).item()
    if n_speechless > 0:
        logger.warning(f"Dropping {n_speechless}/{df.height} rows with no readable speed")
    df = df.filter(~speechless)

    return df.with_columns(measure_group_key().alias("measure_group_key"))


def compute_period_fields(df: pl.DataFrame):
    """Permanent, every day, no end, and a null start date.

    The source has no date (R-39), and the day of the run would make every limit look
    modified each morning. `sync/dating.py` fills the null when DiaLog is written.
    """
    return df.with_columns(
        [
            pl.lit(None, dtype=pl.String).alias("period_start_date"),
            pl.lit(None).alias("period_end_date"),
            pl.lit("everyDay").alias("period_recurrence_type"),
            pl.lit(True).alias("period_is_permanent"),
        ]
    )


# `cote` is left out: it becomes `location_direction`, so a stretch limited alike both ways
# is one measure, not two.
SEGMENT_KEY = [
    "location_road_number",
    "location_from_point_number",
    "location_from_abscissa",
    "location_to_point_number",
    "location_to_abscissa",
    "measure_max_speed",
]


def compute_direction() -> pl.Expr:
    """Read the direction from the sides the limit is signposted on.

    `droite` faces traffic towards increasing PR, our A_TO_B (checked 2026-08-31 on the
    D920, PR 39+719). `centre` or no side gives BOTH: overstated rather than misdirected.
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
    """Departmental-road location; `geo_shape` is not sent, DiaLog geocodes the milestones."""

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

# Prefixed so our batch stays recognisable and removable in one go (R-29).
IDENTIFIER_PREFIX = "AV-LV"


def normalize_reference(reference: pl.Expr) -> pl.Expr:
    """Reduce a producer reference ("143/2025 Conques") to what survives a URL path.

    Identifiers travel in the path of the DELETE and publish endpoints.
    """
    return reference.str.replace_all(r"[^A-Za-z0-9]+", "-").str.strip_chars("-")


def compute_regulation_fields(df: pl.DataFrame):
    """Identifier, title and category of the permanent regulation (R-28).

    `AV-LV-{num_arrete}`, or `AV-LV-{V70}`: one grouped arrete per speed for the rows citing
    none (R-70). Identifying by stretch would invent one legal act per section and move with
    every redrawn trace. Rows are deduplicated on SEGMENT_KEY (R-75).
    """
    df = df.with_columns(normalize_reference(pl.col("num_arrete")).alias("num_arrete"))

    has_arrete = pl.col("num_arrete").is_not_null() & (pl.col("num_arrete") != "")
    n_no_arrete = df.select((~has_arrete).sum()).item()
    if n_no_arrete > 0:
        logger.info(
            f"{n_no_arrete}/{df.height} rows cite no num_arrete: gathered into one grouped "
            "fallback per speed (R-70)"
        )

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

    df = df.with_columns(
        (
            pl.lit(f"{IDENTIFIER_PREFIX}-")
            + pl.when(has_arrete).then(pl.col("num_arrete")).otherwise(pl.col("measure_group_key"))
        ).alias("regulation_identifier")
    )

    n_too_long = df.select(
        (pl.col("regulation_identifier").str.len_chars() > MAX_IDENTIFIER_LENGTH).sum()
    ).item()
    if n_too_long > 0:
        logger.error(f"{n_too_long} identifiers exceed {MAX_IDENTIFIER_LENGTH} characters")

    # The API keeps the title of the first row only, so titles are computed per regulation.
    roads = pl.col("location_road_number").unique().sort().str.join(", ")
    speeds = pl.col("measure_max_speed").unique().sort().cast(pl.Utf8).str.join(", ")
    # The count tells a reader how wide the arrete is: a fallback can hold 160 sections.
    sections = pl.format(
        "{} section{}", pl.len(), pl.when(pl.len() > 1).then(pl.lit("s")).otherwise(pl.lit(""))
    )

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
                + pl.lit(")")
            )
            .otherwise(
                # Naming one stretch would misdescribe the N others of the fallback.
                pl.lit("Limitation de vitesse ")
                + speeds.over("regulation_identifier")
                + pl.lit(" km/h - Département de l'Aveyron (")
                + sections.over("regulation_identifier")
                + pl.lit(")")
            )
            .alias("regulation_title"),
            pl.lit("Limitation de vitesse").alias("regulation_other_category_text"),
        ]
    )


def compute_vehicle_fields(df: pl.DataFrame):
    return df.with_columns([pl.lit(True).alias("vehicle_all_vehicles")])
