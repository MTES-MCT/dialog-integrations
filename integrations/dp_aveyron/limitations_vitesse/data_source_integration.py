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


# Ceiling on the locations of one POST, far below Lyon's 1 000 (rawGeoJSON stretches of a
# few dozen metres): DiaLog geocodes a departmental-road location from its milestones, a
# stretch here runs for kilometres, and the router cuts at 60 s. Only the grouped fallbacks
# (`AV-LV-V50`, `AV-LV-V90`…) reach it. 100 timed out on the staging (D-20); 50 keeps a
# POST well under.
MAX_LOCATIONS_PER_REGULATION = 50


class DataSourceIntegration(BaseDataSourceIntegration):
    """Data source for Limitations de vitesse du département de l'Aveyron"""

    raw_data_schema = AveyronLimitationsVitesseRawDataSchema
    name = "limitation_vitesse"

    # R-28: one arrete carries one measure per distinct signature, and that measure
    # carries every stretch it applies to — not one measure per stretch. The grouped
    # fallbacks of the default limits (R-70) carry thousands, hence the ceiling.
    group_locations_by_measure = True
    max_locations_per_regulation = MAX_LOCATIONS_PER_REGULATION

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
            # Production ignores `direction` (D-19): a limit signposted one way would be
            # broadcast both ways. Remove this line once the bug is fixed.
            .pipe(discard_directional_stretches)
            # After the locations: the blocklist names stretches by their milestones.
            .pipe(discard_refused_segments)
            # Before `compute_regulation_fields` merges the duplicated stretches: the
            # rows fetched count them too, so both sides of the rate do.
            .pipe(self.count_retained_restrictions)
            .pipe(compute_regulation_fields)
            .pipe(compute_vehicle_fields)
            .pipe(compute_split_order)
        )


# Stretches the API refuses to geolocate: « La géolocalisation de la route entre ces points
# de repère a échoué », its resolver cannot place these PRs on the departmental road
# reference system. Found by probing (`ai/tools/probe_refused_segments.py`, not versioned).
# The API validates a regulation as a whole, so one refused stretch sinks the regulation
# and every other emprise it carries.
#
# Keyed on the stretch itself, because this source carries **no line identifier at all**:
# if the producer re-cuts a PR, an entry stops matching — it will not block anything, but
# it will not protect either.
#
# **Do not replace this list with a rule.** The obvious one was tested and fails: three of
# the first seven read PR 0+0 → 999+0, which looks like a sentinel, but four do not (D888,
# D911, on plausible PRs) and 17 stretches carrying PR 999 are accepted. Nor are whole
# roads at fault — D888 has 71 emprises of which 2 are refused, D911 44 of which 2.
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


def discard_directional_stretches(df: pl.DataFrame) -> pl.DataFrame:
    """Drop the stretches whose limit depends on the direction of travel.

    DiaLog's production ignores the `direction` of a numbered-road location (D-19): a
    90 km/h signposted towards increasing PR only would be published in both directions,
    and the D920 at PR 39+719, 90 one way and 50 the other, would come out as two
    contradictory limits both ways. Until the back end reads the direction, only what
    applies both ways is sent. This is a stopgap, not a rule: delete the `.pipe` line in
    `compute_clean_data` to lift it.
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
    """Rank the rows along the road network so a split regulation stays coherent.

    A grouped fallback such as `AV-LV-V90` carries thousands of stretches and is cut into
    slices of `MAX_LOCATIONS_PER_REGULATION`. Sorted by road, then by milestone, each
    slice covers a run of consecutive roads instead of a random sample of the department.
    """
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
    """`D920-de-39+719-a-39+880` — a stretch named by its road and its milestones.

    The only thing that designates a row in a source that ships no key of its own.
    """
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
    """`V70` — the signature of a speed limitation.

    Two stretches share it when they carry the same limit, which is what "the same
    measure" means here. Rows sharing it inside one regulation collapse into a single
    measure carrying N emprises (R-28). Nothing else enters the signature: this source
    holds only `speedLimitation` measures applying to every vehicle, and the direction
    travels on each emprise rather than on the measure.
    """
    return pl.format("V{}", pl.col("measure_max_speed"))


def compute_measure_fields(df: pl.DataFrame):
    """Measure type, speed, and the key that collapses identical speeds into one measure.

    A row with no readable speed is dropped rather than published: a `speedLimitation`
    without a `maxSpeed` states a limit it does not carry, and it has no signature to be
    grouped by. The column is full on every draw observed; the guard is here so a change
    upstream surfaces as a count instead of a null identifier.
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
    """Permanent, every day, no end — and no start date: `period_start_date` is null.

    The source carries no date column at all — neither the signature of the arrete nor
    the day the limit took effect. R-39: no past date is presumed (the file's refresh
    date would claim every limit commenced that day). Nor the day of the run: the date
    is part of what the synchronization compares, so every limit would look modified
    each morning. `integrations/sync/dating.py` resolves the null when DiaLog is
    written: the day of the run on creation ("this applies now"), the date DiaLog
    already holds on update.
    """
    return df.with_columns(
        [
            pl.lit(None, dtype=pl.String).alias("period_start_date"),
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
    A_TO_B of the location we send — checked on 2026-08-31 against the geometries and on
    the ground at the D920, PR 39+719.

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
    """Departmental-road location: `route` 12_D98 -> D98, PR from prd/abd to prf/abf.

    `geo_shape` is not sent: DiaLog geocodes the stretch from its milestones.
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

# Every identifier we create is prefixed, so our batch stays recognisable and removable in
# one go (R-29). The organisation holds only our data today, but that is a circumstance,
# not a rule: the batch that preceded this repository used `{n}/LIMITATION-VITESSE` and had
# to be purged by suffix for want of anything designating it. `AV` is the organisation,
# `LV` the source — the same shape as Lyon's `MGL-CT` and `MGL-CHP`.
IDENTIFIER_PREFIX = "AV-LV"


def normalize_reference(reference: pl.Expr) -> pl.Expr:
    """Reduce a producer reference to what survives a URL path unescaped.

    Identifiers travel in the path of DELETE /api/regulations/{identifier} and of the
    publish endpoint. Aveyron writes "143/2025 Conques" and "A21R0212 - A21R0213", so
    anything that is not a letter or a digit becomes a single hyphen.
    """
    return reference.str.replace_all(r"[^A-Za-z0-9]+", "-").str.strip_chars("-")


def compute_regulation_fields(df: pl.DataFrame):
    """Identifier, title and category of the permanent regulation (R-28).

        AV-LV-{num_arrete}   when the producer gives an arrete number — every stretch
                             citing it becomes an emprise of that arrete
        AV-LV-{V70}          otherwise, one departmental arrete per distinct speed,
                             carrying N emprises

    A source row is not an arrete: identifying a numberless stretch by the stretch itself
    fabricates one legal act per section of road, a falsehood that travels to the satnavs
    that rebroadcast us. Nor does the fallback key carry the road, the commune or the
    stretch: the first time the producer redraws a trace the identifier would move and the
    arrete be duplicated. Both forms stay under the API's 60-character cap.

    Rows with no arrete number are kept, default limits included (R-70): a 50 km/h in an
    agglomeration or a 90 outside one is a restriction a satnav needs.

    Everything is then deduplicated on SEGMENT_KEY (R-75). A stretch limited to the same
    speed in both directions is one measure, not two; a stretch limited differently each
    way stays two measures under one regulation.
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

    # A regulation groups every measure sharing an identifier, and the API keeps the title
    # of the first row only, so both titles are computed over that group.
    roads = pl.col("location_road_number").unique().sort().str.join(", ")
    speeds = pl.col("measure_max_speed").unique().sort().cast(pl.Utf8).str.join(", ")
    # `1 section` reads wrong in French, and a grouped fallback can hold 160 of them:
    # the count is the one thing that tells a reader how wide the arrete is.
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
                # A grouped fallback is named after what it does, because that is all it
                # is: every stretch the department limits to this speed without citing an
                # arrete, gathered into one act. Naming a single stretch would describe
                # one of its N emprises and misdescribe the rest.
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
