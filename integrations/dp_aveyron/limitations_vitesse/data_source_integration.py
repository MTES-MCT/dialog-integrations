"""Data source integration for Aveyron : limitations-de-vitesse-du-departement"""

import io
from datetime import date

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
from integrations.shared.local_time import start_of_local_day

URL = "https://opendata.aveyron.fr/api/explore/v2.1/catalog/datasets/limitations-de-vitesse-du-departement-aveyron/exports/parquet"


# Ceiling on the locations of one POST. Lyon cuts at 1 000, measured on rawGeoJSON
# stretches of a few dozen metres. A departmental-road location is different: DiaLog
# geocodes it from its milestones, and a stretch here runs for kilometres, so the
# server-side timeout comes far sooner. 100 is the margin chosen by Thibaut on
# 2026-09-17. Only the grouped fallbacks (`AV-LV-V50`, `AV-LV-V90`…) reach it.
MAX_LOCATIONS_PER_REGULATION = 100


class DataSourceIntegration(BaseDataSourceIntegration):
    """Data source for Limitations de vitesse du département de l'Aveyron"""

    raw_data_schema = AveyronLimitationsVitesseRawDataSchema
    name = "limitation_vitesse"

    # R-28: one arrete carries one measure per distinct signature, and that measure
    # carries every stretch it applies to — not one measure per stretch. An arrete
    # groups at most 160 stretches; the grouped fallbacks of the default limits (R-70)
    # carry thousands and are the reason for the ceiling.
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
            # Après les emprises : la liste noire les désigne par leurs points de repère.
            .pipe(discard_refused_segments)
            .pipe(compute_regulation_fields)
            .pipe(compute_vehicle_fields)
            .pipe(compute_split_order)
        )


# Stretches the API refuses to geolocate — 7 of the 1 020 we post, measured on the preprod
# on 2026-09-07 by `ai/tools/probe_refused_segments.py` (102 probes, not versioned). The
# API answers « La géolocalisation de la route entre ces points de repère a échoué » : its
# resolver cannot place these PRs on the departmental road reference system.
#
# Keyed on the stretch itself, because this source carries **no line identifier at all**.
# That is weaker than Lyon's `codetroncon`: if the producer re-cuts a PR, an entry here
# stops matching — it will not block anything, but it will not protect either.
#
# **Do not replace this list with a rule.** The obvious one was tested and fails: three of
# the seven read PR 0+0 → 999+0, which looks like a sentinel, but four do not (D888, D911,
# on plausible PRs) and 17 stretches carrying PR 999 are accepted. Nor are whole roads at
# fault — D888 has 71 emprises of which 2 are refused, D911 44 of which 2.
#
# What it costs to get this wrong: the API validates a regulation as a whole, so these 7
# emprises alone sank 4 regulations and 92 emprises on 2026-09-07 — `AV-LV-V50`
# lost 63 of them by itself.
REFUSED_SEGMENTS: frozenset[str] = frozenset(
    {
        "D1088B1-de-0+0-a-999+0",
        "D888-de-84+468-a-85+945",
        "D888-de-85+945-a-86+286",
        "D911-de-15+52-a-16+242",
        "D911-de-6+636-a-15+52",
        "D920AB1-de-0+0-a-999+0",
        "D920AB2-de-0+0-a-999+0",
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

    The same shape the identifier of a numberless stretch used before R-28, kept because
    it is the only thing that designates a row in a source that ships no key of its own.
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
    """
    Compute all period fields for SavePeriodDTO.
    - period_start_date: the day of the run, at French midnight
    - period_end_date: None
    - period_recurrence_type: everyDay
    - period_is_permanent: True

    The source carries no date column at all — neither the signature of the arrete nor
    the day the limit took effect. It used to be dated 2024-08-12, the day the file was
    last refreshed, which is neither of those: it stated that every limit in the
    department commenced on a day nothing says it did. R-39 — we never presume a past
    date — so the run's own day is used, on the same footing as the dateless rows of
    `restrictions_gabarits`. The regulation being permanent and open-ended, the start
    date only claims "this applies now".

    It goes through `start_of_local_day` so it carries the real French offset of that
    day: DiaLog reads the offset it is given, and a naive `2024-08-12T00:00:00` is read
    as UTC.
    """
    df = df.with_columns(pl.lit(date.today()).alias("_start_date"))
    return df.with_columns(
        [
            start_of_local_day(df, "_start_date").alias("period_start_date"),
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
    """
    Compute all regulation fields for PostApiRegulationsAddBody.
    - regulation_identifier, following R-28:

        AV-LV-{num_arrete}   when the producer gives an arrete number — every stretch
                             citing it becomes an emprise of that arrete
        AV-LV-{V70}          otherwise, one departmental arrete per distinct speed,
                             carrying N emprises

      A source row is not an arrete. Identifying a numberless stretch by the stretch
      itself fabricated one administrative act per section of road: 348 of them on the
      2026-09-07 draw, for four real measures (30, 50, 70 and 110 km/h). An arrete is a
      legal act, and inventing one per stretch is a falsehood that travels all the way
      to the satnavs that rebroadcast us.

      What the fallback key deliberately is *not*: the road, the commune or the stretch.
      Grouping geographically would attach a stretch to a neighbour by heuristic, and
      the first time the producer redraws a trace the identifier moves and the arrete is
      duplicated. Both forms stay under the API's 60-character cap.
    - regulation_category: PERMANENTREGULATION
    - regulation_subject: OTHER
    - regulation_title: arrete number + roads + section count, or the speed and the
      department for a grouped fallback
    - regulation_other_category_text: "Limitation de vitesse"

    Rows with no arrete number are kept, default limits included (R-70, decided by the
    team on 2026-09-14): a 50 km/h in an agglomeration or a 90 outside one is a
    restriction a satnav needs, whether or not a local act is cited. They gather under
    one grouped fallback per speed (`AV-LV-V50`, `AV-LV-V90`…), see the identifier below.

    Everything is then deduplicated on SEGMENT_KEY. A stretch limited to the same
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
    """
    Compute all vehicle fields for SaveVehicleSetDTO.
    - vehicle_all_vehicles: true
    """
    return df.with_columns([pl.lit(True).alias("vehicle_all_vehicles")])
