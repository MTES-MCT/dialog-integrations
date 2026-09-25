"""Métropole de Lyon — chantiers perturbants.

The layer publishes the **disruption**, not the administrative act: a work site with
its footprint, its dates and the kind of hindrance it creates. It is a snapshot of
what is going on right now — a site that ends simply disappears from the layer.

Measured over three draws (12/08, 17/08 and 02/09/2026, 995 rows in total), that
lifecycle is well behaved:

- every single row carries both a start and an end date — no exception;
- of 182 sites that vanished over 21 days, **182 had an end date already in the
  past**. The producer never withdraws a site before its declared end, so a
  regulation published here expires on its own, on the right day;
- only 2 of 194 common identifiers were extended over 16 days.

Known gaps, all measured and all deliberate:

- `url_document` and `document_joint` exist and are empty on every row: no link to
  the act, so nothing is sent in `regulation_document_url`;
- `validite` is "A vérifier" on 100 % of rows and `avancement` is "Chantier en cours"
  on 100 % of them, including sites that start in the future — neither can be used
  to filter;
- `Circulation réduite` (a narrowed carriageway) has no equivalent among DiaLog's
  five measure types and is dropped under R-32 bis, as is `Circulation sens unique`
  under R-32 for want of a direction column;
- `descripchantierinternet` is free text that nuances the restriction — hours, "sauf
  riverains", but also a direction, a weekend reopening, 15-minute closures. Only the
  first two can be expressed, so a row is published only when its description is read
  in full (R-78, `description.py`).

Volumes dropped per motive: `ai/docs/vers-l-equipe.md`.
"""

import datetime
import json
from zoneinfo import ZoneInfo

import polars as pl
from loguru import logger

from api.dia_log_client.models import (
    MeasureTypeEnum,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
)
from integrations.base_data_source_integration import BaseDataSourceIntegration
from integrations.co_lyon.grand_lyon import fetch_layer
from integrations.shared.local_time import PARIS, end_of_local_day, start_of_local_day
from integrations.shared.time_slots import to_iso_slots

from .description import RESIDUAL_MAX_CHARS, read_description
from .schema import LyonChantiersPerturbantsRawDataSchema

WFS_LAYER = "pvo_patrimoine_voirie.pvochantierperturbant"

# `typeperturbation` is the only column that says what is actually regulated. The
# vocabulary is closed and stable: the same nine values across all three draws, no
# arrival and no departure. Values absent from this table are counted and dropped,
# never guessed.
MEASURE_TYPE_BY_PERTURBATION = {
    "Circulation interdite": MeasureTypeEnum.NOENTRY.value,
    "Circulation interdite de jour": MeasureTypeEnum.NOENTRY.value,
    "Circulation interdite de nuit": MeasureTypeEnum.NOENTRY.value,
    "Circulation alternée": MeasureTypeEnum.ALTERNATEROAD.value,
    "Circulation alternée de jour": MeasureTypeEnum.ALTERNATEROAD.value,
    # "Circulation réduite*" is a narrowed carriageway: no DiaLog type covers it
    # (R-32 bis). "Circulation sens unique" would need a direction the layer does
    # not carry (R-32). Both are dropped and counted.
}

# Above this, a "temporary" restriction stops looking temporary. It is reported, not
# filtered: the call belongs to whoever reads the report, case by case.
LONG_DURATION_WARNING_DAYS = 365

TITLE_MAX_LENGTH = 255
TITLE_ELLIPSIS = "..."


class DataSourceIntegration(BaseDataSourceIntegration):
    """Disruptive work sites of the Métropole de Lyon."""

    name = "chantiers_perturbants"
    raw_data_schema = LyonChantiersPerturbantsRawDataSchema

    def fetch_raw_data(self) -> pl.DataFrame:
        return fetch_layer(WFS_LAYER)

    def preprocess_raw_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        """Cast the WFS date strings before validation."""
        return raw_data.with_columns(
            [
                pl.col("debutchantier").cast(pl.Utf8).str.to_date("%Y-%m-%d", strict=False),
                pl.col("finchantier").cast(pl.Utf8).str.to_date("%Y-%m-%d", strict=False),
            ]
        )

    def compute_clean_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        return (
            raw_data.pipe(compute_measure_fields)
            .pipe(compute_time_slot_fields)
            .pipe(compute_period_fields)
            .pipe(compute_location_fields)
            .pipe(compute_regulation_fields)
            .pipe(compute_vehicle_fields)
        )


def compute_measure_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Map `typeperturbation` to a DiaLog measure type.

    Produces `measure_type_` and `measure_max_speed` (always null: the layer carries
    no speed). Drops every row whose perturbation has no DiaLog equivalent, counted
    by value.
    """
    df = df.with_columns(
        pl.col("typeperturbation")
        .replace_strict(MEASURE_TYPE_BY_PERTURBATION, default=None, return_dtype=pl.Utf8)
        .alias("measure_type_")
    )

    unmapped = (
        df.filter(pl.col("measure_type_").is_null())
        .get_column("typeperturbation")
        .value_counts(sort=True)
    )
    if unmapped.height:
        counts = dict(zip(unmapped.get_column("typeperturbation"), unmapped.get_column("count")))
        logger.warning(
            f"Dropping {unmapped.get_column('count').sum()} rows without a DiaLog "
            f"measure type: {counts}"
        )
    df = df.filter(pl.col("measure_type_").is_not_null())

    return df.with_columns(pl.lit(None, dtype=pl.Int32).alias("measure_max_speed"))


def compute_time_slot_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Read the description in full, or drop the row (R-78).

    The description is the only place where the producer nuances the restriction, and
    DiaLog can express two of those nuances: daily hours and a resident exemption. A
    row is kept only when nothing else is written there — the rest (a direction, a
    weekend reopening, 15-minute closures…) would otherwise be published as a stronger
    restriction than stated. Four checks, each counted:

    1. the type says "de jour" / "de nuit" but no hours can be read: the restriction
       is *known* to be partial, publishing it around the clock overstates it;
    2. a clock reading is left out of a pair (`de 7h à 17h, réouverture à 18h`): the
       hours were not read in full;
    3. once hours and `sauf riverains` are removed, more than a few connector
       characters remain: the description says something we cannot express;
    4. the type contradicts the hours: "de jour" with a window crossing midnight, or
       "de nuit" with one that does not.

    A type without "de jour" / "de nuit" and no description stays around the clock,
    which is the legitimate default. If such a description states hours anyway, they
    are used: publishing 24/7 a restriction the source describes as `7h-17h` would be
    just as wrong. The hours are assumed to be those of the restriction, not of the
    crew's presence (a work site does not open and close in an instant).
    """
    clock_slots = pl.Struct({"start": pl.Utf8, "end": pl.Utf8})
    reading = pl.Struct(
        {
            "time_slot_clocks": pl.List(clock_slots),
            "hours_fully_paired": pl.Boolean,
            "fully_read": pl.Boolean,
            "crosses_midnight": pl.Boolean,
        }
    )

    def read(text: str | None) -> dict:
        r = read_description(text)
        return {
            "time_slot_clocks": [{"start": start, "end": end} for start, end in r.slots],
            "hours_fully_paired": r.hours_fully_paired,
            "fully_read": r.fully_read,
            "crosses_midnight": r.crosses_midnight,
        }

    df = df.with_columns(
        pl.col("descripchantierinternet")
        .cast(pl.Utf8)
        # An empty description is not a missing answer: it means "no hours stated".
        # Skipping nulls would leave the columns null, and null is not a length of zero.
        .map_elements(read, return_dtype=reading, skip_nulls=False)
        .alias("_reading")
    ).unnest("_reading")

    perturbation = pl.col("typeperturbation").cast(pl.Utf8).fill_null("")
    declares_day = perturbation.str.contains("de jour")
    declares_night = perturbation.str.contains("de nuit")
    has_slots = pl.col("time_slot_clocks").list.len() > 0

    checks = {
        "restricted to part of the day, hours absent from the description": (
            (declares_day | declares_night) & ~has_slots
        ),
        "hours not read in full (a clock reading outside any pair)": ~pl.col("hours_fully_paired"),
        "description says more than hours and a resident exemption "
        f"(direction, weekend, exceptions…; residual > {RESIDUAL_MAX_CHARS} characters)": ~pl.col(
            "fully_read"
        ),
        "type contradicts the hours (day type crossing midnight, or night type not)": (
            (declares_day & pl.col("crosses_midnight"))
            | (declares_night & has_slots & ~pl.col("crosses_midnight"))
        ),
    }
    for reason, unusable in checks.items():
        n_unusable = df.select(unusable.sum()).item()
        if n_unusable:
            logger.warning(
                f"Dropping {n_unusable} rows: {reason}. Publishing them would overstate "
                "the restriction (R-78)"
            )
        df = df.filter(~unusable)

    n_recovered = df.select((~declares_day & ~declares_night & has_slots).sum()).item()
    if n_recovered:
        logger.info(
            f"{n_recovered} rows carry hours in their description without declaring a "
            "day/night type: their time slots are published too"
        )
    n_slots = df.select(has_slots.sum()).item()
    logger.info(f"{n_slots}/{df.height} measures carry daily time slots")

    return df.drop(["hours_fully_paired", "fully_read", "crosses_midnight"])


def compute_period_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Temporary period, from `debutchantier` to `finchantier`, plus its daily slots.

    Drops rows without both dates, rows ending before they start (R-38), and rows
    already over (R-40). Long durations are **reported, not dropped**: whether a
    multi-year "temporary" restriction is a data defect or a real one is a case-by-case
    call, and silently discarding it hides the question.
    """
    missing = pl.col("debutchantier").is_null() | pl.col("finchantier").is_null()
    n_missing = df.select(missing.sum()).item()
    if n_missing:
        logger.warning(f"Dropping {n_missing} rows without both start and end dates")
    df = df.filter(~missing)

    inverted = pl.col("finchantier") < pl.col("debutchantier")
    n_inverted = df.select(inverted.sum()).item()
    if n_inverted:
        logger.warning(f"Dropping {n_inverted} rows ending before they start")
    df = df.filter(~inverted)

    # The producer withdraws a work site from the layer on its end date, so this catches
    # nothing on an ordinary day: it is the guard against a stale row being *created* as
    # a restriction already over (R-40). Kept local to this source on purpose — the
    # other sources have their own withdrawal habits, and a shared rule would only add
    # noise to their logs. A site ending today is still in force until midnight.
    expired = pl.col("finchantier") < today_in_paris()
    n_expired = df.select(expired.sum()).item()
    if n_expired:
        logger.warning(f"Dropping {n_expired} rows whose work site is already over (R-40)")
    df = df.filter(~expired)

    warn_long_durations(df)

    df = df.with_columns(
        pl.struct(["debutchantier", "time_slot_clocks"])
        .map_elements(
            lambda row: to_iso_slots(
                row["debutchantier"],
                [(slot["start"], slot["end"]) for slot in row["time_slot_clocks"] or []],
            ),
            return_dtype=pl.List(pl.Struct({"start_time": pl.Utf8, "end_time": pl.Utf8})),
        )
        .alias("period_time_slots")
    )

    return df.with_columns(
        [
            start_of_local_day(df, "debutchantier").alias("period_start_date"),
            end_of_local_day(df, "finchantier").alias("period_end_date"),
            pl.lit("everyDay").alias("period_recurrence_type"),
            pl.lit(False).alias("period_is_permanent"),
        ]
    )


def today_in_paris() -> datetime.date:
    """The producer's calendar day: the runner may sit in another zone."""
    return datetime.datetime.now(ZoneInfo(PARIS)).date()


def warn_long_durations(df: pl.DataFrame) -> None:
    """Report the work sites that last longer than a restriction plausibly can.

    A multi-year "temporary" restriction is the defect that degraded DiaLog's feed
    quality in the past, so it must never pass unnoticed — but the layer also carries
    genuine multi-year sites, and end dates falling on 31 December that look like a
    default rather than a decision. Both leave through this warning.
    """
    duration = (pl.col("finchantier") - pl.col("debutchantier")).dt.total_days()
    long_running = df.filter(duration > LONG_DURATION_WARNING_DAYS).sort(
        pl.col("finchantier") - pl.col("debutchantier"), descending=True
    )
    if not long_running.height:
        return

    logger.warning(
        f"Very long temporary restriction detected: {long_running.height} work site(s) "
        f"last more than {LONG_DURATION_WARNING_DAYS} days. They are published as they "
        "come — review them case by case."
    )
    for row in long_running.iter_rows(named=True):
        days = (row["finchantier"] - row["debutchantier"]).days
        logger.warning(
            f"  gid {row['gid']}: {days} days, {row['debutchantier']} to "
            f"{row['finchantier']} — {row['nom']} ({row['typeperturbation']})"
        )


def compute_location_fields(df: pl.DataFrame) -> pl.DataFrame:
    """The work site's footprint, published as a DiaLog `zone`.

    The layer gives a MultiPolygon footprint, not the road centreline. Since the
    `zone` road type (API, September 2026), DiaLog does the conversion itself: it
    computes the street segments covered by the polygon and uses them as the effective
    geometry, so what reaches the satnavs is road segments, as everywhere else. A zone
    covering no street is refused by the API (HTTP 400): that row then fails alone,
    since every site is its own regulation.

    `zone` takes a single GeoJSON Polygon. Every footprint drawn so far has exactly one
    part; a multi-part footprint becomes one zone per part, all on the same measure.
    """
    n_missing = df.select(pl.col("geometry").is_null().sum()).item()
    if n_missing:
        logger.warning(f"Dropping {n_missing} rows without geometry")
    df = df.filter(pl.col("geometry").is_not_null())

    df = (
        df.with_columns(
            pl.col("geometry")
            .map_elements(polygons_of, return_dtype=pl.List(pl.Utf8), skip_nulls=False)
            .alias("geometry")
        )
        .explode("geometry")
        .filter(pl.col("geometry").is_not_null())
    )

    label = pl.col("nom").cast(pl.Utf8).str.strip_chars().fill_null("Voie non précisée")
    label = label + pl.lit(" – ") + pl.col("commune1").cast(pl.Utf8).fill_null("Métropole de Lyon")
    label = (
        pl.when(pl.col("precisionlocalisation").is_not_null())
        .then(label + pl.lit(" (") + pl.col("precisionlocalisation").cast(pl.Utf8) + pl.lit(")"))
        .otherwise(label)
    )

    return df.with_columns(
        [
            pl.lit(RoadTypeEnum.ZONE.value).alias("location_road_type"),
            label.alias("location_label"),
            pl.col("geometry").alias("location_geometry"),
        ]
    )


def polygons_of(geometry: str | None) -> list[str] | None:
    """Split a GeoJSON geometry into the Polygons a `zone` accepts, serialized.

    A Polygon is returned as is; a MultiPolygon becomes one Polygon per part. Any
    other geometry type is not a footprint and yields nothing, so the row is dropped.
    """
    if geometry is None:
        return None
    parsed = json.loads(geometry)
    if parsed.get("type") == "Polygon":
        return [geometry]
    if parsed.get("type") == "MultiPolygon":
        return [
            json.dumps({"type": "Polygon", "coordinates": part})
            for part in parsed.get("coordinates", [])
        ]
    logger.warning(f"Dropping a footprint of type {parsed.get('type')}: a zone needs a Polygon")
    return []


def compute_regulation_fields(df: pl.DataFrame) -> pl.DataFrame:
    """One work site = one regulation, carrying one measure.

    **Identifier rule:** `MGL-CHP-{gid}`, where `gid` is the layer's own key, unique
    on every draw and stable over the control window (0 reattribution on 301 common
    identifiers). The commune is deliberately *not* part of the identifier: a site
    whose commune is corrected afterwards would otherwise become a second regulation
    (measured case, file 202607828, Francheville → Tassin la Demi Lune).

    The `MGL-` prefix keeps this source in its own namespace, away from the
    `LYON_…` identifiers the organization already receives from another channel.
    """
    # The producer's free text keeps trailing spaces (« Travaux de branchement  »).
    title = pl.col("nomchantier").cast(pl.Utf8).str.strip_chars().fill_null("Chantier")
    title = (
        title
        + pl.lit(" – ")
        + pl.col("nom").cast(pl.Utf8).str.strip_chars().fill_null("voie non précisée")
    )

    return df.with_columns(
        [
            (pl.lit("MGL-CHP-") + pl.col("gid").cast(pl.Utf8)).alias("regulation_identifier"),
            pl.lit(PostApiRegulationsAddBodyCategory.TEMPORARYREGULATION.value).alias(
                "regulation_category"
            ),
            pl.lit(PostApiRegulationsAddBodySubject.ROADMAINTENANCE.value).alias(
                "regulation_subject"
            ),
            pl.when(title.str.len_chars() > TITLE_MAX_LENGTH)
            .then(
                title.str.slice(0, TITLE_MAX_LENGTH - len(TITLE_ELLIPSIS)) + pl.lit(TITLE_ELLIPSIS)
            )
            .otherwise(title)
            .alias("regulation_title"),
            pl.lit("Chantier perturbant").alias("regulation_other_category_text"),
            # `url_document` is empty on every row today; kept so it flows through the
            # day the producer fills it in.
            pl.col("url_document").cast(pl.Utf8).replace("", None).alias("regulation_document_url"),
        ]
    )


def compute_vehicle_fields(df: pl.DataFrame) -> pl.DataFrame:
    """No vehicle column in the layer: the restriction applies to everyone (R-34) —
    except residents when the description says `Sauf riverains` (R-36, `localResident`).
    """
    exempts_residents = (
        pl.col("descripchantierinternet")
        .cast(pl.Utf8)
        .map_elements(
            lambda text: read_description(text).exempts_residents,
            return_dtype=pl.Boolean,
            skip_nulls=False,
        )
        .fill_null(False)
    )
    n_exempting = df.select(exempts_residents.sum()).item()
    if n_exempting:
        logger.info(f"{n_exempting} rows exempt residents (« sauf riverains »)")
    return df.with_columns(
        [
            pl.lit(True).alias("vehicle_all_vehicles"),
            pl.when(exempts_residents)
            .then(pl.lit(["localResident"]))
            .otherwise(pl.lit(None, dtype=pl.List(pl.Utf8)))
            .alias("vehicle_exempted_types"),
        ]
    )
