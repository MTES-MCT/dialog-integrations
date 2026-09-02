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
- only 2 of 194 common identifiers were extended over 16 days. Without an update
  pass, an extension means the restriction is switched off too early — the safe
  direction of the error.

Known gaps, all measured and all deliberate:

- `url_document` and `document_joint` exist and are empty on every row: no link to
  the act, so nothing is sent in `regulation_document_url`;
- `validite` is "A vérifier" on 100 % of rows and `avancement` is "Chantier en cours"
  on 100 % of them, including sites that start in the future — neither can be used
  to filter;
- `Circulation réduite` (a narrowed carriageway, 118 rows on 02/09) has no equivalent
  among DiaLog's five measure types and is dropped under R-32 bis, as is
  `Circulation sens unique` under R-32 for want of a direction column.
"""

import polars as pl
from loguru import logger

from api.dia_log_client.models import (
    MeasureTypeEnum,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
)
from integrations.base_data_source_integration import BaseDataSourceIntegration
from integrations.local_time import end_of_local_day, start_of_local_day
from integrations.shared.wfs import LYON_BBOX, assert_lon_lat_bbox, fetch_wfs_features

from .schema import LyonChantiersPerturbantsRawDataSchema
from .time_slots import parse_time_slots, to_iso_slots

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

# Perturbation types that declare a daily window without giving its hours.
PARTIAL_DAY_MARKERS = ("de jour", "de nuit")

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
        df = fetch_wfs_features(WFS_LAYER)
        assert_lon_lat_bbox(df, LYON_BBOX)
        return df

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
    """Read the daily windows out of the free-text description.

    Two populations, treated differently on purpose:

    - the type says "de jour" / "de nuit", so the restriction is *known* to be
      partial. Publishing it around the clock would state something the source
      contradicts, so a row whose hours cannot be read is **dropped**;
    - the type says nothing, and around the clock is the legitimate default. If the
      description happens to state a window anyway, it is used — publishing 24/7 a
      restriction the source describes as `7h-17h` would be just as wrong.
    """
    clock_slots = pl.Struct({"start": pl.Utf8, "end": pl.Utf8})
    df = df.with_columns(
        pl.col("descripchantierinternet")
        .cast(pl.Utf8)
        .map_elements(
            lambda text: [{"start": start, "end": end} for start, end in parse_time_slots(text)],
            return_dtype=pl.List(clock_slots),
            # An empty description is not a missing answer: it means "no hours
            # stated". Skipping nulls would leave the column null instead of empty,
            # and null is not a length of zero.
            skip_nulls=False,
        )
        .alias("time_slot_clocks")
    )

    declares_partial_day = (
        pl.col("typeperturbation")
        .cast(pl.Utf8)
        .str.contains("|".join(PARTIAL_DAY_MARKERS))
        .fill_null(False)
    )
    has_slots = pl.col("time_slot_clocks").list.len() > 0

    unusable = declares_partial_day & ~has_slots
    n_unusable = df.select(unusable.sum()).item()
    if n_unusable:
        logger.warning(
            f"Dropping {n_unusable} rows restricted to part of the day whose hours are "
            "absent from descripchantierinternet: publishing them around the clock "
            "would overstate the restriction"
        )
    df = df.filter(~unusable)

    n_recovered = df.select((~declares_partial_day & has_slots).sum()).item()
    if n_recovered:
        logger.info(
            f"{n_recovered} rows carry hours in their description without declaring a "
            "day/night type: their time slots are published too"
        )
    n_slots = df.select(has_slots.sum()).item()
    logger.info(f"{n_slots}/{df.height} measures carry daily time slots")

    return df


def compute_period_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Temporary period, from `debutchantier` to `finchantier`, plus its daily slots.

    Drops rows without both dates and rows ending before they start (R-38). Long
    durations are **reported, not dropped**: whether a multi-year "temporary"
    restriction is a data defect or a real one is a case-by-case call, and silently
    discarding it hides the question.
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
    """Raw GeoJSON footprint, as published.

    The layer gives a MultiPolygon footprint, not the road centreline. The API accepts
    it, and it is what the producer publishes. Converting footprints back to segments
    of the road reference system is feasible (the road name matches one of the covered
    streets in 92 % of cases) but it is a separate decision.
    """
    n_missing = df.select(pl.col("geometry").is_null().sum()).item()
    if n_missing:
        logger.warning(f"Dropping {n_missing} rows without geometry")
    df = df.filter(pl.col("geometry").is_not_null())

    label = pl.col("nom").cast(pl.Utf8).fill_null("Voie non précisée")
    label = label + pl.lit(" – ") + pl.col("commune1").cast(pl.Utf8).fill_null("Métropole de Lyon")
    label = (
        pl.when(pl.col("precisionlocalisation").is_not_null())
        .then(label + pl.lit(" (") + pl.col("precisionlocalisation").cast(pl.Utf8) + pl.lit(")"))
        .otherwise(label)
    )

    return df.with_columns(
        [
            pl.lit(RoadTypeEnum.RAWGEOJSON.value).alias("location_road_type"),
            label.alias("location_label"),
            pl.col("geometry").alias("location_geometry"),
        ]
    )


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
    title = pl.col("nomchantier").cast(pl.Utf8).fill_null("Chantier")
    title = title + pl.lit(" – ") + pl.col("nom").cast(pl.Utf8).fill_null("voie non précisée")

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
    """No vehicle column in the layer: the restriction applies to everyone (R-34)."""
    return df.with_columns(pl.lit(True).alias("vehicle_all_vehicles"))
