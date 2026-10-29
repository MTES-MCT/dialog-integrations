"""Métropole de Lyon — chantiers perturbants: the disruption, not the administrative act.

The layer is a snapshot: a site that ends disappears from it. Over three draws (12/08,
17/08 and 02/09/2026, 995 rows), every row had both dates and all 182 vanished sites
were past their end date, so a regulation published here expires on its own, on the
right day. `validite` and `avancement` hold one value on 100 % of rows: neither filters.
"""

import json

import polars as pl
from loguru import logger

from api.dia_log_client.models import (
    MeasureTypeEnum,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
)
from canonical.source import BaseDataSourceIntegration
from integrations.co_lyon.grand_lyon import fetch_layer
from toolkit.rules.period import DATES_MISSING, EXPIRED
from toolkit.transforms.local_time import end_of_local_day, start_of_local_day
from toolkit.transforms.time_slots import to_iso_slots

from .description import RESIDUAL_MAX_CHARS, read_description
from .schema import LyonChantiersPerturbantsRawDataSchema

WFS_LAYER = "pvo_patrimoine_voirie.pvochantierperturbant"

# Closed vocabulary, the same nine values over three draws; any other value is counted and
# dropped, never guessed.
MEASURE_TYPE_BY_PERTURBATION = {
    "Circulation interdite": MeasureTypeEnum.NOENTRY.value,
    "Circulation interdite de jour": MeasureTypeEnum.NOENTRY.value,
    "Circulation interdite de nuit": MeasureTypeEnum.NOENTRY.value,
    "Circulation alternée": MeasureTypeEnum.ALTERNATEROAD.value,
    "Circulation alternée de jour": MeasureTypeEnum.ALTERNATEROAD.value,
    "Circulation alternée de nuit": MeasureTypeEnum.ALTERNATEROAD.value,
    # Dropped: "Circulation réduite*" has no DiaLog type, "Circulation sens
    # unique" needs a direction the layer lacks.
}

# Above this, a "temporary" restriction stops looking temporary. It is reported, not
# filtered: the call belongs to whoever reads the report, case by case.
LONG_DURATION_WARNING_DAYS = 365

TITLE_MAX_LENGTH = 255
TITLE_ELLIPSIS = "..."


class DataSourceIntegration(BaseDataSourceIntegration):
    """Disruptive work sites of the Métropole de Lyon."""

    name = "chantiers_perturbants"
    identifier_prefix = "MGL-CHP-"
    raw_data_schema = LyonChantiersPerturbantsRawDataSchema
    # The producer withdraws a site on its end date: EXPIRED only guards against a stale row.
    rules = (EXPIRED, DATES_MISSING)

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
    """Map `typeperturbation` to a DiaLog measure type, dropping and counting the rest."""
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
    """Read the description in full, or drop the row.

    Hours read in full are published whatever the type says: "de jour" / "de nuit" only
    tells that hours exist, and a night slot need not cross midnight (decided 2026-10-10).
    Hours on a type without either are used too, as 24/7 would be just as wrong. They are
    taken as the restriction's hours, not the crew's.
    """
    clock_slots = pl.Struct({"start": pl.Utf8, "end": pl.Utf8})
    reading = pl.Struct(
        {
            "time_slot_clocks": pl.List(clock_slots),
            "hours_fully_paired": pl.Boolean,
            "fully_read": pl.Boolean,
        }
    )

    def read(text: str | None) -> dict:
        r = read_description(text)
        return {
            "time_slot_clocks": [{"start": start, "end": end} for start, end in r.slots],
            "hours_fully_paired": r.hours_fully_paired,
            "fully_read": r.fully_read,
        }

    df = df.with_columns(
        pl.col("descripchantierinternet")
        .cast(pl.Utf8)
        # A null description means "no hours stated", not an unknown: read it as empty.
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
    }
    for reason, unusable in checks.items():
        n_unusable = df.select(unusable.sum()).item()
        if n_unusable:
            logger.warning(
                f"Dropping {n_unusable} rows: {reason}. Publishing them would overstate "
                "the restriction"
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

    return df.drop(["hours_fully_paired", "fully_read"])


def compute_period_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Temporary period from `debutchantier` to `finchantier`, with its daily slots.

    Long durations are reported, not dropped: whether a multi-year "temporary" restriction
    is a defect or real is a case-by-case call.
    """
    # A missing date is left to the rules, applied to the canonical rows.
    inverted = (pl.col("finchantier") < pl.col("debutchantier")).fill_null(False)
    n_inverted = df.select(inverted.sum()).item()
    if n_inverted:
        logger.warning(f"Dropping {n_inverted} rows ending before they start")
    df = df.filter(~inverted)

    warn_long_durations(df)

    df = df.with_columns(
        pl.struct(["debutchantier", "time_slot_clocks"])
        .map_elements(
            lambda row: (
                to_iso_slots(
                    row["debutchantier"],
                    [(slot["start"], slot["end"]) for slot in row["time_slot_clocks"] or []],
                )
                if row["debutchantier"] is not None
                else []
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

    Multi-year "temporary" restrictions have degraded DiaLog's feed before, but the layer
    also has genuine ones, and 31 December end dates that look like a default.
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
    """The work site's footprint, published as a DiaLog `zone`, one per Polygon part.

    DiaLog turns it into the street segments it covers. A zone covering no street is
    refused (HTTP 400) and fails alone, each site being its own regulation.
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
    """Split a GeoJSON geometry into the serialized Polygons a `zone` accepts.

    Any other geometry type is not a footprint and yields nothing, so the row is dropped.
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
    """One work site = one regulation `MGL-CHP-{gid}`, carrying one measure.

    `gid` held over the control window (0 reattribution on 301 common identifiers). The
    commune is left out: a corrected commune would make a second regulation (file
    202607828, Francheville → Tassin la Demi Lune).
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
    """Every vehicle, except residents when the description says so."""
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
