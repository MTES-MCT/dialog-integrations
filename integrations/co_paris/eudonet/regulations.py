from datetime import date

import polars as pl
from loguru import logger

from api.dia_log_client.models import PeriodRecurrenceTypeEnum, PostApiRegulationsAddBodySubject
from integrations.shared.local_time import end_of_local_day, start_of_local_day

from .counts import count_by
from .text import plain_text, truncate
from .vocabulary import (
    ACCEPTED_STATES_BY_TYPE,
    PERMANENT_SUBJECT_TEXT,
    REGULATION_CATEGORY_BY_LABEL,
    REGULATION_SUBJECT_BY_REASON,
    SERVICE_POLICE_PREFECTURE,
    TYPE_PERMANENT_LABEL,
    TYPE_TEMPORARY_LABEL,
    UNKNOWN_REASON_TEXT,
)

IDENTIFIER_PREFIX = "PARIS-EUDO-"

TITLE_MAX_LENGTH = 255
OTHER_CATEGORY_TEXT_MAX_LENGTH = 100
SUBJECT_OTHER = PostApiRegulationsAddBodySubject.OTHER.value

IS_PERMANENT = pl.col("a_type") == TYPE_PERMANENT_LABEL
IS_TEMPORARY = pl.col("a_type") == TYPE_TEMPORARY_LABEL


def filter_police_prefecture(df: pl.DataFrame, publish: bool) -> pl.DataFrame:
    """Keep or drop the acts of the police prefecture (R-73, Q-36).

    Counted either way, so the report always carries the share of the prefecture.
    """
    is_prefecture = (pl.col("a_service") == SERVICE_POLICE_PREFECTURE).fill_null(False)
    prefecture = df.filter(is_prefecture)
    if not prefecture.height:
        return df
    regulations = prefecture["a_identifier"].n_unique()
    if publish:
        logger.info(
            f"Keeping {prefecture.height} rows of {regulations} police prefecture regulations "
            "(PUBLISH_POLICE_PREFECTURE, never on the production host)"
        )
        return df
    logger.warning(
        f"Dropping {prefecture.height} rows of {regulations} police prefecture regulations "
        "(R-73: another authority)"
    )
    return df.filter(~is_prefecture)


def compute_regulation_fields(df: pl.DataFrame, today: date) -> pl.DataFrame:
    """The regulation half of the pivot, and the perimeter filter that goes with it.

    The perimeter is filtered again here: the Eudonet query already did it on the day of
    the extraction, but a frozen extract replayed on a later day has to lose the acts that
    ended since.

    `1101` « N° d'arrêté » is Eudonet's own business key (R-23): unique across the base and
    stable across the life of the act, in both its shapes (`2021T113851` since 2017,
    `1996-10345` before). It is used as is, with no normalisation that could make two
    distinct acts collide.
    """
    return (
        df.pipe(_discard_unnumbered)
        .pipe(_discard_unknown_types)
        .pipe(_discard_out_of_state)
        .pipe(_discard_unusable_temporaries, today)
        .pipe(_regulation_columns)
    )


def _discard_unnumbered(df: pl.DataFrame) -> pl.DataFrame:
    unnumbered = df.filter(pl.col("a_identifier").is_null())
    if unnumbered.height:
        logger.warning(
            f"Dropping {unnumbered.height} rows of {unnumbered['a_file_id'].n_unique()} "
            "regulations without an order number (1101): no stable identifier (R-01)"
        )
    return df.filter(pl.col("a_identifier").is_not_null())


def _discard_unknown_types(df: pl.DataFrame) -> pl.DataFrame:
    """The category decides how the periods are built: an unknown type is never guessed."""
    known = pl.col("a_type").is_in(list(REGULATION_CATEGORY_BY_LABEL)).fill_null(False)
    unknown = df.filter(~known)
    if unknown.height:
        logger.warning(
            f"Dropping {unknown.height} rows of {unknown['a_identifier'].n_unique()} "
            f"regulations with a type unknown to catalog 1108: {count_by(unknown, 'a_type')}. "
            "Qualify it in vocabulary.REGULATION_CATEGORY_BY_LABEL."
        )
    return df.filter(known)


def _discard_out_of_state(df: pl.DataFrame) -> pl.DataFrame:
    kept_state = pl.any_horizontal(
        ((pl.col("a_type") == type_label) & pl.col("a_state").is_in(list(states))).fill_null(False)
        for type_label, states in ACCEPTED_STATES_BY_TYPE.items()
    )
    dropped = df.filter(~kept_state)
    if dropped.height:
        logger.warning(
            f"Dropping {dropped.height} rows whose regulation state is out of scope: "
            f"{count_by(dropped, 'a_state')}"
        )
    return df.filter(kept_state)


def _discard_unusable_temporaries(df: pl.DataFrame, today: date) -> pl.DataFrame:
    expired = df.filter(IS_TEMPORARY & (pl.col("a_end_date") < today).fill_null(False))
    if expired.height:
        logger.warning(
            f"Dropping {expired.height} rows of {expired['a_identifier'].n_unique()} temporary "
            f"regulations that ended before {today}"
        )
    undated = df.filter(IS_TEMPORARY & pl.col("a_end_date").is_null())
    if undated.height:
        logger.warning(
            f"Dropping {undated.height} rows of {undated['a_identifier'].n_unique()} temporary "
            "regulations without an end date: nothing would ever close them (R-37)"
        )
    unstarted = df.filter(IS_TEMPORARY & pl.col("a_start_date").is_null())
    if unstarted.height:
        logger.warning(
            f"Dropping {unstarted.height} rows of {unstarted['a_identifier'].n_unique()} "
            "temporary regulations without a start date: the write date would stand in (R-37)"
        )
    return df.filter(
        ~IS_TEMPORARY
        | ((pl.col("a_end_date") >= today) & pl.col("a_start_date").is_not_null()).fill_null(False)
    )


def _regulation_columns(df: pl.DataFrame) -> pl.DataFrame:
    title = pl.Series(
        "regulation_title",
        [
            truncate(
                f"Arrêté n° {identifier} — {cleaned}" if cleaned else f"Arrêté n° {identifier}",
                TITLE_MAX_LENGTH,
            )
            for identifier, cleaned in zip(
                df.get_column("a_identifier"),
                (plain_text(raw) for raw in df.get_column("a_title_html")),
            )
        ],
        dtype=pl.Utf8,
    )
    reason = pl.col("a_reason").fill_null("").str.strip_chars()
    mapped_subject = reason.replace_strict(
        REGULATION_SUBJECT_BY_REASON, default=None, return_dtype=pl.Utf8
    )
    return df.with_columns(
        [
            (pl.lit(IDENTIFIER_PREFIX) + pl.col("a_identifier")).alias("regulation_identifier"),
            title,
            pl.col("a_type")
            .replace_strict(REGULATION_CATEGORY_BY_LABEL, return_dtype=pl.Utf8)
            .alias("regulation_category"),
            pl.when(IS_PERMANENT)
            .then(pl.lit(SUBJECT_OTHER))
            .otherwise(mapped_subject.fill_null(SUBJECT_OTHER))
            .alias("regulation_subject"),
            pl.when(IS_PERMANENT)
            .then(pl.lit(PERMANENT_SUBJECT_TEXT))
            .when(reason == "")
            .then(pl.lit(UNKNOWN_REASON_TEXT))
            .otherwise(reason.str.slice(0, OTHER_CATEGORY_TEXT_MAX_LENGTH))
            .alias("regulation_other_category_text"),
            # No durable URL for the signed PDF: the Eudonet attachment link is a one-shot
            # token, and the BOVP has no per-act permalink we could rebuild.
            pl.lit(None, dtype=pl.Utf8).alias("regulation_document_url"),
        ]
    )


def compute_period_fields(df: pl.DataFrame, today: date) -> pl.DataFrame:
    """`period_*`, from the dates of the regulation — measures carry none of their own."""
    return (
        df.pipe(_discard_implausible_dates, today).pipe(_warn_long_or_undated).pipe(_period_columns)
    )


def _discard_implausible_dates(df: pl.DataFrame, today: date) -> pl.DataFrame:
    """A typo on the year leaves the act "En vigueur" in Eudonet until that year comes
    (`2019T15796` runs to 2109), so the state alone cannot be trusted."""
    max_start_year = today.year + 1
    max_end_year = today.year + 5
    invalid = (
        (pl.col("a_start_date") > pl.col("a_end_date"))
        | (pl.col("a_start_date").dt.year() > max_start_year)
        | (pl.col("a_end_date").dt.year() > max_end_year)
    ).fill_null(False)
    rejected = df.filter(invalid).get_column("a_identifier").unique().sort().to_list()
    if not rejected:
        return df
    dropped = df.filter(pl.col("a_identifier").is_in(rejected))
    logger.warning(
        f"Dropping {dropped.height} rows of {len(rejected)} regulations failing the R-38 "
        f"date controls (start > end, start after {max_start_year}, end after "
        f"{max_end_year}): {rejected[:20]}"
    )
    return df.filter(~pl.col("a_identifier").is_in(rejected))


def _warn_long_or_undated(df: pl.DataFrame) -> pl.DataFrame:
    long_running = df.filter(
        ~IS_PERMANENT
        & ((pl.col("a_end_date") - pl.col("a_start_date")).dt.total_days() > 365).fill_null(False)
    )
    if long_running.height:
        logger.warning(
            f"{long_running['a_identifier'].n_unique()} temporary regulations last more than a "
            f"year ({long_running.height} rows): kept, but they widen the broadcast window"
        )
    undated = df.filter(IS_PERMANENT & pl.col("a_start_date").is_null())
    if undated.height:
        logger.warning(
            f"{undated.height} permanent rows have no start date: dated on first write, never "
            "on the signature (R-39)"
        )
    return df


def _period_columns(df: pl.DataFrame) -> pl.DataFrame:
    if not df.height:
        return df.with_columns(
            [
                pl.lit(None, dtype=pl.Utf8).alias("period_start_date"),
                pl.lit(None, dtype=pl.Utf8).alias("period_end_date"),
                pl.lit(None, dtype=pl.Utf8).alias("period_recurrence_type"),
                pl.lit(None, dtype=pl.Boolean).alias("period_is_permanent"),
            ]
        )
    return df.with_columns(
        [
            start_of_local_day(df, "a_start_date").alias("period_start_date"),
            pl.when(IS_PERMANENT)
            .then(pl.lit(None, dtype=pl.Utf8))
            .otherwise(end_of_local_day(df, "a_end_date"))
            .alias("period_end_date"),
            pl.lit(PeriodRecurrenceTypeEnum.EVERYDAY.value).alias("period_recurrence_type"),
            IS_PERMANENT.alias("period_is_permanent"),
        ]
    )
