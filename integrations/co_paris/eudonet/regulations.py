from datetime import date

import polars as pl
from loguru import logger

from api.dia_log_client.models import PeriodRecurrenceTypeEnum, PostApiRegulationsAddBodySubject
from toolkit.rules.rule import Nature, Outcome, Rule
from toolkit.transforms.local_time import end_of_local_day, start_of_local_day
from toolkit.transforms.titles import OTHER_CATEGORY_TEXT_MAX_LENGTH, truncate, truncate_text

from .counts import count_by
from .text import plain_text
from .vocabulary import (
    ACCEPTED_STATES_BY_TYPE,
    PERMANENT_SUBJECT_TEXT,
    REGULATION_CATEGORY_BY_LABEL,
    REGULATION_SUBJECT_BY_REASON,
    SERVICE_POLICE_PREFECTURE,
    TYPE_PERMANENT_LABEL,
    UNKNOWN_REASON_TEXT,
)

IDENTIFIER_PREFIX = "PARIS-EUDO-"

SUBJECT_OTHER = PostApiRegulationsAddBodySubject.OTHER.value

IS_PERMANENT = pl.col("a_type") == TYPE_PERMANENT_LABEL

# "ABROGE_par…", "… / Abrogé par l'arrêté …", "article 1er abrogé" : the act itself, still
# "En vigueur" in Eudonet; not "abrogeant l'arrêté …", nor "l'arrêté n° … abrogé par …".
# Polars has no look-ahead: the participle ends where letters do ("abrogeant" goes on).
_REPEALED = r"abrog[ée]e?s?(?:\P{L}|$)"
_TITLE_SAYS_REPEALED = rf"(?i)(?:^|—|/)\s*{_REPEALED}|\barticle\s+\S+\s+{_REPEALED}"

TITLE_SAYS_REPEALED = Rule(
    code="TITLE_SAYS_REPEALED",
    label="arrêté en vigueur dont le titre dit abrogé",
    nature=Nature.DATA_DEFECT,
    outcome=Outcome.DROP,
    valid=~pl.col("regulation_title").str.contains(_TITLE_SAYS_REPEALED).fill_null(False),
)


def filter_police_prefecture(df: pl.DataFrame, publish: bool) -> pl.DataFrame:
    """Keep or drop the acts of the police prefecture, counted either way."""
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
        "(another authority)"
    )
    return df.filter(~is_prefecture)


def compute_regulation_fields(df: pl.DataFrame) -> pl.DataFrame:
    """The regulation half of the canonical model.

    `1101` « N° d'arrêté » is unique and stable for the life of the act, in both its
    shapes (`2021T113851`, `1996-10345`): used as is, so two acts can never collide.
    """
    return (
        df.pipe(_discard_unnumbered)
        .pipe(_discard_unknown_types)
        .pipe(_discard_out_of_state)
        .pipe(_regulation_columns)
    )


def _discard_unnumbered(df: pl.DataFrame) -> pl.DataFrame:
    unnumbered = df.filter(pl.col("a_identifier").is_null())
    if unnumbered.height:
        logger.warning(
            f"Dropping {unnumbered.height} rows of {unnumbered['a_file_id'].n_unique()} "
            "regulations without an order number (1101): no stable identifier"
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


def _regulation_columns(df: pl.DataFrame) -> pl.DataFrame:
    title = pl.Series(
        "regulation_title",
        [
            truncate_text(
                f"Arrêté n° {identifier} — {cleaned}" if cleaned else f"Arrêté n° {identifier}"
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
            .otherwise(truncate(reason, OTHER_CATEGORY_TEXT_MAX_LENGTH))
            .alias("regulation_other_category_text"),
            # No durable URL for the signed PDF: the Eudonet attachment link is a one-shot
            # token, and the BOVP has no per-act permalink we could rebuild.
            pl.lit(None, dtype=pl.Utf8).alias("regulation_document_url"),
        ]
    )


def compute_period_fields(df: pl.DataFrame, today: date) -> pl.DataFrame:
    """`period_*`, from the dates of the regulation — measures carry none of their own."""
    return (
        df.pipe(_discard_implausible_dates, today)
        .pipe(_warn_undated_permanents)
        .pipe(_period_columns)
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
        f"Dropping {dropped.height} rows of {len(rejected)} regulations failing the "
        f"date controls (start > end, start after {max_start_year}, end after "
        f"{max_end_year}): {rejected[:20]}"
    )
    return df.filter(~pl.col("a_identifier").is_in(rejected))


def _warn_undated_permanents(df: pl.DataFrame) -> pl.DataFrame:
    undated = df.filter(IS_PERMANENT & pl.col("a_start_date").is_null())
    if undated.height:
        logger.warning(
            f"{undated.height} permanent rows have no start date: dated on first write, never "
            "on the signature"
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
