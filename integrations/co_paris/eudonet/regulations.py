import re
from datetime import date

import polars as pl
from loguru import logger

from api.dia_log_client.models import PeriodRecurrenceTypeEnum, PostApiRegulationsAddBodySubject
from toolkit.transforms.local_time import end_of_local_day, start_of_local_day
from toolkit.transforms.titles import OTHER_CATEGORY_TEXT_MAX_LENGTH, truncate, truncate_text

from .text import plain_text
from .verdicts import (
    IMPLAUSIBLE_DATES,
    NO_NUMBER,
    OUT_OF_STATE,
    UNKNOWN_REGULATION_TYPE,
    among,
    refusal,
)
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
# The participle ends where letters do ("abrogeant" goes on).
_REPEALED = r"abrog[ée]e?s?(?:[\W\d_]|$)"
TITLE_SAYS_REPEALED = re.compile(
    rf"(?:^|—|/)\s*{_REPEALED}|\barticle\s+\S+\s+{_REPEALED}", re.IGNORECASE
)

# A title that names a day, an hour or a market: the act holds at those times only, which no
# field carries ("une aire piétonne les dimanches", "à certaines heures"). Not "km/heure",
# nor a street ("rue du Marché Saint-Honoré").
TITLE_NAMES_A_TIME = re.compile(
    r"\b(lundi|mardi|mercredi|jeudi|vendredi|samedi|dimanche)s?\b|jours? fériés?"
    r"|certaines heures|(?<!km/)\bheures?\b|\bnuits?\b"
    r"|marchés? (alimentaires?|découverts?|aux puces)|tenue (de|des) marchés?"
    r"|abords (immédiats )?(du|des) marchés?",
    re.IGNORECASE,
)
# Catalog keys (`1103`, `1122`) of operations that hold on some days only.
TIME_LIMITED_KEYS = frozenset({"PARIS RESPIRE", "MARCHES"})


# A title that makes the act hold under a condition no field carries ("durant les épisodes de
# pollution atmosphérique", "en cas d'épisode neigeux"): published, it would hold at all times.
# Checked before the title formulas, which could take the condition in (decided 2026-11-19).
TITLE_NAMES_A_CONDITION = re.compile(
    r"\bpollution|\bépisodes?\b|\ben cas d|\blorsque\b|\bintempéries|\bneige|\bverglas"
    r"|\bcrues?\b|\bcanicule",
    re.IGNORECASE,
)


def title_condition(title: str | None) -> str | None:
    match = TITLE_NAMES_A_CONDITION.search(plain_text(title))
    return match.group(0).lower() if match else None


def title_says_repealed(title: str | None) -> bool:
    return bool(TITLE_SAYS_REPEALED.search(plain_text(title)))


def title_names_a_time(title: str | None) -> bool:
    return bool(TITLE_NAMES_A_TIME.search(plain_text(title)))


CITY = "city"
PREFECTURE = "prefecture"


def keep_authority(df: pl.DataFrame, authority: str | None) -> pl.DataFrame:
    """The acts one authority signed (`CITY`, `PREFECTURE`), or all of them for None.

    Applied last: every act of the perimeter is read and judged together, the citations
    crossing from one authority to the other."""
    if authority is None:
        return df
    is_prefecture = (pl.col("a_service") == SERVICE_POLICE_PREFECTURE).fill_null(False)
    return df.filter(is_prefecture if authority == PREFECTURE else ~is_prefecture)


def judge_regulation_fields(df: pl.DataFrame) -> pl.DataFrame:
    """The regulation half of the canonical model, and the acts outside it: no number, a type
    unknown to the catalog (never guessed: it decides how the periods are built), a state out
    of scope.

    `1101` « N° d'arrêté » is unique and stable for the life of the act, in both its
    shapes (`2021T113851`, `1996-10345`): used as is, so two acts can never collide.
    """
    known_type = pl.col("a_type").is_in(list(REGULATION_CATEGORY_BY_LABEL)).fill_null(False)
    kept_state = pl.any_horizontal(
        ((pl.col("a_type") == type_label) & pl.col("a_state").is_in(list(states))).fill_null(False)
        for type_label, states in ACCEPTED_STATES_BY_TYPE.items()
    )
    return df.pipe(_regulation_columns).with_columns(
        refusal(pl.when(pl.col("a_identifier").is_null()).then(pl.lit("no 1101"))).alias(
            NO_NUMBER.column
        ),
        refusal(pl.when(~known_type).then(pl.col("a_type").fill_null("(empty)"))).alias(
            UNKNOWN_REGULATION_TYPE.column
        ),
        refusal(pl.when(~kept_state).then(pl.col("a_state").fill_null("(empty)"))).alias(
            OUT_OF_STATE.column
        ),
    )


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
            .replace_strict(REGULATION_CATEGORY_BY_LABEL, default=None, return_dtype=pl.Utf8)
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


def judge_period_fields(df: pl.DataFrame, alive: pl.Series, today: date) -> pl.DataFrame:
    """`period_*`, from the dates of the regulation (measures carry none of their own), and the
    acts whose dates are not plausible.

    A typo on the year leaves the act "En vigueur" in Eudonet until that year comes
    (`2019T15796` runs to 2109), so the state alone cannot be trusted. Judged by number, the
    DiaLog identifier, on the acts still alive."""
    max_start_year = today.year + 1
    max_end_year = today.year + 5
    invalid = (
        (pl.col("a_start_date") > pl.col("a_end_date"))
        | (pl.col("a_start_date").dt.year() > max_start_year)
        | (pl.col("a_end_date").dt.year() > max_end_year)
    ).fill_null(False)
    rejected = df.filter(invalid & pl.lit(alive)).get_column("a_identifier").unique().to_list()
    why = f"start > end, start after {max_start_year}, end after {max_end_year}"
    return (
        df.pipe(_warn_undated_permanents, alive)
        .pipe(_period_columns)
        .with_columns(
            among(
                alive, refusal(pl.when(pl.col("a_identifier").is_in(rejected)).then(pl.lit(why)))
            ).alias(IMPLAUSIBLE_DATES.column)
        )
    )


def _warn_undated_permanents(df: pl.DataFrame, alive: pl.Series) -> pl.DataFrame:
    undated = df.filter(IS_PERMANENT & pl.col("a_start_date").is_null() & pl.lit(alive))
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
