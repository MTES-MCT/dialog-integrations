import re

import polars as pl

from toolkit.rules.vehicles import pedestrians_or_cycles_named
from toolkit.transforms import pedestrian_area
from toolkit.transforms.pedestrian_area import PEDESTRIAN_AREA_SPEED

from .free_text import read_text
from .verdicts import (
    LANE_PARAMETER,
    NO_MEASURE,
    OUT_OF_MODEL,
    SPEED_UNREADABLE,
    TIME_SLOT,
    UNEXPECTED_OPENING,
    UNKNOWN_MEASURE_LABEL,
    UNKNOWN_OPTION_FORMULA,
    UNKNOWN_PARAMETER,
    refusal,
    refusals,
)
from .vocabulary import (
    IMPLIED_MAX_SPEED_BY_LABEL,
    KNOWN_OPTION_FORMULAS,
    KNOWN_PARAMETERS,
    LABEL_PEDESTRIAN_AREA,
    LABEL_SPEED_LIMIT,
    LANE_PARAMETERS,
    MEASURE_TYPE_BY_LABEL,
    MULTI_VALUE_SEPARATOR,
    OPENING_BY_LABEL,
    PEDESTRIAN_AREA_SPEED_BY_KEY,
    SPEED_PARAMETER,
    TIME_SLOT_PARAMETERS,
)

PEDESTRIAN_OR_CYCLE_MEASURES = pedestrians_or_cycles_named(pl.col("m_type"))


def judge_measure_fields(df: pl.DataFrame) -> pl.DataFrame:
    """`measure_type_`, `measure_max_speed`, and `measure_group_key`: the Eudonet measure, so
    its locations collapse into one DiaLog measure. And the measures DiaLog cannot take as
    they are written.

    A measure DiaLog does not publish keeps `measure_type_` empty: its texts are read and it
    may change the act's other measures, until `judge_out_of_model`. So does a label new to
    catalog `1202`, read as a free text (`_unread_label`)."""
    known = pl.col("m_type").is_in(list(MEASURE_TYPE_BY_LABEL)).fill_null(False)
    df = df.with_columns(
        pl.col("m_type")
        .replace_strict(MEASURE_TYPE_BY_LABEL, default=None, return_dtype=pl.Utf8)
        .alias("measure_type_"),
        pl.Series(
            "measure_max_speed",
            [
                _max_speed(label, params)
                for label, params in zip(df["m_type"], df["m_params"].to_list())
            ],
            dtype=pl.Int32,
        ),
    )
    expected = pl.col("m_type").replace_strict(OPENING_BY_LABEL, default=None, return_dtype=pl.Utf8)
    opening = pl.col("m_opening").str.strip_chars()
    unexpected_opening = (
        _PUBLISHED & opening.is_not_null() & (opening != "") & (opening != expected).fill_null(True)
    )
    speedless = (pl.col("m_type") == LABEL_SPEED_LIMIT) & pl.col("measure_max_speed").is_null()
    published = df.select(_PUBLISHED.fill_null(False)).to_series()
    labels = df.select(pl.col("m_type"), known.alias("_known")).iter_rows()
    return df.with_columns(
        refusal(pl.when(pl.col("m_type").is_null()).then(pl.lit("no measure"))).alias(
            NO_MEASURE.column
        ),
        refusals(
            [None if label is None or known else _unread_label(label) for label, known in labels]
        ).alias(UNKNOWN_MEASURE_LABEL.column),
        refusal(pl.when(unexpected_opening).then(pl.col("m_opening").str.strip_chars())).alias(
            UNEXPECTED_OPENING.column
        ),
        refusals(_unknown_option_formulas(df, published)).alias(UNKNOWN_OPTION_FORMULA.column),
        refusal(pl.when(speedless).then(pl.lit("no speed"))).alias(SPEED_UNREADABLE.column),
        refusal(
            pl.when(pl.lit(has_filled_parameter(df, TIME_SLOT_PARAMETERS)) & _PUBLISHED).then(
                pl.lit("Jours et Horaires")
            )
        ).alias(TIME_SLOT.column),
        refusal(
            pl.when(pl.lit(has_filled_parameter(df, LANE_PARAMETERS)) & _PUBLISHED).then(
                pl.lit("voie concernée")
            )
        ).alias(LANE_PARAMETER.column),
        refusals(_unknown_parameters(df, published)).alias(UNKNOWN_PARAMETER.column),
        pl.col("m_file_id").cast(pl.Utf8).alias("measure_group_key"),
    )


_PUBLISHED = pl.col("measure_type_").is_not_null()


def judge_out_of_model(df: pl.DataFrame) -> pl.DataFrame:
    """Refuse the measures without a DiaLog equivalent, once read: last of the measure rules."""
    return df.with_columns(
        refusal(pl.when(~_PUBLISHED).then(pl.col("m_type"))).alias(OUT_OF_MODEL.column)
    )


def _unread_label(label: str) -> str | None:
    """A label new to catalog `1202` is a free text (decided 2026-11-19): understood and saying
    nothing, its measure is one DiaLog does not publish; otherwise what it does to the act's
    other measures is not known, and the act is refused."""
    reading = read_text(label)
    return None if reading.understood and not reading.effects else label


def _unknown_option_formulas(df: pl.DataFrame, published: pl.Series) -> list[str | None]:
    """The formulas the mail merge prints before an optional parameter that no catalog value
    is: they may say the opposite of the known ones."""
    found = []
    for row, is_published in zip(df["m_option_formulas"].to_list(), published):
        unknown = [
            formula.strip()
            for formula, known in zip(row or [], KNOWN_OPTION_FORMULAS, strict=False)
            if formula.strip() not in {"", *known}
        ]
        found.append(" | ".join(unknown) if unknown and is_published else None)
    return found


def _unknown_parameters(df: pl.DataFrame, published: pl.Series) -> list[str | None]:
    """The filled parameters of an unknown name: they may narrow the measure like a time slot
    or a lane does."""
    found = []
    for params, is_published in zip(df["m_params"].to_list(), published):
        names = [
            pair[0].strip()
            for pair in params or []
            if len(pair) == 2 and pair[0].strip() not in KNOWN_PARAMETERS and pair[1].strip()
        ]
        found.append(", ".join(names) if names and is_published else None)
    return found


def _max_speed(label: str, params: list[list[str]] | None) -> int | None:
    implied = IMPLIED_MAX_SPEED_BY_LABEL.get(label)
    if implied is not None or label != LABEL_SPEED_LIMIT:
        return implied
    for value in measure_parameter_values(params, SPEED_PARAMETER):
        match = re.search(r"(\d+)\s*km/h", value)
        if match:
            return int(match.group(1))
    return None


def measure_parameter_values(params: list[list[str]] | None, wanted: str) -> list[str]:
    """The non-empty values of a named parameter, already split on the "; " separator."""
    values: list[str] = []
    for pair in params or []:
        if len(pair) != 2 or pair[0].strip() != wanted:
            continue
        values.extend(part.strip() for part in pair[1].split(MULTI_VALUE_SEPARATOR) if part.strip())
    return values


def has_filled_parameter(df: pl.DataFrame, names: frozenset[str]) -> pl.Series:
    return pl.Series(
        [
            any(
                len(pair) == 2 and pair[0].strip() in names and pair[1].strip()
                for pair in params or []
            )
            for params in df.get_column("m_params").to_list()
        ],
        dtype=pl.Boolean,
    )


def add_pedestrian_area_speed(df: pl.DataFrame) -> pl.DataFrame:
    speed = pl.coalesce(
        *(
            pl.col(key).replace_strict(PEDESTRIAN_AREA_SPEED_BY_KEY, default=None)
            for key in ("a_key_1", "a_key_2")
        ),
        pl.lit(PEDESTRIAN_AREA_SPEED),
    )
    return pedestrian_area.add_pedestrian_area_speed(
        df,
        is_ban=pl.col("m_type") == LABEL_PEDESTRIAN_AREA,
        twin_group_key=pl.col("measure_group_key") + pl.lit("-V") + speed.cast(pl.String),
        speed=speed,
    )
