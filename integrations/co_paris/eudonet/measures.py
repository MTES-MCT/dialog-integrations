import re
from collections import Counter

import polars as pl
from loguru import logger

from api.dia_log_client.models import MeasureTypeEnum

from .counts import count_by
from .vocabulary import (
    IMPLIED_MAX_SPEED_BY_LABEL,
    KNOWN_PARAMETERS,
    LABEL_PEDESTRIAN_AREA,
    LABEL_SPEED_LIMIT,
    LANE_PARAMETERS,
    MEASURE_TYPE_BY_LABEL,
    MULTI_VALUE_SEPARATOR,
    PEDESTRIAN_AREA_SPEED,
    SPEED_PARAMETER,
    TIME_SLOT_PARAMETERS,
)


def compute_measure_fields(df: pl.DataFrame) -> pl.DataFrame:
    """`measure_type_`, `measure_max_speed`, and `measure_group_key`: the Eudonet measure, so
    its locations collapse into one DiaLog measure."""
    return (
        df.pipe(map_measure_types)
        .pipe(compute_max_speed)
        .pipe(discard_narrowed_measures)
        .pipe(discard_unknown_parameters)
        .with_columns(pl.col("m_file_id").cast(pl.Utf8).alias("measure_group_key"))
    )


def map_measure_types(df: pl.DataFrame) -> pl.DataFrame:
    """`measure_type_`; a label new to catalog `1202` is dropped with a warning until a human
    qualifies it, never guessed and never stopping the run."""
    no_measure = df.filter(pl.col("m_type").is_null())
    if no_measure.height:
        logger.warning(
            f"Dropping {no_measure.height} rows without any measure "
            f"({no_measure['a_identifier'].n_unique()} regulations)"
        )
    df = df.filter(pl.col("m_type").is_not_null())

    known = pl.col("m_type").is_in(list(MEASURE_TYPE_BY_LABEL))
    unknown = df.filter(~known)
    if unknown.height:
        logger.warning(
            f"Dropping {unknown.height} measures with a label unknown to catalog 1202 "
            f"({unknown['a_identifier'].n_unique()} regulations): "
            f"{count_by(unknown, 'm_type')}. "
            "Qualify them in vocabulary.MEASURE_TYPE_BY_LABEL."
        )
    df = df.filter(known).with_columns(
        pl.col("m_type")
        .replace_strict(MEASURE_TYPE_BY_LABEL, return_dtype=pl.Utf8)
        .alias("measure_type_")
    )

    out_of_model = df.filter(pl.col("measure_type_").is_null())
    if out_of_model.height:
        logger.warning(
            f"Dropping {out_of_model.height} measures without a DiaLog equivalent: "
            f"{count_by(out_of_model, 'm_type')}"
        )
    return df.filter(pl.col("measure_type_").is_not_null())


def compute_max_speed(df: pl.DataFrame) -> pl.DataFrame:
    """`measure_max_speed`: implied by the label, or read in the speed parameter. A speed
    limitation whose speed cannot be read is dropped: there is no default speed."""
    speeds = [
        _max_speed(label, params)
        for label, params in zip(df.get_column("m_type"), df.get_column("m_params").to_list())
    ]
    df = df.with_columns(pl.Series("measure_max_speed", speeds, dtype=pl.Int32))

    speedless = (pl.col("m_type") == LABEL_SPEED_LIMIT) & pl.col("measure_max_speed").is_null()
    dropped = df.filter(speedless)
    if dropped.height:
        logger.warning(
            f"Dropping {dropped.height} '{LABEL_SPEED_LIMIT}' measures without a readable "
            f"'{SPEED_PARAMETER}' parameter: there is no default speed"
        )
    return df.filter(~speedless)


def _max_speed(label: str, params: list[list[str]] | None) -> int | None:
    implied = IMPLIED_MAX_SPEED_BY_LABEL.get(label)
    if implied is not None or label != LABEL_SPEED_LIMIT:
        return implied
    for value in measure_parameter_values(params, SPEED_PARAMETER):
        match = re.search(r"(\d+)\s*km/h", value)
        if match:
            return int(match.group(1))
    return None


def discard_narrowed_measures(df: pl.DataFrame) -> pl.DataFrame:
    """Drop the measures a filled time slot or lane narrows: DiaLog would publish them on
    the whole street, every day."""
    narrowings = (
        (TIME_SLOT_PARAMETERS, "whose 'Jours et Horaires' text DiaLog cannot express"),
        (
            LANE_PARAMETERS,
            "narrowed to one lane by 'voie concernée', which DiaLog would apply "
            "to the whole street",
        ),
    )
    for names, motive in narrowings:
        narrowed = has_filled_parameter(df, names)
        dropped = df.filter(narrowed)
        if dropped.height:
            logger.warning(
                f"Dropping {dropped.height} rows of {dropped['m_file_id'].n_unique()} measures "
                f"{motive}: {count_by(dropped, 'm_type')}"
            )
        df = df.filter(~narrowed)
    return df


def discard_unknown_parameters(df: pl.DataFrame) -> pl.DataFrame:
    """Drop a measure with a filled parameter of an unknown name: it may narrow the measure
    like a time slot or a lane does."""
    names = [
        [
            pair[0].strip()
            for pair in params or []
            if len(pair) == 2 and pair[0].strip() not in KNOWN_PARAMETERS and pair[1].strip()
        ]
        for params in df.get_column("m_params").to_list()
    ]
    unknown = pl.Series([bool(found) for found in names], dtype=pl.Boolean)
    dropped = df.filter(unknown)
    if dropped.height:
        counted = Counter(name for found in names for name in found)
        logger.warning(
            f"Dropping {dropped.height} rows of {dropped['m_file_id'].n_unique()} measures "
            f"carrying a filled parameter of an unknown name, which may narrow them: "
            f"{dict(counted)}. Qualify it in vocabulary.py."
        )
    return df.filter(~unknown)


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
    """Give every published pedestrian area its walking-pace limit, in the same regulation.

    Code de la route R. 110-2: no exemption on the limit, as the vehicles let in still
    drive at walking pace.
    """
    bans = df.filter(pl.col("m_type") == LABEL_PEDESTRIAN_AREA)
    if not bans.height:
        return df
    speeds = bans.with_columns(
        pl.lit(MeasureTypeEnum.SPEEDLIMITATION.value).alias("measure_type_"),
        pl.lit(PEDESTRIAN_AREA_SPEED, dtype=df.schema["measure_max_speed"]).alias(
            "measure_max_speed"
        ),
        (pl.col("measure_group_key") + pl.lit(f"-V{PEDESTRIAN_AREA_SPEED}")).alias(
            "measure_group_key"
        ),
        pl.lit(None, dtype=pl.List(pl.Utf8)).alias("vehicle_exempted_types"),
        pl.lit(None, dtype=pl.Utf8).alias("vehicle_other_exempted_type_text"),
    )
    logger.info(
        f"{bans['m_file_id'].n_unique()} pedestrian-area bans get their "
        f"{PEDESTRIAN_AREA_SPEED} km/h twin ({speeds.height} locations)"
    )
    return pl.concat([df, speeds], how="vertical")
