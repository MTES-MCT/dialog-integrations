from collections import Counter
from dataclasses import dataclass, field
from typing import Any

import polars as pl
from loguru import logger

from api.dia_log_client.models import (
    PostApiRegulationsAddBodyMeasuresItemVehicleSetType0ExemptedTypesType0Item as VehicleExemptedTypeEnum,  # noqa: E501
)
from toolkit.rules.vehicles import pedestrians_or_cycles_named
from toolkit.transforms.titles import truncate_text

from .vocabulary import (
    ALL_VEHICLES_LABELS,
    EXEMPTED_OTHER,
    EXEMPTION_BY_LABEL,
    EXEMPTION_PARAMETERS,
    LABEL_CATEGORY_LIMIT,
    LABEL_DIMENSION_LIMIT,
    LABEL_PEDESTRIAN_AREA,
    MULTI_VALUE_SEPARATOR,
    RESTRICTED_OTHER,
    RESTRICTION_BY_LABEL,
    RESTRICTION_PARAMETERS,
    RESTRICTIONS_KEPT_AS_OTHER,
)

# `otherRestrictedTypeText` / `otherExemptedTypeText` have no documented limit; capped on
# the longest string the API is known to accept (`title`) rather than left unbounded.
OTHER_TYPE_TEXT_MAX_LENGTH = 255
FREE_TEXT_SEPARATOR = " ; "

VEHICLE_COLUMNS: dict[str, Any] = {
    "vehicle_all_vehicles": pl.Boolean,
    "vehicle_restricted_types": pl.List(pl.Utf8),
    "vehicle_exempted_types": pl.List(pl.Utf8),
    "vehicle_heavyweight_max_weight": pl.Float64,
    # No catalog value carries a length, height or width: always empty.
    "vehicle_max_length": pl.Float64,
    "vehicle_max_height": pl.Float64,
    "vehicle_max_width": pl.Float64,
    "vehicle_other_restricted_type_text": pl.Utf8,
    "vehicle_other_exempted_type_text": pl.Utf8,
}

# Why a measure is dropped.
OUTSIDE_THE_CLOSED_LIST = "outside"


@dataclass
class VehicleTally:
    """Values counted on every row, for the warnings."""

    kept_as_other: Counter[str] = field(default_factory=Counter)
    unknown_exemptions: Counter[str] = field(default_factory=Counter)
    outside_restrictions: Counter[str] = field(default_factory=Counter)


def read_vehicle_set(
    label: str, params: list[list[str]] | None, tally: VehicleTally
) -> tuple[dict[str, Any], str | None]:
    """The vehicle columns of one measure, and why it is dropped (or None)."""
    restricted: list[str] = []
    exempted: list[str] = []
    restricted_texts: list[str] = []
    exempted_texts: list[str] = []
    weight = None
    outside = False
    kept_as_other: list[str] = []
    unknown_exemptions: list[str] = []

    for pair in params or []:
        if len(pair) != 2:
            continue
        name = pair[0].strip()
        if name not in RESTRICTION_PARAMETERS and name not in EXEMPTION_PARAMETERS:
            continue
        for part in pair[1].split(MULTI_VALUE_SEPARATOR):
            value = part.strip()
            if not value:
                continue
            if name in RESTRICTION_PARAMETERS:
                if value in ALL_VEHICLES_LABELS:
                    continue
                known = RESTRICTION_BY_LABEL.get(value)
                if known is not None:
                    _append_unique(restricted, known.restricted_type)
                    weight = _keep_stricter(weight, known.heavyweight_max_weight)
                elif value in RESTRICTIONS_KEPT_AS_OTHER:
                    kept_as_other.append(value)
                    _append_unique(restricted, RESTRICTED_OTHER)
                    _append_unique(restricted_texts, value)
                else:
                    tally.outside_restrictions[value] += 1
                    outside = True
                continue
            known_exemption = EXEMPTION_BY_LABEL.get(value)
            if known_exemption is None:
                unknown_exemptions.append(value)
                _append_unique(exempted, EXEMPTED_OTHER)
                _append_unique(exempted_texts, value)
            else:
                _append_unique(exempted, known_exemption)

    rejection = None
    if outside:
        rejection = OUTSIDE_THE_CLOSED_LIST
    else:
        tally.kept_as_other.update(kept_as_other)
        tally.unknown_exemptions.update(unknown_exemptions)
    if label == LABEL_PEDESTRIAN_AREA:
        # Only the vehicles serving the area may enter.
        exempted.insert(0, VehicleExemptedTypeEnum.DESSERTELOCALE.value)

    columns = {
        # A gauge or category limit never targets every vehicle, readable or not.
        "vehicle_all_vehicles": not restricted
        and label not in (LABEL_DIMENSION_LIMIT, LABEL_CATEGORY_LIMIT),
        "vehicle_restricted_types": restricted or None,
        "vehicle_exempted_types": exempted or None,
        "vehicle_heavyweight_max_weight": weight,
        "vehicle_max_length": None,
        "vehicle_max_height": None,
        "vehicle_max_width": None,
        "vehicle_other_restricted_type_text": _free_text(restricted_texts)
        if RESTRICTED_OTHER in restricted
        else None,
        "vehicle_other_exempted_type_text": _free_text(exempted_texts)
        if EXEMPTED_OTHER in exempted
        else None,
    }
    return columns, rejection


def compute_vehicle_fields(df: pl.DataFrame) -> pl.DataFrame:
    """`vehicleSet`, read from the named parameters of the measure.

    Drops a measure restricting a value outside the closed list.
    """
    tally = VehicleTally()
    read = [
        read_vehicle_set(label, params, tally)
        for label, params in zip(df.get_column("m_type"), df.get_column("m_params").to_list())
    ]
    vehicles = pl.DataFrame([columns for columns, _ in read], schema=VEHICLE_COLUMNS)
    rejection = pl.Series("vehicle_rejection", [why for _, why in read], dtype=pl.Utf8)
    df = df.with_columns(vehicles.get_columns()).with_columns(rejection)

    if tally.kept_as_other:
        logger.warning(
            f"{sum(tally.kept_as_other.values())} restriction values of the closed list kept "
            f"as 'other' + free text: {dict(tally.kept_as_other)}"
        )
    if tally.unknown_exemptions:
        logger.warning(
            f"{sum(tally.unknown_exemptions.values())} exemption values outside the DiaLog "
            f"enumeration, kept as 'other' + free text: {dict(tally.unknown_exemptions)}"
        )
    motives = {
        OUTSIDE_THE_CLOSED_LIST: lambda dropped: (
            "measures restricting a vehicle value outside the closed list: "
            f"{dict(tally.outside_restrictions)}. Qualify it in vocabulary.py."
        ),
    }
    for why, motive in motives.items():
        dropped = df.filter(pl.col("vehicle_rejection") == why)
        if dropped.height:
            logger.warning(
                f"Dropping {dropped.height} rows of {dropped['m_file_id'].n_unique()} "
                f"{motive(dropped)}"
            )
    return df.filter(pl.col("vehicle_rejection").is_null()).drop("vehicle_rejection")


def _free_text(texts: list[str]) -> str:
    return truncate_text(FREE_TEXT_SEPARATOR.join(texts), OTHER_TYPE_TEXT_MAX_LENGTH)


def _append_unique(values: list[str], value: str) -> None:
    if value not in values:
        values.append(value)


def _keep_stricter(current: float | None, candidate: float | None) -> float | None:
    """Two thresholds on one measure: keep the lower one, which forbids the most."""
    if candidate is None:
        return current
    if current is None:
        return candidate
    return min(current, candidate)


def _restricted_labels(params: list[list[str]] | None) -> str:
    return MULTI_VALUE_SEPARATOR.join(
        pair[1]
        for pair in params or []
        if len(pair) == 2 and pair[0].strip() in RESTRICTION_PARAMETERS
    )


PEDESTRIAN_OR_CYCLE_VEHICLES = pedestrians_or_cycles_named(
    pl.col("m_params").map_elements(
        lambda params: _restricted_labels(params.to_list() if params is not None else None),
        return_dtype=pl.String,
        skip_nulls=False,
    )
)
