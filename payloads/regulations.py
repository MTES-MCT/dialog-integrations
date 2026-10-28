"""From pivot rows (`RegulationMeasure`) to the API payloads.

Pure functions: no network, no organization. A row's columns are mapped to the DTOs by
prefix — `period_*`, `location_*`, `vehicle_*`, with the prefix stripped — and rows are
grouped by `regulation_identifier` into one regulation carrying one measure per row.

Two opt-ins, set by the data source, change that last step: grouping the rows that
share a `measure_group_key` into one measure carrying all their locations, and cutting
a regulation with too many locations for a single POST into numbered slices.
"""

import re
from collections.abc import Callable, Collection

import polars as pl
from loguru import logger

from api.dia_log_client.models import (
    PostApiRegulationsAddBody,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodyStatus,
    PostApiRegulationsAddBodySubject,
    SaveMeasureDTO,
)

# `build_measure(row)` or `build_measure(row, locations)`: see `build_measure`.
MeasureBuilder = Callable[..., SaveMeasureDTO]


def build_regulations(
    clean_data: pl.DataFrame,
    status: PostApiRegulationsAddBodyStatus,
    build_measure: MeasureBuilder,
    *,
    group_locations_by_measure: bool = False,
    max_locations_per_regulation: int | None = None,
    unbuilt: set[str] | None = None,
) -> list[PostApiRegulationsAddBody]:
    """Group the rows by `regulation_identifier`, one measure per row.

    The regulation fields are read on the first row of the group: every row of a
    regulation carries the same values. A regulation with a measure that cannot be built
    is left out whole, every slice of it, and its identifier goes into `unbuilt` when
    given: sending the rest would drop that measure from DiaLog.

    Two opt-ins, driven by the data source (see `BaseDataSourceIntegration`):

    - `group_locations_by_measure`: rows sharing a `measure_group_key` collapse into a
      single measure carrying all of their locations, instead of one measure per row;
    - `max_locations_per_regulation`: a regulation carrying more rows than the ceiling
      is cut into `IDENTIFIER-01`, `IDENTIFIER-02`… slices, each its own regulation.
    """
    regulations = []

    for _, group_df in clean_data.group_by("regulation_identifier"):
        slices = _split_regulation_rows(group_df, max_locations_per_regulation)
        built = []
        complete = True

        for index, slice_df in enumerate(slices):
            measures, slice_complete = _build_measures(
                slice_df, build_measure, group_locations_by_measure
            )
            complete = complete and slice_complete
            if not measures:
                continue

            first_row = slice_df.row(0, named=True)
            identifier = first_row["regulation_identifier"]
            if len(slices) > 1:
                identifier = f"{identifier}-{index + 1:02d}"

            built.append(_build_regulation(first_row, identifier, status, measures))

        if complete:
            regulations.extend(built)
        elif unbuilt is not None:
            unbuilt.add(group_df["regulation_identifier"][0])

    return regulations


def is_part_of(identifier: str, regulations: Collection[str]) -> bool:
    """Whether `identifier` is one of `regulations`, or a slice of one (`X-02` of `X`)."""
    return identifier in regulations or re.sub(r"-\d{2}$", "", identifier) in regulations


def _build_measures(
    slice_df: pl.DataFrame, build_measure: MeasureBuilder, group_by_measure: bool
) -> tuple[list[SaveMeasureDTO], bool]:
    """The measures of one regulation (or one slice of it), and whether all were built.

    Without grouping, every row is its own measure with its own location: the
    historical behaviour of every source in production. With grouping, the rows
    sharing a `measure_group_key` become one measure carrying all their locations.
    """
    measures = []
    complete = True

    if not group_by_measure:
        for row in slice_df.iter_rows(named=True):
            try:
                measures.append(build_measure(row))
            except Exception as e:
                logger.error(f"Error creating measure: {e}")
                complete = False
        return measures, complete

    for _, measure_df in slice_df.group_by("measure_group_key", maintain_order=True):
        rows = list(measure_df.iter_rows(named=True))
        try:
            measures.append(build_measure(rows[0], rows))
        except Exception as e:
            logger.error(f"Error creating measure: {e}")
            complete = False
    return measures, complete


def _split_regulation_rows(group_df: pl.DataFrame, max_locations: int | None) -> list[pl.DataFrame]:
    """Cut a regulation into slices small enough for a single POST.

    The API is not bounded by payload size but by how long it takes to persist the
    locations: past roughly 1 700 of them the request dies on a server-side timeout,
    and splitting the same total across several measures does not help, the ceiling
    is per regulation. Slices are cut along `regulation_split_order`, so a source that
    ranks its rows geographically gets spatially coherent slices.
    """
    if not max_locations or group_df.height <= max_locations:
        return [group_df]

    if "regulation_split_order" in group_df.columns:
        group_df = group_df.sort("regulation_split_order", nulls_last=True)

    slices = [
        group_df.slice(offset, max_locations) for offset in range(0, group_df.height, max_locations)
    ]
    logger.info(
        f"Regulation {group_df.row(0, named=True)['regulation_identifier']} carries "
        f"{group_df.height} locations, above the {max_locations} ceiling: "
        f"splitting into {len(slices)} regulations"
    )
    return slices


def _build_regulation(
    first_row: dict,
    identifier: str,
    status: PostApiRegulationsAddBodyStatus,
    measures: list[SaveMeasureDTO],
) -> PostApiRegulationsAddBody:
    regulation = PostApiRegulationsAddBody(
        identifier=identifier,
        category=PostApiRegulationsAddBodyCategory(first_row["regulation_category"]),
        status=PostApiRegulationsAddBodyStatus(status),
        subject=PostApiRegulationsAddBodySubject(first_row["regulation_subject"]),
        title=first_row["regulation_title"],
        other_category_text=first_row.get("regulation_other_category_text"),
        measures=measures,  # type: ignore
    )

    # The document URL is not a field of the generated model; it goes as an extra.
    if first_row.get("regulation_document_url"):
        regulation.additional_properties["documentUrl"] = first_row["regulation_document_url"]

    return regulation
