from datetime import date
from typing import Any

from loguru import logger

from .client import EQUALS, GREATER_OR_EQUAL, EudonetClient, EudonetError, all_of, any_of, criterion
from .raw_rows import (
    FIELD_MEASURE_LINK,
    FILTER_MEASURE_OF_LOCATION,
    LOCATION_QUERY_FIELDS,
    MEASURE_QUERY_FIELDS,
    REGULATION_FIELDS,
    REGULATION_QUERY_FIELDS,
    TABLE_LOCATIONS,
    TABLE_MEASURES,
    TABLE_REGULATIONS,
    needs_locations,
)
from .vocabulary import (
    ACCEPTED_STATES_BY_TYPE,
    TYPE_CODE_BY_LABEL,
    TYPE_PERMANENT_LABEL,
    TYPE_TEMPORARY_LABEL,
)

# Measured at 2.2 % on the 2026-09-08 dump; well above means the locations went missing.
MAX_UNLOCATED_MEASURES_SHARE = 0.25


def perimeter_criteria(today: date) -> dict[str, Any]:
    """The regulations DiaLog broadcasts: permanent in force, or temporary still running."""
    field_type = REGULATION_FIELDS["a_type"].descid
    field_state = REGULATION_FIELDS["a_state"].descid
    field_end_date = REGULATION_FIELDS["a_end_date"].descid

    def in_accepted_state(type_label: str) -> dict[str, Any]:
        states = [
            criterion(field_state, EQUALS, code)
            for code in ACCEPTED_STATES_BY_TYPE[type_label].values()
        ]
        return states[0] if len(states) == 1 else any_of(*states)

    return any_of(
        all_of(
            criterion(field_type, EQUALS, TYPE_CODE_BY_LABEL[TYPE_PERMANENT_LABEL]),
            in_accepted_state(TYPE_PERMANENT_LABEL),
        ),
        all_of(
            criterion(field_type, EQUALS, TYPE_CODE_BY_LABEL[TYPE_TEMPORARY_LABEL]),
            # Ended temporaries left out at the source; the common rule covers a replayed extract.
            criterion(field_end_date, GREATER_OR_EQUAL, today.strftime("%Y/%m/%d")),
            in_accepted_state(TYPE_TEMPORARY_LABEL),
        ),
    )


def extract_tables(client: EudonetClient, today: date) -> dict[str, list[dict[str, Any]]]:
    """The three tables of the perimeter, as flat rows keyed by table number.

    The API filters on a parent's fields one level up, not two: locations are fetched by
    batches of identifiers of the measures DiaLog can carry.
    """
    where = perimeter_criteria(today)

    regulations = client.search_all(
        TABLE_REGULATIONS, REGULATION_QUERY_FIELDS, where, "regulations"
    )
    measures = client.search_all(TABLE_MEASURES, MEASURE_QUERY_FIELDS, where, "measures")
    dialog_measures = [row for row in measures if needs_locations(row)]
    logger.info(
        f"Fetching the locations of {len(dialog_measures)} measures out of {len(measures)}: "
        "the others have no DiaLog equivalent, or a label the catalog table does not know"
    )
    locations = client.search_by_ids(
        TABLE_LOCATIONS,
        LOCATION_QUERY_FIELDS,
        FILTER_MEASURE_OF_LOCATION,
        [row["FileId"] for row in dialog_measures],
        label="locations",
    )

    check_extraction_completeness(dialog_measures, locations)

    return {
        str(TABLE_REGULATIONS): regulations,
        str(TABLE_MEASURES): measures,
        str(TABLE_LOCATIONS): locations,
    }


def check_extraction_completeness(
    measures: list[dict[str, Any]], locations: list[dict[str, Any]]
) -> None:
    """Refuse an extraction that lost its locations on the way, rather than close the
    regulations they belonged to."""
    if not measures:
        raise EudonetError("Eudonet returned no measure at all for the perimeter")
    located = {row.get(f"{FIELD_MEASURE_LINK}_fid") for row in locations}
    unlocated = sum(1 for row in measures if row["FileId"] not in located)
    share = unlocated / len(measures)
    logger.info(f"{unlocated} measures out of {len(measures)} have no location ({share:.1%})")
    if share > MAX_UNLOCATED_MEASURES_SHARE:
        raise EudonetError(
            f"{share:.0%} of the measures have no location "
            f"(limit {MAX_UNLOCATED_MEASURES_SHARE:.0%}): the extraction looks truncated"
        )
