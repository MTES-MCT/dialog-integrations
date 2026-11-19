from datetime import date
from typing import Any

from loguru import logger

from .client import EQUALS, GREATER_OR_EQUAL, EudonetClient, EudonetError, all_of, any_of, criterion
from .raw_rows import (
    COMPLEMENTARY_ARTICLE_QUERY_FIELDS,
    FIELD_MEASURE_LINK,
    FILTER_MEASURE_OF_LOCATION,
    LOCATION_QUERY_FIELDS,
    MEASURE_ARTICLE_QUERY_FIELDS,
    MEASURE_QUERY_FIELDS,
    REGULATION_FIELDS,
    REGULATION_QUERY_FIELDS,
    TABLE_COMPLEMENTARY_ARTICLES,
    TABLE_LOCATIONS,
    TABLE_MEASURE_ARTICLES,
    TABLE_MEASURES,
    TABLE_REGULATIONS,
    is_read,
)
from .vocabulary import (
    ACCEPTED_STATES_BY_TYPE,
    TYPE_CODE_BY_LABEL,
    TYPE_PERMANENT_LABEL,
    TYPE_TEMPORARY_LABEL,
)

# Measured at 1.5 % of all the measures on 2026-11-17; well above means the locations went missing.
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
    """The five tables of the perimeter, as flat rows keyed by table number.

    The API filters on a parent's fields one level up, not two: locations and the measures'
    articles are fetched by batches of measure identifiers, for every measure DiaLog does not
    publish too: a bus lane against the flow changes the one-way street of the same act, and
    their texts are read like the others. Only the measures that can change nothing else
    (`UNREAD_MEASURE_LABELS`) are left aside.
    """
    where = perimeter_criteria(today)

    regulations = client.search_all(
        TABLE_REGULATIONS, REGULATION_QUERY_FIELDS, where, "regulations"
    )
    measures = client.search_all(TABLE_MEASURES, MEASURE_QUERY_FIELDS, where, "measures")
    # The complementary articles may exclude a street or add a time: read to drop the act.
    articles = client.search_all(
        TABLE_COMPLEMENTARY_ARTICLES,
        COMPLEMENTARY_ARTICLE_QUERY_FIELDS,
        where,
        "complementary articles",
    )
    if regulations and not articles:
        raise EudonetError(
            "Eudonet returned no article for the perimeter: their texts could not be checked"
        )
    read = [row for row in measures if is_read(row)]
    logger.info(
        f"Fetching the locations and articles of {len(read)} measures out of {len(measures)}"
    )
    measure_ids = [row["FileId"] for row in read]
    locations = client.search_by_ids(
        TABLE_LOCATIONS,
        LOCATION_QUERY_FIELDS,
        FILTER_MEASURE_OF_LOCATION,
        measure_ids,
        label="locations",
    )

    check_extraction_completeness(read, locations)
    measure_articles = client.search_by_ids(
        TABLE_MEASURE_ARTICLES,
        MEASURE_ARTICLE_QUERY_FIELDS,
        TABLE_MEASURES,
        measure_ids,
        label="measure articles",
    )
    if read and not measure_articles:
        raise EudonetError(
            "Eudonet returned no measure article for the perimeter: their texts could not be "
            "checked"
        )

    return {
        str(TABLE_REGULATIONS): regulations,
        str(TABLE_MEASURES): measures,
        str(TABLE_LOCATIONS): locations,
        str(TABLE_COMPLEMENTARY_ARTICLES): articles,
        str(TABLE_MEASURE_ARTICLES): measure_articles,
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
