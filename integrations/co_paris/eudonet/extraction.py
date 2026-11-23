from collections.abc import Callable
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
    Expansion,
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


Tables = dict[str, list[dict[str, Any]]]


def extract_tables(
    client: EudonetClient,
    today: date,
    articles_of: Callable[[Tables], frozenset[int]],
    locations_of: Callable[[Tables, frozenset[int]], frozenset[int]],
) -> tuple[Tables, Expansion]:
    """The five tables of the perimeter, as flat rows keyed by table number, and the measures
    whose articles and locations were read.

    The API filters on a parent's fields one level up, not two: locations and the measures'
    articles are fetched by batches of measure identifiers.
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
    act_tables = {
        str(TABLE_REGULATIONS): regulations,
        str(TABLE_MEASURES): measures,
        str(TABLE_COMPLEMENTARY_ARTICLES): articles,
    }
    articles_chosen = articles_of(act_tables)
    articled = [row for row in read if row["FileId"] in articles_chosen]
    measure_articles = client.search_by_ids(
        TABLE_MEASURE_ARTICLES,
        MEASURE_ARTICLE_QUERY_FIELDS,
        TABLE_MEASURES,
        [row["FileId"] for row in articled],
        label="measure articles",
        sparse=True,
    )
    if articled and not measure_articles:
        raise EudonetError(
            "Eudonet returned no measure article for the perimeter: their texts could not be "
            "checked"
        )
    locations_chosen = locations_of(
        {**act_tables, str(TABLE_MEASURE_ARTICLES): measure_articles}, articles_chosen
    )
    located = [row for row in read if row["FileId"] in locations_chosen]
    logger.info(
        f"Fetching the locations of {len(located)} measures out of {len(measures)}, their "
        f"articles read for {len(articled)}: the others' acts are refused already"
    )
    locations = client.search_by_ids(
        TABLE_LOCATIONS,
        LOCATION_QUERY_FIELDS,
        FILTER_MEASURE_OF_LOCATION,
        [row["FileId"] for row in located],
        label="locations",
    )
    check_extraction_completeness(located, locations)

    tables = {
        str(TABLE_REGULATIONS): regulations,
        str(TABLE_MEASURES): measures,
        str(TABLE_LOCATIONS): locations,
        str(TABLE_COMPLEMENTARY_ARTICLES): articles,
        str(TABLE_MEASURE_ARTICLES): measure_articles,
    }
    expansion = Expansion(
        frozenset(row["FileId"] for row in located), frozenset(row["FileId"] for row in articled)
    )
    return tables, expansion


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
