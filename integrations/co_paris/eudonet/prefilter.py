from typing import Any

import polars as pl

from .decision import alive_now, decide, judged
from .free_text_filters import judge_superseded_regulations, unread_act_texts
from .measures import judge_measure_fields
from .published_elsewhere import judge_published_elsewhere
from .raw_rows import (
    TABLE_COMPLEMENTARY_ARTICLES,
    TABLE_MEASURE_ARTICLES,
    TABLE_MEASURES,
    TABLE_REGULATIONS,
    Expansion,
    build_raw_dataframe,
)
from .regulations import judge_regulation_fields
from .verdicts import UNREAD_ACT_TEXT, Stage, among, by_key

Tables = dict[str, list[dict[str, Any]]]
ACT_TABLES = (
    TABLE_REGULATIONS,
    TABLE_MEASURES,
    TABLE_COMPLEMENTARY_ARTICLES,
    TABLE_MEASURE_ARTICLES,
)


def judge_head(
    rows: pl.DataFrame, numbers_published_elsewhere: dict[str, str] | None
) -> pl.DataFrame:
    """The checks of the act alone: its fields, the citations, links, keys and title, the
    other channels."""
    rows = judge_regulation_fields(rows)
    rows = judge_superseded_regulations(rows, alive_now(rows))
    return judge_published_elsewhere(rows, numbers_published_elsewhere)


def measures_whose_articles_to_read(tables: Tables) -> frozenset[int]:
    """The measures of every act that may cite another: those its own fields keep in scope."""
    rows = build_raw_dataframe(_act_tables(tables), Expansion(frozenset(), frozenset()), log=False)
    return _measures(rows.filter(alive_now(judge_regulation_fields(rows))))


def measures_whose_locations_to_read(
    tables: Tables, articled: frozenset[int], numbers_published_elsewhere: dict[str, str] | None
) -> frozenset[int]:
    """The measures of the acts not refused from the act, its measures and their articles."""
    rows = build_raw_dataframe(_act_tables(tables), Expansion(frozenset(), articled), log=False)
    head = judge_head(rows, numbers_published_elsewhere or {}).pipe(judge_measure_fields)
    alive = alive_now(head)
    head = head.with_columns(
        among(alive, by_key("a_file_id", unread_act_texts(head.filter(alive)))).alias(
            UNREAD_ACT_TEXT.column
        )
    )
    early = [check for check in judged(head) if check.stage <= Stage.PREFILTER]
    return _measures(decide(head, early).kept)


def _act_tables(tables: Tables) -> Tables:
    return {str(table): tables.get(str(table), []) for table in ACT_TABLES}


def _measures(rows: pl.DataFrame) -> frozenset[int]:
    return frozenset(rows["m_file_id"].drop_nulls().to_list())
