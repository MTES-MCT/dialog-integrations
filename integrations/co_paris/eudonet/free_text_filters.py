"""Drop the rows whose free text says more than what is published: everything is read, or
the row goes. The neutral formulas are in `neutral_text`."""

from collections import Counter

import polars as pl
from loguru import logger

from .counts import emptied_regulations
from .locations import SCOPE_POINT
from .neutral_text import (
    TIME_LIMITED_KEYS,
    act_number_key,
    article_is_neutral,
    note_is_neutral,
    numbers_repealed_or_amended,
    paragraph_exemptions,
    paragraph_is_neutral,
    title_names_a_time,
)
from .vocabulary import (
    LABEL_PEDESTRIAN_AREA,
    LABEL_ZONE_30,
    PARAGRAPH_EXEMPTION_LABELS,
    PARAGRAPH_EXEMPTION_PARAMETER,
)

CITED_REASON = "repealed or amended by another act"
# A later act took this one's place, or repealed it: Eudonet may still say "En vigueur".
SUPERSEDING_LINK_COLUMNS = {
    "a_next_version": "a newer version",
    "a_repealing_act": "repealed",
    "a_repealed_by_clone": "repealed by a clone",
    "a_repealed_by_multiple": "repealed by a multiple repeal",
}
LOCATION_DETAIL_COLUMNS = {
    "l_notes": "notes",
    "l_reserved_space_type": "reserved places",
    "l_reserved_places": "reserved places",
    "l_reserved_length": "reserved places",
    "l_displayed_length": "displayed length",
}
# On a single house number the reserved places are the point itself ("1 place payante au
# droit du n° 12"): published as a point, it is no wider than the act (decided 2026-11-16).
RESERVED_PLACE_COLUMNS = ("l_reserved_space_type", "l_reserved_places", "l_reserved_length")
# Same for a displayed length on a point ("sur 24 mètres"), decided 2026-11-16.
DETAILS_KEPT_ON_A_POINT = (*RESERVED_PLACE_COLUMNS, "l_displayed_length")


def _regulation_reason(row: dict, has_pedestrian_area: bool) -> str | None:
    for column, reason in SUPERSEDING_LINK_COLUMNS.items():
        if row[column]:
            return reason
    if row["a_key_1"] in TIME_LIMITED_KEYS or row["a_key_2"] in TIME_LIMITED_KEYS:
        return "operation on some days only"
    if title_names_a_time(row["a_title_html"]):
        return "title names a time"
    if not note_is_neutral(row["a_notes"]):
        return "notes"
    if not all(
        article_is_neutral(article, has_pedestrian_area) for article in row["a_articles"] or []
    ):
        return "complementary article"
    return None


def _cited_by_another_act(regulations: list[dict]) -> set:
    """`a_file_id` of the acts another act of the perimeter repeals, amends or suspends in its
    title or a complementary article, while Eudonet still says "En vigueur" (70 of the 922
    publishable acts on 2026-11-16)."""
    citing: dict[tuple, set] = {}
    for row in regulations:
        texts = [row["a_title_html"] or "", *(row["a_articles"] or [])]
        for key in numbers_repealed_or_amended(texts):
            citing.setdefault(key, set()).add(row["a_file_id"])
    cited = set()
    for row in regulations:
        key = act_number_key(row["a_identifier"] or "")
        if key is not None and citing.get(key, set()) - {row["a_file_id"]}:
            cited.add(row["a_file_id"])
    return cited


def discard_unread_regulation_texts(df: pl.DataFrame) -> pl.DataFrame:
    """Drop a whole act when a link says it was replaced or repealed, when another act in force
    repeals, amends or suspends it, when its title or its keys say it holds on some days only,
    or when its notes or a complementary article say something no field carries (an excluded
    street, a time, an exemption, a part repealed by a later act)."""
    with_pedestrian_area = set(
        df.filter(pl.col("m_type") == LABEL_PEDESTRIAN_AREA)["a_file_id"].unique().to_list()
    )
    columns = [
        "a_file_id",
        "a_identifier",
        "a_title_html",
        "a_key_1",
        "a_key_2",
        "a_notes",
        "a_articles",
        *SUPERSEDING_LINK_COLUMNS,
    ]
    regulations = list(df.select(columns).unique("a_file_id").iter_rows(named=True))
    cited = _cited_by_another_act(regulations)
    reasons = {
        row["a_file_id"]: (
            CITED_REASON
            if row["a_file_id"] in cited
            else _regulation_reason(row, row["a_file_id"] in with_pedestrian_area)
        )
        for row in regulations
    }
    dropped = {regulation for regulation, reason in reasons.items() if reason}
    if dropped:
        counts = Counter(reasons[regulation] for regulation in dropped)
        logger.warning(
            f"Dropping {len(dropped)} regulations whose links or free text say more than the "
            f"published fields: {dict(counts.most_common())}"
        )
    return df.filter(~pl.col("a_file_id").is_in(list(dropped)))


def discard_unread_measure_paragraphs(df: pl.DataFrame) -> pl.DataFrame:
    """Drop a measure whose paragraph (`Alinéa`) adds a vehicle, a time, an exemption…

    An exemption of emergency vehicles ("sauf aux véhicules de secours") becomes the
    measure's exemption parameter, read with the others.
    """
    df = _paragraph_exemptions_as_parameters(df)
    unread = pl.Series(
        [
            not paragraph_is_neutral(paragraph, label == LABEL_ZONE_30)
            for paragraph, label in df.select("m_paragraph", "m_type").iter_rows()
        ],
        dtype=pl.Boolean,
    )
    dropped = df.filter(unread)
    if dropped.height:
        logger.warning(
            f"Dropping {dropped['m_file_id'].n_unique()} measures whose paragraph says more "
            f"than the published fields; {emptied_regulations(df, pl.lit(unread))} regulations "
            "left with no measure"
        )
    return df.filter(~unread)


def _paragraph_exemptions_as_parameters(df: pl.DataFrame) -> pl.DataFrame:
    params, paragraphs, moved = [], [], 0
    for paragraph, measure_params in df.select("m_paragraph", "m_params").iter_rows():
        labels = paragraph_exemptions(paragraph, PARAGRAPH_EXEMPTION_LABELS)
        if labels is None:
            params.append(measure_params)
            paragraphs.append(paragraph)
            continue
        moved += 1
        extra = [[PARAGRAPH_EXEMPTION_PARAMETER, label] for label in labels]
        params.append([*(measure_params or []), *extra])
        paragraphs.append(None)
    if moved:
        logger.info(f"{moved} rows read an emergency exemption from their measure paragraph")
    return df.with_columns(
        pl.Series("m_params", params, dtype=df.schema["m_params"]),
        pl.Series("m_paragraph", paragraphs, dtype=pl.Utf8),
    )


def discard_unread_location_details(df: pl.DataFrame) -> pl.DataFrame:
    """Drop a location narrowed by its notes, its reserved places or a displayed length
    ("sur 10 ml"), which DiaLog would publish on the whole section. Reserved places on a
    single point stay."""
    reasons = [
        next(
            (
                reason
                for column, reason in LOCATION_DETAIL_COLUMNS.items()
                if row[column]
                and not (column in DETAILS_KEPT_ON_A_POINT and row["l_scope"] == SCOPE_POINT)
            ),
            None,
        )
        for row in df.select(*LOCATION_DETAIL_COLUMNS, "l_scope").iter_rows(named=True)
    ]
    detailed = pl.Series([reason is not None for reason in reasons], dtype=pl.Boolean)
    dropped = df.filter(detailed)
    if dropped.height:
        counts = Counter(reason for reason in reasons if reason)
        logger.warning(
            f"Dropping {dropped.height} locations whose details narrow the measure: "
            f"{dict(counts.most_common())}; {dropped['a_identifier'].n_unique()} regulations "
            f"published without them, {emptied_regulations(df, pl.lit(detailed))} left with "
            "no location"
        )
    return df.filter(~detailed)
