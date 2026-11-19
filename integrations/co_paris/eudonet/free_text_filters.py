"""Drop the rows whose free text says more than what is published: everything is read, or the
row goes.

Every free text of an act (its notes and complementary articles, its measures' paragraphs,
notes and articles, its locations' complements, mail-merge details and notes) goes through the
same reading, `free_text.read_text`. A text not understood drops the record it qualifies: the
act, the measure or the location (decided 2026-11-18). A text understood may carry an effect,
applied here: an exemption, bicycles against the flow of the act's one-way streets, the list
of vehicles a pedestrian area lets in, places on a single point.

The title is not read in full: a description, it is checked for what changes the act (a day,
an hour, "abrogé", the acts it repeals).
"""

import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from enum import StrEnum

import polars as pl
from loguru import logger

from .citations import Authority, act_number_key, numbers_repealed_or_amended
from .counts import emptied_regulations
from .free_text import (
    TEMPLATE_PREFIX,
    WHOLE_LOCATION_WORDING,
    Code,
    Explained,
    Reading,
    read_text,
)
from .locations import SCOPE_POINT
from .recurring_formulas import report_recurring_formulas
from .regulations import TIME_LIMITED_KEYS, title_names_a_time
from .vocabulary import (
    CYCLES_EXEMPTION_LABEL,
    LABEL_ONE_WAY,
    LABEL_PEDESTRIAN_AREA,
    LABEL_ZONE_30,
    MULTI_VALUE_SEPARATOR,
    PARAGRAPH_EXEMPTION_LABELS,
    PARAGRAPH_EXEMPTION_PARAMETER,
    PEDESTRIAN_ACCESS_PARAMETER,
    SERVICE_POLICE_PREFECTURE,
    exemption_label,
)

CITED_REASON = "repealed or amended by another act"
# A later act took this one's place, or repealed it: Eudonet may still say "En vigueur".
SUPERSEDING_LINK_COLUMNS = {
    "a_next_version": "a newer version",
    "a_repealing_act": "repealed",
    "a_repealed_by_clone": "repealed by a clone",
    "a_repealed_by_multiple": "repealed by a multiple repeal",
}
# Structured details that narrow a location; its free texts are read with the others.
LOCATION_DETAIL_COLUMNS = {
    "l_reserved_space_type": "reserved places",
    "l_reserved_places": "reserved places",
    "l_reserved_length": "reserved places",
    "l_displayed_length": "displayed length",
}
# On a single house number the reserved places are the point itself ("1 place payante au
# droit du n° 12"): published as a point, it is no wider than the act (decided 2026-11-16).
# Same for a displayed length on a point ("sur 24 mètres").
DETAILS_KEPT_ON_A_POINT = tuple(LOCATION_DETAIL_COLUMNS)


class Level(StrEnum):
    REGULATION = "regulation"
    MEASURE = "measure"
    LOCATION = "location"


# The free texts read, by the record they qualify.
TEXT_COLUMNS: dict[Level, tuple[str, ...]] = {
    Level.REGULATION: ("a_notes", "a_articles"),
    Level.MEASURE: ("m_paragraph", "m_notes", "m_article"),
    Level.LOCATION: (
        "l_from_location_complement",
        "l_to_location_complement",
        "l_point_location_complement",
        "l_article_details",
        "l_notes",
    ),
}
TEXT_LABELS = {
    "a_notes": "notes",
    "a_articles": "complementary article",
    "m_paragraph": "paragraph",
    "m_notes": "measure notes",
    "m_article": "measure article",
    "l_from_location_complement": "location complement",
    "l_to_location_complement": "location complement",
    "l_point_location_complement": "location complement",
    "l_article_details": "mail-merge details",
    "l_notes": "location notes",
}
# Where the numbers of the bounds are written: a number in a text must be one of them.
BOUND_NUMBER_COLUMNS = (
    "l_from_house_number",
    "l_from_road_name",
    "l_from_address_label",
    "l_to_house_number",
    "l_to_road_name",
    "l_to_address_label",
    "l_point_house_number",
    "l_point_address_label",
)
# What the mail merge prints of a location into its measure's article.
LOCATION_PRINTED_COLUMNS = (
    "l_road_name",
    "l_district",
    "l_side",
    *BOUND_NUMBER_COLUMNS,
    "l_from_number_complement",
    "l_to_number_complement",
    "l_point_number_complement",
    "l_from_location_complement",
    "l_to_location_complement",
    "l_point_location_complement",
    "l_reserved_space_type",
    "l_article_details",
    "l_displayed_length",
)
_DIGITS = re.compile(r"\d+")


@dataclass(frozen=True)
class FreeText:
    """One free text of an act, with the record it qualifies."""

    level: Level
    record: int
    column: str
    text: str
    explained: Explained = Explained()


def _cited_by_another_act(regulations: list[dict], texts_by_act: dict[int, list[str]]) -> set:
    """`a_file_id` of the acts another act of the perimeter repeals, amends or suspends in one
    of its texts, while Eudonet still says "En vigueur" (70 of the 922 publishable acts on
    2026-11-16)."""
    citing: dict[tuple, set[tuple[int, Authority | None]]] = {}
    for row in regulations:
        texts = [row["a_title_html"] or "", *texts_by_act.get(row["a_file_id"], [])]
        for key, authority in numbers_repealed_or_amended(texts):
            citing.setdefault(key, set()).add((row["a_file_id"], authority))
    cited = set()
    for row in regulations:
        key = act_number_key(row["a_identifier"] or "")
        if key is None:
            continue
        excluded = _other_authority(row["a_service"])
        if any(
            act != row["a_file_id"] and (excluded is None or authority != excluded)
            for act, authority in citing.get(key, set())
        ):
            cited.add(row["a_file_id"])
    return cited


def _other_authority(service: str | None) -> Authority | None:
    """The authority a citation names when it cannot mean this act. Not the prefect for a city
    act: the prefect's acts of before the transfer of traffic powers came over to the city's
    services with the 2017 migration ("arrêté préfectoral n° 89-10393")."""
    return Authority.CITY if service == SERVICE_POLICE_PREFECTURE else None


def _regulation_reason(row: dict) -> str | None:
    for column, reason in SUPERSEDING_LINK_COLUMNS.items():
        if row[column]:
            return reason
    if row["a_key_1"] in TIME_LIMITED_KEYS or row["a_key_2"] in TIME_LIMITED_KEYS:
        return "operation on some days only"
    if title_names_a_time(row["a_title_html"]):
        return "title names a time"
    return None


def _texts_by_act(df: pl.DataFrame) -> dict[int, list[str]]:
    """Every free text of each act, its measures' included."""
    texts: dict[int, list[str]] = defaultdict(list)
    for row in df.unique("a_file_id").select("a_file_id", "a_notes", "a_articles").iter_rows():
        texts[row[0]] += [row[1] or "", *(row[2] or [])]
    measures = df.filter(pl.col("m_file_id").is_not_null()).unique("m_file_id")
    for act, *measure_texts in measures.select(
        "a_file_id", *TEXT_COLUMNS[Level.MEASURE]
    ).iter_rows():
        texts[act] += [text for text in measure_texts if text]
    return texts


def discard_superseded_regulations(df: pl.DataFrame) -> pl.DataFrame:
    """Drop a whole act when a link says it was replaced or repealed, when another act in force
    repeals, amends or suspends it, or when its title or its keys say it holds on some days
    only."""
    columns = [
        "a_file_id",
        "a_identifier",
        "a_title_html",
        "a_key_1",
        "a_key_2",
        "a_service",
        *SUPERSEDING_LINK_COLUMNS,
    ]
    regulations = list(df.select(columns).unique("a_file_id").iter_rows(named=True))
    cited = _cited_by_another_act(regulations, _texts_by_act(df))
    reasons = {
        row["a_file_id"]: CITED_REASON if row["a_file_id"] in cited else _regulation_reason(row)
        for row in regulations
    }
    dropped = {regulation for regulation, reason in reasons.items() if reason}
    if dropped:
        counts = Counter(reasons[regulation] for regulation in dropped)
        logger.warning(
            f"Dropping {len(dropped)} regulations replaced, repealed or limited to some days by "
            f"a link, another act, their keys or their title: {dict(counts.most_common())}"
        )
    return df.filter(~pl.col("a_file_id").is_in(list(dropped)))


def _numbers(row: dict) -> frozenset[str]:
    return frozenset(
        str(int(digits))
        for column in BOUND_NUMBER_COLUMNS
        for digits in _DIGITS.findall(row[column] or "")
    )


def _parameter_spans(row: dict) -> list[str]:
    """The parameter values, alone and after each formula the mail merge prints before an
    optional one."""
    values = []
    for pair in row["m_params"] or []:
        if len(pair) == 2 and pair[1].strip():
            values.append(pair[1].strip())
            for part in pair[1].split(MULTI_VALUE_SEPARATOR):
                # Printed with or without its preposition: "aux véhicules de secours".
                values += [part.strip(), re.sub(r"^(aux|à|au|des)\s+", "", part.strip())]
    formulas = [formula for formula in row["m_option_formulas"] or [] if formula]
    return [*values, *formulas, *(f"{formula} {value}" for formula in formulas for value in values)]


# "5ème arrondissement" is also typed "5e arrondissement".
_DISTRICT_NUMBER = re.compile(r"^(\d+)(?:ème|er|e)\b")


def _district_variants(location: dict) -> list[str]:
    district = location["l_district"] or ""
    return [
        _DISTRICT_NUMBER.sub(rf"\g<1>{suffix}", part.strip())
        for part in district.split(";")
        for suffix in ("e", "ème", "er")
    ]


def _places(location: dict) -> list[str]:
    """How the mail merge prints a number of places: "sur 4 places", "4 place.s"."""
    counts = {
        value.strip()
        for value in (location["l_reserved_places"], location["l_article_details"])
        if value and value.strip().isdigit()
    }
    kind = (location["l_reserved_space_type"] or "").strip()
    forms = ("sur {} places", "sur {} place", "{} place.s", "{} places", "sur {} emplacements")
    printed = [form.format(count) for count in counts for form in forms]
    return [*printed, *(f"{form} {kind}" for form in printed if kind)]


def _measure_explained(measure: dict, locations: list[dict]) -> Explained:
    """What a measure's own fields and its locations print into its article."""
    spans = [
        *(location[column] or "" for location in locations for column in LOCATION_PRINTED_COLUMNS),
        *(variant for location in locations for variant in _district_variants(location)),
        *(form for location in locations for form in _places(location)),
    ]
    wording = [
        TEMPLATE_PREFIX,
        *WHOLE_LOCATION_WORDING,
        measure["m_opening"] or "",
        measure["m_paragraph"] or "",
        measure["m_notes"] or "",
        *_parameter_spans(measure),
    ]
    numbers = frozenset(number for location in locations for number in _numbers(location))
    return Explained(
        tuple(span for span in spans if span.strip()),
        numbers,
        tuple(span for span in wording if span.strip()),
    )


def free_texts(df: pl.DataFrame, all_rows: pl.DataFrame | None = None) -> list[FreeText]:
    """The free texts of every act: one array, each text with the record it qualifies.

    `all_rows`: the raw rows, whose locations explain a measure's article even when an earlier
    rule dropped them (one direction, one side)."""
    texts: list[FreeText] = []
    for row in df.unique("a_file_id").select("a_file_id", "a_notes", "a_articles").iter_rows():
        act, notes, articles = row
        if notes:
            texts.append(FreeText(Level.REGULATION, act, "a_notes", notes))
        texts += [FreeText(Level.REGULATION, act, "a_articles", text) for text in articles or []]

    location_columns = [
        "l_file_id",
        "l_scope",
        *LOCATION_PRINTED_COLUMNS,
        "l_reserved_places",
        "l_notes",
    ]
    present = set(df["l_file_id"].drop_nulls().to_list())
    locations_by_measure: dict[int, list[dict]] = defaultdict(list)
    for row in (
        (df if all_rows is None else all_rows)
        .filter(pl.col("l_file_id").is_not_null())
        .unique("l_file_id")
        .select("m_file_id", *location_columns)
        .iter_rows(named=True)
    ):
        locations_by_measure[row["m_file_id"]].append(row)

    measure_columns = ["m_file_id", "m_opening", "m_option_formulas", "m_params"]
    for measure in (
        df.filter(pl.col("m_file_id").is_not_null())
        .unique("m_file_id")
        .select(*dict.fromkeys([*measure_columns, *TEXT_COLUMNS[Level.MEASURE]]))
        .iter_rows(named=True)
    ):
        for column in ("m_paragraph", "m_notes"):
            if measure[column]:
                texts.append(FreeText(Level.MEASURE, measure["m_file_id"], column, measure[column]))
        if measure["m_article"]:
            explained = _measure_explained(measure, locations_by_measure[measure["m_file_id"]])
            texts.append(
                FreeText(
                    Level.MEASURE,
                    measure["m_file_id"],
                    "m_article",
                    measure["m_article"],
                    explained,
                )
            )

    present_locations = (
        location
        for group in locations_by_measure.values()
        for location in group
        if location["l_file_id"] in present
    )
    for location in present_locations:
        explained = Explained(numbers=_numbers(location))
        for column in TEXT_COLUMNS[Level.LOCATION]:
            if location[column]:
                texts.append(
                    FreeText(
                        Level.LOCATION, location["l_file_id"], column, location[column], explained
                    )
                )
    return texts


@dataclass
class Outcome:
    """What the readings do to the rows: records dropped (with why), parameters added."""

    dropped: dict[Level, dict[int, str]]
    added_params: dict[int, list[list[str]]]
    # By act: who may ride against the flow of its one-way streets.
    against_the_flow: dict[int, list[str]]


def _apply(df: pl.DataFrame, readings: list[tuple[FreeText, Reading]]) -> Outcome:
    measures = (
        df.filter(pl.col("m_file_id").is_not_null())
        .unique("m_file_id")
        .select("m_file_id", "a_file_id", "m_type")
    )
    measure_type = dict(zip(measures["m_file_id"], measures["m_type"]))
    act_of_measure = dict(zip(measures["m_file_id"], measures["a_file_id"]))
    measures_of_act: dict[int, list[int]] = defaultdict(list)
    for measure, act in act_of_measure.items():
        measures_of_act[act].append(measure)
    locations = df.filter(pl.col("l_file_id").is_not_null()).unique("l_file_id")
    scope = dict(zip(locations["l_file_id"], locations["l_scope"]))

    dropped: dict[Level, dict[int, str]] = {level: {} for level in Level}
    added: dict[int, list[list[str]]] = defaultdict(list)
    against_the_flow: dict[int, list[str]] = defaultdict(list)

    for text, reading in readings:
        label = TEXT_LABELS[text.column]
        problems = [] if reading.understood else [label]
        if text.level == Level.LOCATION:
            for effect in reading.effects:
                if effect.code != Code.RESERVED_PLACES:
                    problems.append(f"{label}: {effect.code}")
                elif scope.get(text.record) != SCOPE_POINT:
                    problems.append(f"{label}: places on a section")
        elif reading.understood:
            is_act = text.level == Level.REGULATION
            act = text.record if is_act else act_of_measure[text.record]
            own = measures_of_act[act] if is_act else [text.record]
            for effect in reading.effects:
                if effect.code == Code.AGAINST_THE_FLOW:
                    against_the_flow[act] += effect.values
                elif effect.code == Code.PEDESTRIAN_ACCESS:
                    areas = [m for m in own if measure_type[m] == LABEL_PEDESTRIAN_AREA]
                    if not areas or not effect.values:
                        problems.append(f"{label}: pedestrian access without a pedestrian area")
                    for area in areas:
                        added[area] += [
                            [PEDESTRIAN_ACCESS_PARAMETER, exemption_label(category)]
                            for category in effect.values
                        ]
                elif effect.code == Code.AMENDMENT_WORDING and is_act:
                    continue
                elif effect.code == Code.EXEMPTION and not is_act:
                    labels = [_exemption(value) for value in effect.values]
                    if (
                        labels == [CYCLES_EXEMPTION_LABEL]
                        and measure_type[text.record] == LABEL_ONE_WAY
                    ):
                        # The one-way street does not bind bicycles: they ride against the flow.
                        against_the_flow[act].append(CYCLES_EXEMPTION_LABEL)
                    elif labels and set(labels) <= PARAGRAPH_EXEMPTION_LABELS:
                        added[text.record] += [[PARAGRAPH_EXEMPTION_PARAMETER, x] for x in labels]
                    else:
                        problems.append(f"{label}: exemption")
                elif not (
                    effect.code == Code.ZONE_NAME
                    and text.column == "m_paragraph"
                    and measure_type[text.record] == LABEL_ZONE_30
                ):
                    problems.append(f"{label}: {effect.code}")
        if problems:
            dropped[text.level].setdefault(text.record, problems[0])
    return Outcome(dropped, dict(added), dict(against_the_flow))


def _exemption(value: str) -> str:
    return re.sub(r"^(aux |des |les )?", "aux ", value.strip(), flags=re.IGNORECASE).lower()


def _report(readings: list[tuple[FreeText, Reading]], alerts: list[str] | None) -> None:
    for level, texts, team_label in (
        (Level.REGULATION, "Regulation texts", "textes des arrêtés"),
        (Level.MEASURE, "Measure texts", "textes des mesures"),
        (Level.LOCATION, "Location texts", "textes des localisations"),
    ):
        by_record: dict[int, list[str]] = defaultdict(list)
        for text, reading in readings:
            if text.level == level:
                blocking = set(reading.blocking)
                by_record[text.record] += [c for c in reading.unread if c not in blocking]
        report_recurring_formulas(by_record.values(), texts, team_label, alerts)


def discard_unread_texts(
    df: pl.DataFrame, alerts: list[str] | None = None, all_rows: pl.DataFrame | None = None
) -> pl.DataFrame:
    """Read every free text; drop the act, measure or location a text not understood qualifies,
    and apply what the texts understood say."""
    texts = free_texts(df, all_rows)
    readings = [(text, read_text(text.text, text.explained)) for text in texts]
    _report(readings, alerts)
    outcome = _apply(df, readings)

    against = (pl.col("m_type") == LABEL_ONE_WAY) & pl.col("a_file_id").is_in(
        list(outcome.against_the_flow)
    )
    params = []
    for act, measure, measure_params, one_way in df.select(
        "a_file_id", "m_file_id", "m_params", against.alias("_against")
    ).iter_rows():
        extra = list(outcome.added_params.get(measure, []))
        if one_way:
            riders = dict.fromkeys(outcome.against_the_flow[act])
            extra += [[PARAGRAPH_EXEMPTION_PARAMETER, rider] for rider in riders]
        params.append([*(measure_params or []), *extra] if extra else measure_params)
    df = df.with_columns(pl.Series("m_params", params, dtype=df.schema["m_params"]))
    exempted = df.filter(against)
    if exempted.height:
        logger.info(
            f"{exempted['m_file_id'].n_unique()} one-way street measures of "
            f"{exempted['a_file_id'].n_unique()} regulations exempt who a text of the act lets "
            "ride against the flow (bicycles, taxis, buses…)"
        )
    pedestrian_lists = sum(
        any(pair[0] == PEDESTRIAN_ACCESS_PARAMETER for pair in extra)
        for extra in outcome.added_params.values()
    )
    if pedestrian_lists:
        logger.info(
            f"{pedestrian_lists} pedestrian areas published with the closed list of vehicles "
            "their act lets in, instead of the local access exemption"
        )

    for level, column, kind in (
        (Level.REGULATION, "a_file_id", "regulations"),
        (Level.MEASURE, "m_file_id", "measures"),
        (Level.LOCATION, "l_file_id", "locations"),
    ):
        reasons = outcome.dropped[level]
        if not reasons:
            continue
        is_dropped = pl.col(column).is_in(list(reasons))
        dropped = df.filter(is_dropped)
        if dropped.height:
            counts = Counter(reasons[record] for record in dropped[column].unique())
            logger.warning(
                f"Dropping {dropped[column].n_unique()} {kind} whose free text says more than "
                f"the published fields: {dict(counts.most_common())}; "
                f"{emptied_regulations(df, is_dropped)} regulations left with nothing"
            )
        df = df.filter(~is_dropped.fill_null(False))
    return df


def discard_location_details(df: pl.DataFrame) -> pl.DataFrame:
    """Drop a location narrowed by its reserved places or a displayed length ("sur 10 ml"),
    which DiaLog would publish on the whole section. On a single point they stay."""
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
