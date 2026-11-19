"""Refuse the rows whose free text says more than what is published: everything is read, or
the row goes. And the act-level checks that read the other acts or the act's own fields:
citations, links, keys and title.

Every free text of an act (its notes and complementary articles, its measures' paragraphs,
notes and articles, its locations' complements, mail-merge details and notes) goes through the
same reading, `free_text.read_text`. A text not understood refuses the record it qualifies: the
act, the measure or the location (decided 2026-11-18). On a measure DiaLog does not publish, or
one of its locations, it refuses the act: what such a text does to the other measures is not
known (decided 2026-11-19). A text understood may carry an effect, applied here: an exemption,
bicycles against the flow of the act's one-way streets, the list of vehicles a pedestrian area
lets in, the number of places a location holds. The title has its own reader (`titles`).
"""

import re
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass

import polars as pl
from loguru import logger

from .citations import Authority, act_number_key, citations
from .free_text import (
    TEMPLATE_PREFIX,
    WHOLE_LOCATION_WORDING,
    Code,
    Explained,
    Reading,
    read_text,
)
from .locations import SCOPE_POINT
from .printed import (
    BOUND_NUMBER_COLUMNS,
    LOCATION_PRINTED_COLUMNS,
    all_narrower,
    bound_numbers,
    location_spans,
    narrower_than_the_street,
    parameter_spans,
    place_forms,
)
from .recurring_formulas import report_recurring_formulas
from .regulations import (
    TIME_LIMITED_KEYS,
    title_condition,
    title_names_a_time,
    title_says_repealed,
)
from .verdicts import (
    CITED_BY_ANOTHER_ACT,
    LEVEL_KEY,
    LOCATION_DETAIL,
    PARAMS,
    SOME_DAYS_ONLY,
    SUPERSEDED_BY_LINK,
    TITLE_CONDITION,
    TITLE_SAYS_REPEALED,
    UNREAD_ACT_TEXT,
    UNREAD_LOCATION_TEXT,
    UNREAD_MEASURE_TEXT,
    Level,
    among,
    by_key,
    refusals,
)
from .vocabulary import (
    CYCLES_EXEMPTION_LABEL,
    LABEL_NO_CONTRAFLOW_CYCLES,
    LABEL_ONE_WAY,
    LABEL_PEDESTRIAN_AREA,
    LABEL_ZONE_30,
    MEASURE_TYPE_BY_LABEL,
    PARAGRAPH_EXEMPTION_LABELS,
    PARAGRAPH_EXEMPTION_PARAMETER,
    PEDESTRIAN_ACCESS_PARAMETER,
    SERVICE_POLICE_PREFECTURE,
    exemption_labels,
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
    "l_reserved_length": "reserved length",
    "l_displayed_length": "displayed length",
}
# A number of places says how many a point or a bounded section holds, the geometry saying
# where: nothing narrower. On a whole street, it is (decided 2026-11-19). A length on a single
# house number is the point itself ("sur 24 mètres", decided 2026-11-16); on a section, a
# portion of it.
PLACE_COLUMNS = ("l_reserved_space_type", "l_reserved_places")

# The free texts read, by the record they qualify.
TEXT_COLUMNS: dict[Level, tuple[str, ...]] = {
    Level.ACT: ("a_notes", "a_articles"),
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
# A few words without a number are a zone's name in the paragraph of its zone 30, or of the
# cycle contraflow ban that goes with it. Anywhere else, a measure DiaLog does not publish
# included, they are not understood: "une voie", "voie bus" say a lane, not the carriageway
# (decided 2026-11-19).
ZONE_NAME_MEASURES = frozenset({LABEL_ZONE_30, LABEL_NO_CONTRAFLOW_CYCLES})
# A measure DiaLog does not publish, a label unknown to the catalog included: read like the
# others, refused after the arbitration.
_UNPUBLISHED = pl.col("m_type").is_not_null() & ~pl.col("m_type").is_in(
    [label for label, kind in MEASURE_TYPE_BY_LABEL.items() if kind is not None]
)


@dataclass(frozen=True)
class FreeText:
    """One free text of an act, with the record it qualifies."""

    level: Level
    record: int
    column: str
    text: str
    explained: Explained = Explained()


# A cycle of repeals ("A repeals B", "B repeals A") never settles: past this many rounds, both
# are taken as in force, so that both their citations count.
MAX_REPEAL_ROUNDS = 20


def _cited_by_another_act(
    regulations: list[dict], texts_by_act: dict[int, list[str]], no_longer: set[int]
) -> set:
    """`a_file_id` of the acts another act of the perimeter repeals, amends or suspends in one
    of its texts, while Eudonet still says "En vigueur" (70 of the 922 publishable acts on
    2026-11-16).

    Only an act still in force cites, published or not: not one of `no_longer` (a link or its
    title says replaced or repealed), nor one another act in force repeals, to a fixed point.
    An act only amended or suspended holds in part: it cites (decided 2026-11-19)."""
    acts_by_key: dict[tuple, list[dict]] = defaultdict(list)
    for row in regulations:
        if (key := act_number_key(row["a_identifier"] or "")) is not None:
            acts_by_key[key].append(row)
    citations_of = {
        row["a_file_id"]: citations(
            [row["a_title_html"] or "", *texts_by_act.get(row["a_file_id"], [])]
        )
        for row in regulations
    }

    def hit(citers: set[int], repeals_only: bool) -> set[int]:
        found = set()
        for citer in citers:
            for key, authority, repeals in citations_of[citer]:
                if repeals_only and not repeals:
                    continue
                for row in acts_by_key.get(key, []):
                    excluded = _other_authority(row["a_service"])
                    if row["a_file_id"] != citer and (excluded is None or authority != excluded):
                        found.add(row["a_file_id"])
        return found

    acts = set(citations_of)
    in_force = following = acts - no_longer
    for _ in range(MAX_REPEAL_ROUNDS):
        following = acts - no_longer - hit(in_force, repeals_only=True)
        if following == in_force:
            break
        in_force = following
    else:
        in_force |= following
    return hit(in_force, repeals_only=False)


def _other_authority(service: str | None) -> Authority | None:
    """The authority a citation names when it cannot mean this act. Not the prefect for a city
    act: the prefect's acts of before the transfer of traffic powers came over to the city's
    services with the 2017 migration ("arrêté préfectoral n° 89-10393")."""
    return Authority.CITY if service == SERVICE_POLICE_PREFECTURE else None


def _superseding_link(row: dict) -> str | None:
    return next(
        (reason for column, reason in SUPERSEDING_LINK_COLUMNS.items() if row[column]), None
    )


def _title_repealed(row: dict) -> str | None:
    return "title says repealed" if title_says_repealed(row["a_title_html"]) else None


def _some_days_only(row: dict) -> str | None:
    if row["a_key_1"] in TIME_LIMITED_KEYS or row["a_key_2"] in TIME_LIMITED_KEYS:
        return "operation on some days only"
    if title_names_a_time(row["a_title_html"]):
        return "title names a time"
    return None


def _title_condition(row: dict) -> str | None:
    return title_condition(row["a_title_html"])


def _texts_by_act(df: pl.DataFrame) -> dict[int, list[str]]:
    """Every free text of each act, its measures' included."""
    texts: dict[int, list[str]] = defaultdict(list)
    for act, notes, articles in (
        df.unique("a_file_id", maintain_order=True)
        .select("a_file_id", "a_notes", "a_articles")
        .iter_rows()
    ):
        texts[act] += [notes or "", *(articles or [])]
    measures = df.filter(pl.col("m_file_id").is_not_null()).unique("m_file_id", maintain_order=True)
    for act, *measure_texts in measures.select(
        "a_file_id", *TEXT_COLUMNS[Level.MEASURE]
    ).iter_rows():
        texts[act] += [text for text in measure_texts if text]
    return texts


def judge_superseded_regulations(rows: pl.DataFrame, alive: pl.Series) -> pl.DataFrame:
    """Refuse a whole act when another act in force repeals, amends or suspends it, when a link
    or its title says it was replaced or repealed, or when its title or its keys say it holds
    on some days only or under a condition. Reads the other acts still alive."""
    present = rows.filter(alive)
    regulations = list(
        present.unique("a_file_id", maintain_order=True)
        .select(
            "a_file_id",
            "a_identifier",
            "a_title_html",
            "a_key_1",
            "a_key_2",
            "a_service",
            *SUPERSEDING_LINK_COLUMNS,
        )
        .iter_rows(named=True)
    )
    reasons: dict[str, Callable[[dict], str | None]] = {
        SUPERSEDED_BY_LINK.column: _superseding_link,
        SOME_DAYS_ONLY.column: _some_days_only,
        TITLE_SAYS_REPEALED.column: _title_repealed,
        TITLE_CONDITION.column: _title_condition,
    }
    found = {
        column: {row["a_file_id"]: why for row in regulations if (why := reason(row))}
        for column, reason in reasons.items()
    }
    no_longer = set(found[SUPERSEDED_BY_LINK.column]) | set(found[TITLE_SAYS_REPEALED.column])
    cited = _cited_by_another_act(regulations, _texts_by_act(present), no_longer)
    found = {CITED_BY_ANOTHER_ACT.column: dict.fromkeys(cited, CITED_REASON), **found}
    return rows.with_columns(
        among(alive, by_key("a_file_id", details)).alias(column)
        for column, details in found.items()
    )


def _measure_explained(measure: dict, locations: list[dict]) -> Explained:
    """What a measure's own fields and its locations print into its article."""
    spans = [
        *(span for location in locations for span in location_spans(location)),
        *(form for location in locations for form in place_forms(location)),
    ]
    wording = [
        TEMPLATE_PREFIX,
        *WHOLE_LOCATION_WORDING,
        measure["m_opening"] or "",
        measure["m_paragraph"] or "",
        measure["m_notes"] or "",
        *parameter_spans(measure),
    ]
    numbers = frozenset(number for location in locations for number in bound_numbers(location))
    return Explained(
        tuple(span for span in spans if span.strip()),
        numbers,
        tuple(span for span in wording if span.strip()),
        all_narrower(locations),
    )


def _act_texts(acts: pl.DataFrame, explained_of: Callable[[int], Explained]) -> list[FreeText]:
    texts: list[FreeText] = []
    for act, notes, articles in (
        acts.unique("a_file_id", maintain_order=True)
        .select("a_file_id", "a_notes", "a_articles")
        .iter_rows()
    ):
        explained = explained_of(act)
        if notes:
            texts.append(FreeText(Level.ACT, act, "a_notes", notes, explained))
        texts += [
            FreeText(Level.ACT, act, "a_articles", text, explained) for text in articles or []
        ]
    return texts


def free_texts(rows: pl.DataFrame, alive: pl.Series) -> list[FreeText]:
    """The free texts of the records still alive, each with the record it qualifies. A
    measure's article is explained by all its locations, those refused already included: it
    prints them all."""
    present = rows.filter(alive)
    location_columns = [
        "a_file_id",
        "m_file_id",
        "l_file_id",
        "l_scope",
        *LOCATION_PRINTED_COLUMNS,
        "l_reserved_places",
        "l_notes",
    ]
    locations_by_measure: dict[int, list[dict]] = defaultdict(list)
    locations_by_act: dict[int, list[dict]] = defaultdict(list)
    for location in (
        rows.filter(pl.col("l_file_id").is_not_null())
        .unique("l_file_id", maintain_order=True)
        .select(*dict.fromkeys(location_columns))
        .iter_rows(named=True)
    ):
        locations_by_measure[location["m_file_id"]].append(location)
        locations_by_act[location["a_file_id"]].append(location)

    texts = _act_texts(
        present,
        lambda act: Explained(narrower_than_the_street=all_narrower(locations_by_act[act])),
    )
    measure_columns = ["m_file_id", "m_opening", "m_option_formulas", "m_params"]
    for measure in (
        present.filter(pl.col("m_file_id").is_not_null())
        .unique("m_file_id", maintain_order=True)
        .select(*dict.fromkeys([*measure_columns, *TEXT_COLUMNS[Level.MEASURE]]))
        .iter_rows(named=True)
    ):
        record = measure["m_file_id"]
        locations = locations_by_measure[record]
        on_measure = Explained(narrower_than_the_street=all_narrower(locations))
        for column in ("m_paragraph", "m_notes"):
            if measure[column]:
                texts.append(FreeText(Level.MEASURE, record, column, measure[column], on_measure))
        if measure["m_article"]:
            explained = _measure_explained(measure, locations)
            texts.append(
                FreeText(Level.MEASURE, record, "m_article", measure["m_article"], explained)
            )

    present_locations = set(present["l_file_id"].drop_nulls().to_list())
    for locations in locations_by_measure.values():
        for location in locations:
            if location["l_file_id"] not in present_locations:
                continue
            explained = Explained(
                numbers=bound_numbers(location),
                narrower_than_the_street=narrower_than_the_street(location),
            )
            texts += [
                FreeText(Level.LOCATION, location["l_file_id"], column, location[column], explained)
                for column in TEXT_COLUMNS[Level.LOCATION]
                if location[column]
            ]
    return texts


def unread_act_texts(df: pl.DataFrame) -> dict[int, str]:
    """The acts whose own texts (notes, complementary articles) are not understood even read
    as leniently as they can be, as if every location were narrower than the street: refused
    whatever their locations say, before they are read."""
    lenient = Explained(narrower_than_the_street=True)
    texts = _act_texts(df, lambda act: lenient)
    readings = [(text, read_text(text.text, text.explained)) for text in texts]
    return _apply(df, readings).dropped[Level.ACT]


@dataclass
class Outcome:
    """What the readings do to the rows: records refused (with why), parameters added."""

    dropped: dict[Level, dict[int, str]]
    added_params: dict[int, list[list[str]]]
    # By act: who may ride against the flow of its one-way streets.
    against_the_flow: dict[int, list[str]]


def _apply(df: pl.DataFrame, readings: list[tuple[FreeText, Reading]]) -> Outcome:
    """What the readings do. A text understood on a measure DiaLog does not publish, or on one
    of its locations, qualifies that record only, except for what reaches the act's other
    measures: riders against the flow, the vehicles let into its pedestrian areas. Its
    exemptions are kept on it, for whoever reads who uses a lane. A problem there refuses the
    act: what it does to the other measures is not known."""
    measures = (
        df.filter(pl.col("m_file_id").is_not_null())
        .unique("m_file_id", maintain_order=True)
        .select("m_file_id", "a_file_id", "m_type")
    )
    measure_type = dict(zip(measures["m_file_id"], measures["m_type"]))
    act_of_measure = dict(zip(measures["m_file_id"], measures["a_file_id"]))
    unpublished = set(measures.filter(_UNPUBLISHED)["m_file_id"])
    measures_of_act: dict[int, list[int]] = defaultdict(list)
    for measure, act in act_of_measure.items():
        measures_of_act[act].append(measure)
    locations = df.filter(pl.col("l_file_id").is_not_null()).unique(
        "l_file_id", maintain_order=True
    )
    measure_of_location = dict(zip(locations["l_file_id"], locations["m_file_id"]))
    narrower = {
        row["l_file_id"]: narrower_than_the_street(row)
        for row in locations.select("l_file_id", "l_scope", *BOUND_NUMBER_COLUMNS).iter_rows(
            named=True
        )
    }

    dropped: dict[Level, dict[int, str]] = {level: {} for level in Level}
    added: dict[int, list[list[str]]] = defaultdict(list)
    against_the_flow: dict[int, list[str]] = defaultdict(list)

    for text, reading in readings:
        label = TEXT_LABELS[text.column]
        problems = [] if reading.understood else [label]
        measure = {
            Level.MEASURE: text.record,
            Level.LOCATION: measure_of_location.get(text.record),
        }.get(text.level)
        if text.level is Level.LOCATION:
            for effect in reading.effects:
                if effect.code == Code.ZONE_NAME:
                    # See `ZONE_NAME_MEASURES`: on a location, never a name.
                    problems.append(f"{label}: {effect.code}")
                    continue
                if measure in unpublished:
                    continue
                if effect.code != Code.RESERVED_PLACES:
                    problems.append(f"{label}: {effect.code}")
                elif not narrower.get(text.record, False):
                    # See `PLACE_COLUMNS`.
                    problems.append(f"{label}: places on the whole street")
        elif reading.understood:
            is_act = text.level is Level.ACT
            act = text.record if is_act else act_of_measure[text.record]
            is_unpublished = not is_act and text.record in unpublished
            own = measures_of_act[act] if is_act or is_unpublished else [text.record]
            for effect in reading.effects:
                if effect.code == Code.AGAINST_THE_FLOW:
                    against_the_flow[act] += effect.values
                elif effect.code == Code.PEDESTRIAN_ACCESS:
                    areas = [m for m in own if measure_type[m] == LABEL_PEDESTRIAN_AREA]
                    if not areas or not effect.values:
                        problems.append(f"{label}: pedestrian access without a pedestrian area")
                    for area in areas:
                        added[area] += [
                            [PEDESTRIAN_ACCESS_PARAMETER, label]
                            for category in effect.values
                            for label in exemption_labels(category)
                        ]
                elif effect.code == Code.ZONE_NAME:
                    if not (
                        text.column == "m_paragraph"
                        and measure_type.get(text.record) in ZONE_NAME_MEASURES
                    ):
                        problems.append(f"{label}: {effect.code}")
                elif is_unpublished:
                    if effect.code == Code.EXEMPTION:
                        added[text.record] += [
                            [PARAGRAPH_EXEMPTION_PARAMETER, _exemption(value)]
                            for value in effect.values
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
                else:
                    problems.append(f"{label}: {effect.code}")
        if not problems:
            continue
        if measure in unpublished:
            dropped[Level.ACT].setdefault(
                act_of_measure[measure], f"{problems[0]}, on a measure DiaLog does not publish"
            )
        else:
            dropped[text.level].setdefault(text.record, problems[0])
    return Outcome(dropped, dict(added), dict(against_the_flow))


def _exemption(value: str) -> str:
    return re.sub(r"^(aux |des |les )?", "aux ", value.strip(), flags=re.IGNORECASE).lower()


def _report(readings: list[tuple[FreeText, Reading]], alerts: list[str] | None) -> None:
    for level, texts, team_label in (
        (Level.ACT, "Regulation texts", "textes des arrêtés"),
        (Level.MEASURE, "Measure texts", "textes des mesures"),
        (Level.LOCATION, "Location texts", "textes des localisations"),
    ):
        by_record: dict[int, list[str]] = defaultdict(list)
        for text, reading in readings:
            if text.level is level:
                blocking = set(reading.blocking)
                by_record[text.record] += [c for c in reading.unread if c not in blocking]
        report_recurring_formulas(by_record.values(), texts, team_label, alerts)


def judge_unread_texts(
    rows: pl.DataFrame, alive: pl.Series, alerts: list[str] | None = None
) -> pl.DataFrame:
    """Read the free texts of the records still alive; refuse the act, measure or location a
    text not understood qualifies, and add what the texts understood say to `x_params`."""
    readings = [(text, read_text(text.text, text.explained)) for text in free_texts(rows, alive)]
    _report(readings, alerts)
    outcome = _apply(rows.filter(alive), readings)

    against = (pl.col("m_type") == LABEL_ONE_WAY) & pl.col("a_file_id").is_in(
        list(outcome.against_the_flow)
    )
    params = []
    for act, measure, measure_params, one_way, is_alive in rows.select(
        "a_file_id", "m_file_id", "m_params", against.alias("_against"), pl.lit(alive)
    ).iter_rows():
        extra = list(outcome.added_params.get(measure, [])) if is_alive else []
        if one_way and is_alive:
            riders = dict.fromkeys(outcome.against_the_flow[act])
            extra += [[PARAGRAPH_EXEMPTION_PARAMETER, rider] for rider in riders]
        params.append([*(measure_params or []), *extra] if extra else measure_params)
    rows = rows.with_columns(pl.Series(PARAMS, params, dtype=rows.schema["m_params"]))
    exempted = rows.filter(against & pl.lit(alive))
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
    return rows.with_columns(
        among(alive, by_key(LEVEL_KEY[level], outcome.dropped[level])).alias(check.column)
        for level, check in (
            (Level.ACT, UNREAD_ACT_TEXT),
            (Level.MEASURE, UNREAD_MEASURE_TEXT),
            (Level.LOCATION, UNREAD_LOCATION_TEXT),
        )
    )


def judge_location_details(df: pl.DataFrame) -> pl.DataFrame:
    """Refuse a location narrowed by a length ("sur 10 ml") on a section, or by a number of
    places on a whole street: DiaLog would publish the restriction on more than the act."""
    columns = ["l_scope", *BOUND_NUMBER_COLUMNS, *LOCATION_DETAIL_COLUMNS]
    reasons = [
        next(
            (
                reason
                for column, reason in LOCATION_DETAIL_COLUMNS.items()
                if row[column]
                and not (
                    narrower_than_the_street(row)
                    if column in PLACE_COLUMNS
                    else row["l_scope"] == SCOPE_POINT
                )
            ),
            None,
        )
        for row in df.select(*dict.fromkeys(columns)).iter_rows(named=True)
    ]
    return df.with_columns(refusals(reasons).alias(LOCATION_DETAIL.column))
