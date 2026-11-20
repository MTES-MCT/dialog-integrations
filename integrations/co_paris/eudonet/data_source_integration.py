import json
from typing import Any

import polars as pl
from loguru import logger

from canonical.source import BaseDataSourceIntegration
from dialog.calls import DialogApi
from sync.snapshot import state_dir
from toolkit.rules.rule import Rule
from toolkit.transforms.local_time import today_in_paris

from .client import EudonetClient, EudonetError
from .decision import Decision, alive_now, decide
from .extraction import extract_tables
from .free_text_filters import judge_location_details, judge_unread_texts
from .location_filters import (
    judge_bound_qualifiers,
    judge_copied_locations,
    judge_points_beyond_parking,
    judge_refused_locations,
)
from .locations import ONE_DIRECTION, judge_location_fields, judge_one_way_orientation
from .measures import (
    PEDESTRIAN_OR_CYCLE_MEASURES,
    add_pedestrian_area_speed,
    judge_measure_fields,
    judge_out_of_model,
)
from .one_way_exceptions import judge_one_way_lanes, split_one_way_measures
from .prefilter import (
    judge_head,
    measures_whose_articles_to_read,
    measures_whose_locations_to_read,
)
from .published_elsewhere import (
    ELSEWHERE_STATE_FILE,
    load_frozen_elsewhere,
    read_numbers_published_elsewhere,
)
from .raw_rows import (
    TABLE_LOCATIONS,
    TABLE_MEASURES,
    TABLE_REGULATIONS,
    build_raw_dataframe,
    count_unreadable_measures,
)
from .regulations import IDENTIFIER_PREFIX, judge_period_fields, keep_authority
from .report import report
from .schema import EudonetRawDataSchema
from .titles import judge_unread_titles
from .vehicles import (
    EXEMPTION_IN_TITLE_ONLY,
    PEDESTRIAN_OR_CYCLE_VEHICLES,
    judge_vehicle_fields,
)
from .verdicts import STATUS, Check, Status, among, rule_check, rule_status

CREDENTIALS_ENV_VAR = "EUDONET_PARIS_CREDENTIALS"

# The police prefecture signs about a third of the acts, under its own authority: published
# everywhere, production included, while a contact with the prefecture is set up (2026-11-19).
PUBLISH_POLICE_PREFECTURE = True

# Every location is a named street geocoded on the BD TOPO by DiaLog: past about a hundred,
# a POST can outlive the API timeout (a geometry sent as is holds up to 1 000).
MAX_LOCATIONS_PER_REGULATION = 100


class DataSourceIntegration(BaseDataSourceIntegration):
    name = "eudonet"
    identifier_prefix = IDENTIFIER_PREFIX
    raw_data_schema = EudonetRawDataSchema
    group_locations_by_measure = True
    max_locations_per_regulation = MAX_LOCATIONS_PER_REGULATION
    numbers_published_elsewhere: dict[str, str] | None = None
    # The last run's decision: every reason, for the report and the analyses.
    decision: Decision | None = None
    # The rules judged in the run, by verdict column, with their labels for the reasons.
    rule_checks: dict[str, Check] = {}
    # The acts of one authority only (`regulations.CITY`, `PREFECTURE`); None for all.
    authority: str | None = None
    # DiaLog orients a bounded street since 2026-11-04: Paris publishes the one-way streets
    # whose bounds say which way (decided 2026-11-16). The `Sens` field stays frozen by the
    # source's own `ONE_DIRECTION`, on `l_direction`.
    disabled_rules = {
        "ONE_DIRECTION": "one-way streets oriented from their bounds by judge_one_way_orientation"
    }

    def fetch_raw_data(self) -> pl.DataFrame:
        """Read the numbers other channels already published in DiaLog, which
        `compute_clean_data` holds back, then the perimeter from Eudonet: the articles, then
        the locations, of the acts not refused already (`prefilter`)."""
        self.numbers_published_elsewhere = numbers = self.fetch_numbers_published_elsewhere()
        tables, expansion = extract_tables(
            EudonetClient(self._credentials()),
            today_in_paris(),
            measures_whose_articles_to_read,
            lambda read, articled: measures_whose_locations_to_read(read, articled, numbers),
        )
        logger.info(
            f"Eudonet perimeter: {len(tables.get(str(TABLE_REGULATIONS), []))} regulations, "
            f"{len(tables.get(str(TABLE_MEASURES), []))} measures "
            f"({count_unreadable_measures(tables)} of regulations the API account cannot "
            f"read), {len(tables.get(str(TABLE_LOCATIONS), []))} locations"
        )
        return build_raw_dataframe(tables, expansion)

    def fetch_numbers_published_elsewhere(self) -> dict[str, str]:
        return read_numbers_published_elsewhere(
            DialogApi(self.client),
            state_dir() / self.organization_settings.organization / ELSEWHERE_STATE_FILE,
            load_frozen_elsewhere(),
        )

    def compute_clean_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        """The rows kept (`decided_rows`), those of this source's authority."""
        self.dataset_restrictions = raw_data.pipe(keep_authority, self.authority).height
        return (
            self.decided_rows(raw_data).pipe(keep_authority, self.authority).pipe(add_split_order)
        )

    def decided_rows(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        """Every act judged (`judge`), then decided once (`decision.decide`): the rows kept,
        one-way measures split among their sections published, pedestrian areas twinned."""
        self.decision = decide(self.judge(raw_data), named=list(self.rule_checks.values()))
        report(self.decision)
        return self.decision.kept.pipe(split_one_way_measures).pipe(add_pedestrian_area_speed)

    def judge(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        """Every check adds its verdict, none drops a row. Those that read the act's other
        records (citations, texts, titles, lanes against one-way streets, vehicles counted,
        dates by number, copied locations) judge the rows still alive: a refused record no
        longer speaks."""
        self.rule_checks = {}
        today = today_in_paris()
        rows = judge_head(raw_data, self.numbers_published_elsewhere)
        # Before the measures' fields, as these rules always were.
        rows = self.judge_rules(rows).pipe(judge_measure_fields)
        # Every measure is read, those DiaLog does not publish included, before any is
        # refused: they may change the act's other measures.
        rows = judge_unread_texts(rows, alive_now(rows), self.alerts)
        rows = judge_unread_titles(rows, alive_now(rows))
        rows = judge_one_way_lanes(rows, alive_now(rows))
        rows = self.judge_rules(rows, PEDESTRIAN_OR_CYCLE_MEASURES, ONE_DIRECTION)
        rows = self.judge_rules(judge_out_of_model(rows), PEDESTRIAN_OR_CYCLE_VEHICLES)
        rows = self.judge_rules(
            judge_vehicle_fields(rows, alive_now(rows)), EXEMPTION_IN_TITLE_ONLY
        )
        rows = (
            judge_period_fields(rows, alive_now(rows), today)
            .pipe(judge_bound_qualifiers)
            .pipe(judge_location_details)
            .pipe(judge_location_fields)
            .pipe(judge_one_way_orientation)
            .pipe(judge_points_beyond_parking)
            .pipe(judge_refused_locations)
        )
        # Last: a location another check refuses no longer speaks against its measure.
        return judge_copied_locations(rows, alive_now(rows))

    def judge_rules(self, rows: pl.DataFrame, *own_rules: Rule) -> pl.DataFrame:
        """The common rules and `own_rules` not judged yet, once their columns are there, as
        verdicts on the rows still alive: as `apply_rules` left them out, counted and logged
        the same way."""
        alive = alive_now(rows)
        fresh = [
            rule
            for rule in [*self.active_rules, *own_rules]
            if rule_check(rule).column not in rows.columns
        ]
        columns = []
        for rule, fails in self.rule_fails(rows, fresh, among=alive).values():
            check = rule_check(rule)
            self.rule_checks[check.column] = check
            status = (
                pl.when(pl.lit(fails))
                .then(pl.lit(rule_status(rule).value))
                .otherwise(pl.lit(Status.OK.value))
                .cast(STATUS)
            )
            verdict = pl.struct(status=status, detail=pl.lit(None, dtype=pl.String))
            columns.append(among(alive, verdict).alias(check.column))
        return rows.with_columns(columns) if columns else rows

    def _credentials(self) -> dict[str, Any]:
        raw = self.organization_settings.eudonet_paris_credentials
        if not raw:
            raise EudonetError(
                f"{CREDENTIALS_ENV_VAR} is not set: the Eudonet credentials are required"
            )
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            # `from None`: the decoding error carries the whole secret as its document.
            raise EudonetError(f"{CREDENTIALS_ENV_VAR} is not valid JSON") from None


def add_split_order(df: pl.DataFrame) -> pl.DataFrame:
    """Above the ceiling, the slices `-01`, `-02`… hold the same rows from one night to the
    next, in Eudonet order, each measure in as few of them as possible. The rows keep their
    place: below the ceiling the payload is unchanged."""
    return (
        df.with_row_index("_row")
        .sort("m_file_id", "measure_group_key", "l_file_id", nulls_last=True)
        .with_row_index("regulation_split_order")
        .sort("_row")
        .drop("_row")
        .with_columns(pl.col("regulation_split_order").cast(pl.Int64))
    )
