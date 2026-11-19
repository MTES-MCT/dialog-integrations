import json
from typing import Any

import polars as pl
from loguru import logger

from canonical.source import BaseDataSourceIntegration
from dialog.calls import DialogApi
from sync.snapshot import state_dir
from toolkit.transforms.local_time import today_in_paris

from .client import EudonetClient, EudonetError
from .extraction import extract_tables
from .free_text_filters import (
    discard_location_details,
    discard_superseded_regulations,
    discard_unread_texts,
)
from .location_filters import (
    discard_bound_qualifiers,
    discard_measures_with_copied_locations,
    discard_points_beyond_parking,
    discard_refused_locations,
)
from .locations import ONE_DIRECTION, compute_location_fields, orient_one_way_streets
from .measures import (
    PEDESTRIAN_OR_CYCLE_MEASURES,
    add_pedestrian_area_speed,
    compute_measure_fields,
    discard_measures_out_of_model,
)
from .one_way_exceptions import arbitrate_one_way_streets
from .published_elsewhere import (
    ELSEWHERE_STATE_FILE,
    discard_published_elsewhere,
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
from .regulations import (
    IDENTIFIER_PREFIX,
    TITLE_SAYS_REPEALED,
    compute_period_fields,
    compute_regulation_fields,
    filter_police_prefecture,
)
from .schema import EudonetRawDataSchema
from .titles import discard_unread_titles
from .vehicles import (
    EXEMPTION_IN_TITLE_ONLY,
    PEDESTRIAN_OR_CYCLE_VEHICLES,
    compute_vehicle_fields,
)

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
    # DiaLog orients a bounded street since 2026-11-04: Paris publishes the one-way streets
    # whose bounds say which way (decided 2026-11-16). The `Sens` field stays frozen by the
    # source's own `ONE_DIRECTION`, on `l_direction`.
    disabled_rules = {
        "ONE_DIRECTION": "one-way streets oriented from their bounds by orient_one_way_streets"
    }

    def fetch_raw_data(self) -> pl.DataFrame:
        """Read the perimeter from Eudonet, and the numbers other channels already published
        in DiaLog, which `compute_clean_data` holds back."""
        client = EudonetClient(self._credentials())
        tables = extract_tables(client, today_in_paris())
        self.numbers_published_elsewhere = self.fetch_numbers_published_elsewhere()
        logger.info(
            f"Eudonet perimeter: {len(tables.get(str(TABLE_REGULATIONS), []))} regulations, "
            f"{len(tables.get(str(TABLE_MEASURES), []))} measures "
            f"({count_unreadable_measures(tables)} of unreadable regulations), "
            f"{len(tables.get(str(TABLE_LOCATIONS), []))} locations"
        )
        return build_raw_dataframe(tables)

    def fetch_numbers_published_elsewhere(self) -> dict[str, str]:
        return read_numbers_published_elsewhere(
            DialogApi(self.client),
            state_dir() / self.organization_settings.organization / ELSEWHERE_STATE_FILE,
            load_frozen_elsewhere(),
        )

    def compute_clean_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        today = today_in_paris()
        return (
            raw_data.pipe(filter_police_prefecture, PUBLISH_POLICE_PREFECTURE)
            .pipe(compute_regulation_fields)
            .pipe(discard_superseded_regulations)
            .pipe(discard_published_elsewhere, self.numbers_published_elsewhere)
            # Before the period and vehicle fields: no other rule can be judged yet.
            .pipe(self.apply_rules, TITLE_SAYS_REPEALED)
            # Every measure is read, those DiaLog does not publish included, before any is
            # dropped: they may change the act's other measures.
            .pipe(compute_measure_fields)
            # The raw rows: an article also prints the locations an earlier rule dropped.
            .pipe(discard_unread_texts, self.alerts, raw_data)
            .pipe(discard_unread_titles, raw_data)
            .pipe(arbitrate_one_way_streets)
            .pipe(self.apply_rules, PEDESTRIAN_OR_CYCLE_MEASURES, ONE_DIRECTION)
            .pipe(discard_measures_out_of_model)
            .pipe(self.apply_rules, PEDESTRIAN_OR_CYCLE_VEHICLES)
            .pipe(compute_vehicle_fields)
            .pipe(self.apply_rules, EXEMPTION_IN_TITLE_ONLY)
            .pipe(compute_period_fields, today)
            .pipe(discard_bound_qualifiers)
            .pipe(discard_location_details)
            .pipe(compute_location_fields)
            .pipe(orient_one_way_streets)
            .pipe(discard_points_beyond_parking)
            .pipe(discard_refused_locations)
            # Last: a location another rule drops no longer speaks against its measure.
            .pipe(discard_measures_with_copied_locations)
            .pipe(add_pedestrian_area_speed)
            .pipe(add_split_order)
        )

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
