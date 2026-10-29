import json
from datetime import date, datetime
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import polars as pl
from loguru import logger

from canonical.source import BaseDataSourceIntegration
from dialog.calls import DialogApi
from sync.snapshot import state_dir
from toolkit.transforms.local_time import PARIS

from .client import EudonetClient, EudonetError
from .extraction import extract_tables
from .location_filters import (
    discard_location_complements,
    discard_points_beyond_parking,
    discard_refused_locations,
)
from .locations import compute_location_fields
from .measures import add_pedestrian_area_speed, compute_measure_fields
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
    compute_period_fields,
    compute_regulation_fields,
    filter_police_prefecture,
)
from .schema import EudonetRawDataSchema
from .vehicles import compute_vehicle_fields

CREDENTIALS_ENV_VAR = "EUDONET_PARIS_CREDENTIALS"

# The police prefecture signs about a third of the acts, under its own authority, like the
# road of another manager: staging only (2026-09-23), never the production host until Paris agrees.
PUBLISH_POLICE_PREFECTURE = True
PRODUCTION_HOST = "dialog.beta.gouv.fr"

# Ceiling on the locations of one POST.
MAX_LOCATIONS_PER_REGULATION = 1000


def today_in_paris() -> date:
    """The current day on the French calendar, which is the one Eudonet dates are on."""
    return datetime.now(ZoneInfo(PARIS)).date()


class DataSourceIntegration(BaseDataSourceIntegration):
    name = "eudonet"
    identifier_prefix = IDENTIFIER_PREFIX
    raw_data_schema = EudonetRawDataSchema
    group_locations_by_measure = True
    max_locations_per_regulation = MAX_LOCATIONS_PER_REGULATION
    numbers_published_elsewhere: dict[str, str] | None = None

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
            raw_data.pipe(filter_police_prefecture, self._publishes_police_prefecture())
            .pipe(compute_regulation_fields, today)
            .pipe(discard_published_elsewhere, self.numbers_published_elsewhere)
            .pipe(compute_measure_fields)
            .pipe(compute_vehicle_fields)
            .pipe(compute_period_fields, today)
            .pipe(discard_location_complements)
            .pipe(compute_location_fields)
            .pipe(discard_points_beyond_parking)
            .pipe(discard_refused_locations)
            .pipe(add_pedestrian_area_speed)
        )

    def _publishes_police_prefecture(self) -> bool:
        host = urlparse(self.organization_settings.base_url or "").hostname or ""
        return PUBLISH_POLICE_PREFECTURE and not host.endswith(PRODUCTION_HOST)

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
