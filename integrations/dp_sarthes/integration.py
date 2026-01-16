import io

import polars as pl
import requests
from loguru import logger

from integrations.shared import DialogIntegration

URL = (
    "https://data.sarthe.fr"
    "/api/explore/v2.1/catalog/datasets/227200029_limitations-vitesse/exports/csv"
    "?lang=fr&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B"
)


class Integration(DialogIntegration):
    draft = True

    def fetch_raw_data(self) -> pl.DataFrame:
        # download
        logger.info(f"Downloading data from {URL}")
        r = requests.get(URL)
        r.raise_for_status()

        # read CSV into Polars
        return pl.read_csv(
            io.BytesIO(r.content), separator=";", encoding="utf8", ignore_errors=True
        )

    def compute_clean_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        return pl.DataFrame()
