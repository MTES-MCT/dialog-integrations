"""Opendatasoft portals (`/api/explore/v2.1`): the export of a whole dataset.

The source keeps its export URL, query string included: `use_labels` or `delimiter`
decide the column names its schema expects.
"""

import io
from urllib.parse import urlsplit, urlunsplit

import polars as pl
import requests
from loguru import logger

from toolkit.connectors.download import DOWNLOAD_TIMEOUT

FORMATS = ("parquet", "csv")


def read_export(url: str, **csv_options) -> pl.DataFrame:
    """Read `…/datasets/{id}/exports/{parquet|csv}?…`; `csv_options` go to `pl.read_csv`."""
    fmt = export_format(url)
    logger.info(f"Downloading data from {url}")
    response = requests.get(url, timeout=DOWNLOAD_TIMEOUT)
    response.raise_for_status()
    content = io.BytesIO(response.content)
    df = pl.read_parquet(content) if fmt == "parquet" else pl.read_csv(content, **csv_options)
    _compare_with_announced_count(url, df.height)
    return df


def export_format(url: str) -> str:
    path = urlsplit(url).path.rstrip("/")
    fmt = path.rsplit("/", 1)[-1]
    if "/exports/" not in path or fmt not in FORMATS:
        raise ValueError(f"Not an Opendatasoft parquet or CSV export: {url}")
    return fmt


def records_count_url(url: str) -> str | None:
    """None for a filtered export: the dataset's count would not match it."""
    parts = urlsplit(url)
    if any(key in parts.query for key in ("where=", "refine=", "exclude=")):
        return None
    dataset = parts.path.rsplit("/exports/", 1)[0]
    return urlunsplit((parts.scheme, parts.netloc, f"{dataset}/records", "limit=0", ""))


def _compare_with_announced_count(url: str, rows: int) -> None:
    # Logged, not raised: the two calls are not atomic, and a real collapse is held by
    # the volume guard of the synchronization.
    count_url = records_count_url(url)
    if count_url is None:
        return
    try:
        response = requests.get(count_url, timeout=DOWNLOAD_TIMEOUT)
        response.raise_for_status()
        announced = int(response.json()["total_count"])
    except (requests.RequestException, ValueError, KeyError, TypeError) as error:
        logger.warning(f"Could not read the announced count of {url}: {error!r}")
        return
    if announced != rows:
        logger.warning(f"Export of {url} has {rows} rows, the portal announces {announced}")
    else:
        logger.info(f"Export complete: {rows} rows, as announced")
