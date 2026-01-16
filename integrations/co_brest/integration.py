import tempfile
import zipfile
from pathlib import Path

import geopandas as gpd
import polars as pl
import requests
from loguru import logger

from integrations.shared import DialogIntegration

URL = "https://www.data.gouv.fr/api/1/datasets/r/3ca7bd06-6489-45a2-aee9-efc6966121b2"
FILENAME = "DEP_ARR_CIRC_STAT_L_V.shp"


class Integration(DialogIntegration):
    draft = False

    def fetch_raw_data(self) -> pl.DataFrame:
        logger.info(f"Downloading and reading shapefile data from {URL}")
        with tempfile.TemporaryDirectory() as tmpdir:
            zip_path = Path(tmpdir) / "data.zip"

            # download
            r = requests.get(URL)
            r.raise_for_status()
            zip_path.write_bytes(r.content)
            logger.info(f"Downloaded zip file to {zip_path}")

            # unzip
            with zipfile.ZipFile(zip_path) as z:
                z.extractall(tmpdir)

            # find .shp
            shp_path = next(Path(tmpdir).rglob("*.shp"))
            shp_path = Path(tmpdir) / FILENAME

            # read
            logger.info(f"Reading file {shp_path}")
            gdf = gpd.read_file(shp_path)

        # geometry -> WKT pour Polars
        gdf["geometry"] = gdf.geometry.to_wkt()
        return pl.from_pandas(gdf)
