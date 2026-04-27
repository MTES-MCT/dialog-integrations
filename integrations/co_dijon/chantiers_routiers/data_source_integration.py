import io
import json

import geopandas as gpd
import polars as pl
import requests
from loguru import logger
from shapely.geometry import mapping

from api.dia_log_client.api.private.post_api_nearby_streets import (
    sync_detailed as _get_reverse_label,
)
from api.dia_log_client.models import (
    MeasureTypeEnum,
    PostApiNearbyStreetsBody,
    PostApiNearbyStreetsBodyGeometry,
    PostApiNearbyStreetsResponse200Item,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
)
from integrations.base_data_source_integration import BaseDataSourceIntegration
from integrations.co_dijon.chantiers_routiers.schema import DijonChantiersRoutiersSchema

URL = (
    "https://data.metropole-dijon.fr"
    "/d4c/api/records/2.0/"
    "downloadfile/format=csv"
    "&resource_id=a1c33f1e-ef80-4876-b6f5-3e6aee573bcf"
    "&use_labels_for_header=true"
    "&user_defined_fields=true"
)

LOCAL_FILE = "explorations/co_dijon/data/travaux.csv"


class DataSourceIntegration(BaseDataSourceIntegration):
    """Data source for Dijon chantiers routiers CSV data."""

    raw_data_schema = DijonChantiersRoutiersSchema
    name = "limitation_vitesse"

    def fetch_raw_data(self):
        logger.info(f"Downloading data from {URL}")

        r = requests.get(URL)
        r.raise_for_status()

        df = pl.read_csv(io.BytesIO(r.content))
        # df = pl.read_csv(LOCAL_FILE)

        return df

    def compute_clean_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        return (
            raw_data.pipe(compute_measure_fields)
            .pipe(compute_period_fields)
            .pipe(self.compute_location_fields)
            .pipe(compute_regulation_fields)
            .pipe(compute_vehicle_fields)
        )

    def compute_location_fields(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Defined in class to be able to use `api.client`
        Compute all location fields for SaveLocationDTO.
        - location_road_type: always RoadTypeEnum.RAWGEOJSON for Dijon
        - location_geometry: from geometry field (WKB) transformed to GeoJSON (WGS84)
        Filter out rows where geometry is null.
        """
        # Count rows with null geometry before filtering
        n_null_geometry = df.select(pl.col("geometry").is_null().sum()).item()
        if n_null_geometry > 0:
            logger.warning(f"Dropping {n_null_geometry} rows with null geometry")

        # Filter out rows where geometry is null
        df = df.filter(pl.col("geometry").is_not_null())

        dfcoords = df.select(
            [
                df["geo_point_2d"]
                .str.split_exact(",", 1)
                .struct.rename_fields(["latitude", "longitude"])
                .alias("point_coords")
            ]
        ).unnest("point_coords")

        geometry = pl.Series(
            [
                json.dumps(mapping(geom))
                for geom in gpd.points_from_xy(
                    dfcoords["longitude"], dfcoords["latitude"], crs="EPSG:2154"
                )
            ]
        )

        def reverse_geocode_label(point):
            logger.info(f"Reverse geocoding {point}...")
            try:
                resp = _get_reverse_label(
                    client=self.client,
                    body=PostApiNearbyStreetsBody(
                        geometry=PostApiNearbyStreetsBodyGeometry.from_dict(json.loads(point))
                    ),
                )
            except Exception:
                logger.warning(f"Failed to reverse geocode {point}")
                return "Rue inconnue"

            if resp.parsed is None:
                logger.warning(f"Failed to reverse geocode {point}")
                return "Rue inconnue"

            found_nearby: list[PostApiNearbyStreetsResponse200Item] = resp.parsed  # type: ignore

            if len(found_nearby) > 0:
                road_name = found_nearby[0].road_name
                logger.info(f"Found a street : {road_name}")
                return road_name
            return "Rue inconnue"

        df = df.with_columns([geometry.map_elements(reverse_geocode_label).alias("location_label")])

        return df.with_columns(
            [
                # Road type (always RAWGEOJSON as enum string value)
                pl.lit(RoadTypeEnum.RAWGEOJSON.value).alias("location_road_type"),
                geometry.alias("location_geometry"),
            ]
        )


def compute_measure_fields(df: pl.DataFrame):
    return df.with_columns(
        [
            pl.lit(MeasureTypeEnum.NOENTRY.value).alias("measure_type_"),
        ]
    )


def compute_period_fields(df: pl.DataFrame):
    """
    Compute all period fields for SavePeriodDTO.
    - period_start_date: from datetime_start
    - period_end_date: from datetime_end
    - period_start_time: from datetime_start
    - period_end_time: from datetime_end
    - period_recurrence_type: everyDay
    - period_is_permanent: True
    """
    return df.with_columns(
        [
            pl.col("datetime_start")
            .str.to_datetime("%d/%m/%Y %H:%M")
            .dt.to_string("iso")
            .alias("period_start_date"),
            pl.col("datetime_end")
            .str.to_datetime("%d/%m/%Y %H:%M")
            .dt.to_string("iso")
            .alias("period_end_date"),
            pl.col("datetime_start")
            .str.to_datetime("%d/%m/%Y %H:%M")
            .dt.to_string("iso")
            .alias("period_start_time"),
            pl.col("datetime_end")
            .str.to_datetime("%d/%m/%Y %H:%M")
            .dt.to_string("iso")
            .alias("period_end_time"),
            pl.lit("everyDay").alias("period_recurrence_type"),
            pl.lit(True).alias("period_is_permanent"),
        ]
    )


def compute_regulation_fields(df: pl.DataFrame):
    """
    Compute all regulation fields for PostApiRegulationsAddBody.
    - regulation_identifier: from reference
    - regulation_category: TEMPORARYREGULATION
    - regulation_subject: ROADMAINTENANCE
    - regulation_title: title

    Filters out rows with duplicate objectid.
    """
    return df.with_columns(
        [
            (pl.lit("21231/") + pl.col("reference").cast(pl.Utf8) + pl.lit("/travaux")).alias(
                "regulation_identifier"
            ),
            pl.lit(PostApiRegulationsAddBodyCategory.TEMPORARYREGULATION.value).alias(
                "regulation_category"
            ),
            pl.lit(PostApiRegulationsAddBodySubject.ROADMAINTENANCE.value).alias(
                "regulation_subject"
            ),
            pl.col("title").alias("regulation_title"),
            pl.lit("Chantier").alias("regulation_other_category_text"),
        ]
    )


def compute_vehicle_fields(df: pl.DataFrame):
    """
    Compute all vehicle fields for SaveVehicleSetDTO.
    - vehicle_all_vehicles: true
    """
    return df.with_columns([pl.lit(True).alias("vehicle_all_vehicles")])
