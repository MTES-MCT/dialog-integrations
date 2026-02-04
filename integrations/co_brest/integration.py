import json
import tempfile
import zipfile
from collections import defaultdict
from pathlib import Path
from typing import Any, cast

import geopandas as gpd
import polars as pl
import requests
from loguru import logger
from pyproj import Transformer
from shapely import wkt
from shapely.geometry import mapping
from shapely.ops import transform

from api.dia_log_client.models import (
    MeasureTypeEnum,
    PostApiRegulationsAddBody,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodyStatus,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
    SaveLocationDTO,
    SaveMeasureDTO,
    SaveRawGeoJSONDTO,
    SaveVehicleSetDTO,
)
from integrations.co_brest.schema import (
    DESCRIPTION_CONFIG,
    BrestMeasure,
    BrestRawDataSchema,
)
from integrations.shared import DialogIntegration

URL = "https://www.data.gouv.fr/api/1/datasets/r/3ca7bd06-6489-45a2-aee9-efc6966121b2"
FILENAME = "DEP_ARR_CIRC_STAT_L_V.shp"

transformer = Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)


class Integration(DialogIntegration):
    draft = False
    raw_data_schema = BrestRawDataSchema

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

    def preprocess_raw_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        """
        Apply Brest-specific preprocessing: cast boolean columns.
        """
        return raw_data.with_columns(
            [
                self.cast_boolean_column("CYCLO"),
                self.cast_boolean_column("VELO"),
            ]
        )

    def compute_clean_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        return (
            raw_data.filter(pl.col("DESCRIPTIF").is_in(DESCRIPTION_CONFIG.keys()))
            .filter(
                ~(pl.col("DESCRIPTIF").eq("Sens interdit / Sens unique") & pl.col("SENS").eq(1))
            )
            .filter(~(pl.col("NOARR").eq("")))
            .pipe(compute_save_period_fields)
        )

    def cast_boolean_column(self, column_name: str) -> pl.Expr:
        return (
            pl.when(pl.col(column_name).str.to_uppercase() == "OUI")
            .then(True)
            .when(pl.col(column_name).str.to_uppercase() == "NON")
            .then(False)
            .cast(pl.Boolean)
            .alias(column_name)
            .fill_null(False)
        )

    def create_regulations(self, clean_data: pl.DataFrame) -> list[PostApiRegulationsAddBody]:
        # First, group measures by regulation ID
        regulation_id_to_measures = defaultdict(list)
        for measure_row in clean_data.sort("NOARR").iter_rows(named=True):
            try:
                measure_row = cast(BrestMeasure, measure_row)
                measure = self.create_measure(measure_row)
                regulation_id_to_measures[measure_row["NOARR"]].append(measure)
            except Exception as e:
                logger.error(f"Error creating measure for regulation {measure_row['NOARR']}: {e}")

        # Then, create regulation payloads
        regulations = []
        for regulation_id, measures in regulation_id_to_measures.items():
            # Get first measure row to build title
            try:
                first_row = (
                    clean_data.filter(pl.col("NOARR") == regulation_id).head(1).row(0, named=True)
                )
            except Exception:
                raise ValueError(f"Could not find first row for regulation {regulation_id}")

            title = f"{first_row['DESCRIPTIF']} – {first_row['LIBRU']}"
            status = (
                PostApiRegulationsAddBodyStatus.DRAFT
                if self.draft
                else PostApiRegulationsAddBodyStatus.PUBLISHED
            )

            # Build payload
            regulations.append(
                PostApiRegulationsAddBody(
                    identifier=regulation_id,
                    category=PostApiRegulationsAddBodyCategory.PERMANENTREGULATION,
                    status=status,
                    subject=PostApiRegulationsAddBodySubject.OTHER,
                    title=title,
                    other_category_text="Circulation",
                    measures=measures,  # type: ignore
                )
            )

        return regulations

    def create_measure(self, measure: BrestMeasure) -> SaveMeasureDTO:
        cfg = DESCRIPTION_CONFIG.get(measure["DESCRIPTIF"], {})

        params = {
            "type_": cfg["measure_type"],
            "max_speed": measure["VITEMAX"],
            "periods": [self.create_save_period_dto(measure)],  # type: ignore
            "locations": [self.create_save_location_dto(measure)],  # type: ignore
            "vehicle_set": self.create_save_vehicle_dto(
                measure, cfg.get("exempted_types"), cfg.get("restricted_types")
            ),
        }

        if params["type_"] == MeasureTypeEnum.SPEEDLIMITATION:
            assert params["max_speed"] is not None, (
                "VITEMAX must be defined for speed limitation measures"
            )
            assert params["max_speed"] > 0, "VITEMAX must be greater than 0"
        else:
            del params["max_speed"]

        return SaveMeasureDTO(**params)

    def create_save_location_dto(self, measure: BrestMeasure) -> SaveLocationDTO:
        geom_wkt = measure["geometry"]
        assert geom_wkt is not None, "geometry must be defined"
        geom_wgs84 = transform(transformer.transform, wkt.loads(geom_wkt))

        geometry = json.dumps(mapping(geom_wgs84))

        return SaveLocationDTO(
            road_type=RoadTypeEnum.RAWGEOJSON,
            raw_geo_json=SaveRawGeoJSONDTO(
                label=f"{measure['LIBCO']} – {measure['LIBRU']}",
                geometry=geometry,
            ),
        )

    def create_save_vehicle_dto(
        self,
        measure: BrestMeasure,
        exempted_types: list[str] | None,
        restricted_types: list[str] | None,
    ) -> SaveVehicleSetDTO:
        def to_float(val):
            if val is None or val == 0:
                return None
            try:
                return float(val)
            except Exception:
                return None

        # Dimensions
        poids = to_float(measure.get("POIDS"))
        hauteur = to_float(measure.get("HAUTEUR"))
        largeur = to_float(measure.get("LARGEUR"))

        # Start building params
        params: dict[str, Any] = {
            "all_vehicles": True,
            "heavyweight_max_weight": poids,
            "max_height": hauteur,
            "max_width": largeur,
            "exempted_types": exempted_types,
            "restricted_types": restricted_types,
        }

        # Auto-fill exempted types from columns
        if params["exempted_types"] is None:
            params["exempted_types"] = []
            if measure.get("CYCLO") is True:
                params["exempted_types"].append("other")
            if measure.get("VELO") is True:
                params["exempted_types"].append("bicycle")

        if params["exempted_types"]:
            if "other" in params["exempted_types"]:
                params["other_exempted_type_text"] = "cyclomoteur"
            else:
                params["other_exempted_type_text"] = "autres véhicules autorisés"

        if params["heavyweight_max_weight"]:
            params["restricted_types"] = ["heavyGoodsVehicle"]

        # If we have dimensions or exemptions or restrictions → not all vehicles
        if params["restricted_types"]:
            params["all_vehicles"] = False
        else:
            params["all_vehicles"] = True

        # Clean params: remove empty lists / None
        cleaned = {k: v for k, v in params.items() if v not in (None, [], {})}

        return SaveVehicleSetDTO(**cleaned)


def compute_save_period_fields(df: pl.DataFrame) -> pl.DataFrame:
    """
    Compute all period fields for SavePeriodDTO.
    - period_start_date: from DT_MAT field
    - period_end_date, period_start_time, period_end_time: None
    - period_recurrence_type: EVERYDAY
    - period_is_permanent: True
    Filter out rows where DT_MAT is null.
    """
    # Count rows with null DT_MAT before filtering
    n_null_dt_mat = df.select(pl.col("DT_MAT").is_null().sum()).item()
    if n_null_dt_mat > 0:
        logger.warning(f"Dropping {n_null_dt_mat} rows with null DT_MAT (no start date available)")

    # Filter out rows where DT_MAT is null
    df = df.filter(pl.col("DT_MAT").is_not_null())

    # Compute all period fields
    return df.with_columns(
        [
            pl.col("DT_MAT").dt.strftime("%Y-%m-%dT%H:%M:%SZ").alias("period_start_date"),
            pl.lit(None).alias("period_end_date"),
            pl.lit(None).alias("period_start_time"),
            pl.lit(None).alias("period_end_time"),
            pl.lit("everyDay").alias("period_recurrence_type"),
            pl.lit(True).alias("period_is_permanent"),
        ]
    )
