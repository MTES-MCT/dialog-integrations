"""Data source integration for Aveyron : prescriptions-routieres-du-departement"""

import io
from datetime import date

import polars as pl
import requests
from loguru import logger

from api.dia_log_client.models import (
    DirectionEnum,
    MeasureTypeEnum,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
)
from api.dia_log_client.models import (
    PostApiRegulationsAddBodyMeasuresItemVehicleSetType0RestrictedTypesType0Item as VehicleRestrictedTypeEnum,  # noqa
)
from integrations.base_data_source_integration import BaseDataSourceIntegration
from integrations.dp_aveyron.restrictions_gabarits.schema import (
    AveyronPrescriptionsRoutieresRawDataSchema,
)
from integrations.local_time import start_of_local_day

URL = "https://opendata.aveyron.fr/api/explore/v2.1/catalog/datasets/prescriptions-routieres-du-departement-aveyron/exports/parquet"
LOCAL_FILE = "explorations/dp_aveyron/data/prescriptions-routieres-du-departement-aveyron.parquet"


class DataSourceIntegration(BaseDataSourceIntegration):
    """Data source for Prescription Routière du Département"""

    raw_data_schema = AveyronPrescriptionsRoutieresRawDataSchema
    name = "restrictions_gabarits"

    def fetch_raw_data(self):
        logger.info(f"Downloading data from {URL}")

        r = requests.get(URL)
        r.raise_for_status()

        df = pl.read_parquet(io.BytesIO(r.content))

        return df

    def compute_clean_data(self, raw_data):
        return (
            raw_data.pipe(unnest_measures)
            .pipe(filter_unrelevant)
            .pipe(compute_measure_fields)
            .pipe(compute_period_fields)
            .pipe(compute_location_fields)
            .pipe(compute_regulation_fields)
            .pipe(compute_vehicle_fields)
        )


def unnest_measures(df: pl.DataFrame):
    """
    We unnest all records based on the "panneau" column, and format it accordingly
    """
    return (
        df.with_columns(pl.col("panneau").str.split(";").alias("panneau"))
        .explode("panneau")
        .with_columns(
            pl.col("panneau")
            .str.split_exact("_", 1)
            .struct.rename_fields(["panneau_type", "panneau_value"])
            .alias("restriction_fields")
        )
        .unnest("restriction_fields")
        .with_columns(pl.col("panneau_type").str.strip_chars())
        .with_columns(
            pl.col("panneau_value")
            .str.strip_chars("mt²")
            .str.replace_all(",", ".")
            .cast(pl.Float64)
            .alias("panneau_value")
        )
    )


# Pedestrians and cyclists are the only signs left out: DiaLog has no vehicle type for
# them, and `other` would describe a restriction on traffic as one on vehicles.
PANNEAUX = {
    "B9i": "Interdiction caravanes",
    "B18c": "Interdiction Transport Matieres dangereuse",
    "B10a": "Limitation de longueur",
    "B9f": "Limitation de longueur bus",
    "B11": "Limitation de largeur",
    "B12": "Limitation de hauteur",
    "B13": "Limitation de tonnage",
}


def filter_unrelevant(df: pl.DataFrame):
    """
    Keep only rows that are relevant to DiaLog
    """
    return df.filter(pl.col("panneau_type").is_in(PANNEAUX.keys()))


def compute_measure_fields(df: pl.DataFrame):
    """Every row left by filter_unrelevant carries a sign, so every measure is a
    prohibition. This source holds no speed limit, and no measure_max_speed."""
    return df.with_columns(pl.lit(MeasureTypeEnum.NOENTRY.value).alias("measure_type_"))


def compute_period_fields(df: pl.DataFrame):
    """
    Compute all period fields for SavePeriodDTO.
    - period_start_date: from date_darre, the day the arrete was signed, else today
    - period_end_date: None
    - period_recurrence_type: everyDay
    - period_is_permanent: True

    The producer writes dd/mm/yyyy, with a few dd/mm/yy. Inferring the format lets polars
    read "12/01/18" as year 18, so both formats are stated explicitly.

    A third of the rows carry no date at all. They describe a restriction that is signed,
    signposted and in force; only the day it was signed is missing. They are integrated
    with the day of the run rather than dropped, on the same footing as the speed limits,
    which have no date of their own either. The regulation being permanent and open-ended,
    the start date only says "this applies now".
    """
    parsed = (
        pl.when(pl.col("date_darre").str.len_chars() <= 8)
        .then(pl.col("date_darre").str.to_date("%d/%m/%y", strict=False))
        .otherwise(pl.col("date_darre").str.to_date("%d/%m/%Y", strict=False))
    )
    n_dateless = df.select(parsed.is_null().sum()).item()
    if n_dateless > 0:
        logger.info(f"Dating {n_dateless}/{df.height} rows with no readable date_darre from today")

    df = df.with_columns(parsed.fill_null(date.today()).alias("_start_date"))
    return df.with_columns(
        [
            start_of_local_day(df, "_start_date").alias("period_start_date"),
            pl.lit(None).alias("period_end_date"),
            pl.lit("everyDay").alias("period_recurrence_type"),
            pl.lit(True).alias("period_is_permanent"),
        ]
    )


def compute_location_fields(df: pl.DataFrame):
    """
    Compute all location fields for SaveLocationDTO.
    - location_administrator: "Aveyron"
    - location_road_type: RoadTypeEnum.DEPARTMENTALROAD
    - location_road_number: from route, e.g. 12_D98 -> D98
    - location_from_department_code: 12
    - location_from_point_number: from prd
    - location_from_abscissa: from abd
    - location_from_side: "U"
    - location_to_department_code: 12
    - location_to_point_number: from prf
    - location_to_abscissa: from abf
    - location_to_side: "U"
    - location_direction: "BOTH"
    #NOT TRANSMITTTED- location_geometry: from geo_shape
    """

    return df.with_columns(
        [
            pl.lit("Aveyron").alias("location_administrator"),
            pl.lit(RoadTypeEnum.DEPARTMENTALROAD.value).alias("location_road_type"),
            pl.col("route").str.split("_").list.last().alias("location_road_number"),
            pl.lit("12").alias("location_from_department_code"),
            pl.col("prd").cast(pl.Utf8).alias("location_from_point_number"),
            pl.col("abd").round().cast(pl.Int64).alias("location_from_abscissa"),
            pl.lit("U").alias("location_from_side"),
            pl.lit("12").alias("location_to_department_code"),
            pl.col("prf").cast(pl.Utf8).alias("location_to_point_number"),
            pl.col("abf").round().cast(pl.Int64).alias("location_to_abscissa"),
            pl.lit("U").alias("location_to_side"),
            pl.lit(DirectionEnum.BOTH.value).alias("location_direction"),
        ]
    )


MAX_IDENTIFIER_LENGTH = 60  # API contract
# What the producer writes when there is no arrete to refer to. Treated as an absence,
# otherwise every one of them lands in a single catch-all regulation.
PLACEHOLDER_REFERENCES = ["0arrete", "arlot"]
TITLE_MAX_LENGTH = 255
TITLE_ELLIPSIS = "..."
OTHER_CATEGORY_TEXT = "Restriction de gabarit"


def normalize_reference(reference: pl.Expr) -> pl.Expr:
    """Reduce a producer reference to what survives a URL path unescaped.

    Identifiers travel in the path of DELETE /api/regulations/{identifier} and of the
    publish endpoint, so anything that is not a letter or a digit becomes a single hyphen.
    """
    return reference.str.replace_all(r"[^A-Za-z0-9]+", "-").str.strip_chars("-")


def compute_regulation_fields(df: pl.DataFrame):
    """
    Compute all regulation fields for PostApiRegulationsAddBody.
    - regulation_identifier: "gabarit-{numero_dar}" when the producer gives an arrete
      number. A third of the rows have none, or write "0 arrete" to say there is none, so
      those are identified by the stretch itself,
      "gabarit-{road}-de-{fromPR}+{fromAbscissa}-a-{toPR}+{toAbscissa}"
      (R-20 level 3, written out here per R-23). Both forms stay under the 60-character cap.
    - regulation_category: PERMANENTREGULATION
    - regulation_subject: OTHER
    - regulation_title: the prescriptions of the group, and the stretch when there is no
      arrete number to name
    - regulation_other_category_text: "Restriction de gabarit"

    Nothing is dropped here. One arrete can carry several panneaux, so the title lists
    every prescription of the group: the API keeps the title of the first row only, and a
    single panneau would misdescribe the rest. `prescripti` is too long to serve as
    otherCategoryText once joined (the API caps it at 100), so that field states the
    subject instead.
    """
    reference = normalize_reference(pl.col("numero_dar"))
    is_placeholder = (
        pl.col("numero_dar")
        .str.to_lowercase()
        .str.replace_all(" ", "")
        .is_in(PLACEHOLDER_REFERENCES)
    )
    has_reference = reference.is_not_null() & (reference != "") & ~is_placeholder.fill_null(False)

    n_reconstructed = df.select((~has_reference).sum()).item()
    if n_reconstructed > 0:
        logger.info(f"Identifying {n_reconstructed}/{df.height} rows by their stretch")

    stretch = (
        pl.col("location_road_number")
        + pl.lit("-de-")
        + pl.col("location_from_point_number")
        + pl.lit("+")
        + pl.col("location_from_abscissa").cast(pl.Utf8)
        + pl.lit("-a-")
        + pl.col("location_to_point_number")
        + pl.lit("+")
        + pl.col("location_to_abscissa").cast(pl.Utf8)
    )
    df = df.with_columns(
        (pl.lit("gabarit-") + pl.when(has_reference).then(reference).otherwise(stretch)).alias(
            "regulation_identifier"
        )
    )

    n_too_long = df.select(
        (pl.col("regulation_identifier").str.len_chars() > MAX_IDENTIFIER_LENGTH).sum()
    ).item()
    if n_too_long > 0:
        logger.error(f"{n_too_long} identifiers exceed {MAX_IDENTIFIER_LENGTH} characters")

    prescriptions = pl.col("prescripti").fill_null("").unique().sort().str.join(" ; ")
    title = (
        pl.when(has_reference)
        .then(reference + pl.lit(" - ") + prescriptions.over("regulation_identifier"))
        .otherwise(
            prescriptions.over("regulation_identifier")
            + pl.lit(" - ")
            + pl.col("location_road_number")
            + pl.lit(", du PR ")
            + pl.col("location_from_point_number")
            + pl.lit("+")
            + pl.col("location_from_abscissa").cast(pl.Utf8)
            + pl.lit(" au PR ")
            + pl.col("location_to_point_number")
            + pl.lit("+")
            + pl.col("location_to_abscissa").cast(pl.Utf8)
        )
    )

    return df.with_columns(
        [
            pl.lit(PostApiRegulationsAddBodyCategory.PERMANENTREGULATION.value).alias(
                "regulation_category"
            ),
            pl.lit(PostApiRegulationsAddBodySubject.OTHER.value).alias("regulation_subject"),
            pl.when(title.str.len_chars() > TITLE_MAX_LENGTH)
            .then(
                title.str.slice(0, TITLE_MAX_LENGTH - len(TITLE_ELLIPSIS)) + pl.lit(TITLE_ELLIPSIS)
            )
            .otherwise(title)
            .alias("regulation_title"),
            pl.lit(OTHER_CATEGORY_TEXT).alias("regulation_other_category_text"),
        ]
    )


def compute_vehicle_fields(df: pl.DataFrame):
    """
    Compute all vehicle fields for SaveVehicleSetDTO.
    - vehicle_all_vehicles: always false, every measure targets a category of vehicle
    - vehicle_restricted_types :
        heavyGoodsVehicle if B13
        hazardousMaterials if B18c
        other if B9i
        other and dimensions if B9f
        dimensions otherwise
    - vehicle_heavyweight_max_weight if B13
    - vehicle_max_height if B12
    - vehicle_max_width if B11
    - vehicle_max_length if B10a or B9f
    - vehicle_other_restricted_type_text : "Bus" if B9f, "Caravanes" if B9i
    """
    return df.with_columns(
        [
            pl.lit(False).alias("vehicle_all_vehicles"),
            pl.when(pl.col("panneau_type") == "B13")
            .then(pl.lit([VehicleRestrictedTypeEnum.HEAVYGOODSVEHICLE.value]))
            .when(pl.col("panneau_type") == "B18c")
            .then(pl.lit([VehicleRestrictedTypeEnum.HAZARDOUSMATERIALS.value]))
            .when(pl.col("panneau_type") == "B9i")
            .then(pl.lit([VehicleRestrictedTypeEnum.OTHER.value]))
            .when(pl.col("panneau_type") == "B9f")
            .then(
                pl.lit(
                    [
                        VehicleRestrictedTypeEnum.DIMENSIONS.value,
                        VehicleRestrictedTypeEnum.OTHER.value,
                    ]
                )
            )
            .otherwise(pl.lit([VehicleRestrictedTypeEnum.DIMENSIONS.value]))
            .alias("vehicle_restricted_types"),
        ]
    ).with_columns(
        [
            pl.when(pl.col("panneau_type") == "B13")
            .then(pl.col("panneau_value"))
            .alias("vehicle_heavyweight_max_weight"),
            pl.when(pl.col("panneau_type") == "B12")
            .then(pl.col("panneau_value"))
            .alias("vehicle_max_height"),
            pl.when(pl.col("panneau_type") == "B11")
            .then(pl.col("panneau_value"))
            .alias("vehicle_max_width"),
            pl.when(pl.col("panneau_type").is_in(["B10a", "B9f"]))
            .then(pl.col("panneau_value"))
            .alias("vehicle_max_length"),
            pl.when(pl.col("panneau_type") == "B9f")
            .then(pl.lit("Bus"))
            .when(pl.col("panneau_type") == "B9i")
            .then(pl.lit("Caravanes"))
            .alias("vehicle_other_restricted_type_text"),
        ]
    )
