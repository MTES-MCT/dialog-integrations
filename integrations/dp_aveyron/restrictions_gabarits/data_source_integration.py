"""Data source integration for Aveyron : prescriptions-routieres-du-departement"""

import io

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
from integrations.shared.local_time import start_of_local_day

URL = "https://opendata.aveyron.fr/api/explore/v2.1/catalog/datasets/prescriptions-routieres-du-departement-aveyron/exports/parquet"
LOCAL_FILE = "explorations/dp_aveyron/data/prescriptions-routieres-du-departement-aveyron.parquet"


class DataSourceIntegration(BaseDataSourceIntegration):
    """Data source for Prescription Routière du Département"""

    raw_data_schema = AveyronPrescriptionsRoutieresRawDataSchema
    name = "restrictions_gabarits"

    # R-28: one arrete carries one measure per distinct sign-and-value, and that measure
    # carries every stretch it applies to.
    group_locations_by_measure = True

    def fetch_raw_data(self):
        logger.info(f"Downloading data from {URL}")

        r = requests.get(URL)
        r.raise_for_status()

        df = pl.read_parquet(io.BytesIO(r.content))

        return df

    def compute_clean_data(self, raw_data):
        return (
            raw_data.pipe(unnest_measures)
            # One row per sign a record lists: the unit of the retention rate.
            .pipe(self.count_dataset_restrictions)
            .pipe(filter_unrelevant)
            .pipe(compute_measure_fields)
            .pipe(compute_location_fields)
            # The period comes after the regulation: a measure carries one period, so
            # dating it needs to know which group of rows it gathers (R-28).
            .pipe(compute_regulation_fields)
            .pipe(compute_period_fields)
            .pipe(compute_vehicle_fields)
        )


def unnest_measures(df: pl.DataFrame):
    """One row per sign: `panneau` reads `B13_3.5t; B11_2.2m²`, split into type and value."""
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
    """Keep the signs listed in PANNEAUX."""
    return df.filter(pl.col("panneau_type").is_in(PANNEAUX.keys()))


# The unit each sign states its value in. `B9i` (caravans) and `B18c` (hazardous goods)
# carry no value: the sign is the whole measure.
MEASURE_UNITS = {
    "B13": "t",
    "B10a": "m",
    "B9f": "m",
    "B11": "m",
    "B12": "m",
}


def sign_value() -> pl.Expr:
    """`3.5`, `19` — the value the sign states, without the float's trailing zero.

    The producer stores tonnages and heights as floats, so 19 t arrives as `19.0`. It
    reaches a human in the title of a regulation and a GPS through the rebroadcast, and
    neither reads `19.0 t` as a tonnage.
    """
    return pl.col("panneau_value").cast(pl.String).str.replace(r"\.0$", "")


def measure_group_key() -> pl.Expr:
    """`B13-3-5` — the sign and the value it states, which is exactly what the measure is.

    Two stretches share it only when they carry the same sign at the same value. Rows
    sharing it inside one regulation collapse into a single measure carrying N emprises
    (R-28). The decimal point becomes a hyphen so the key survives a URL path, where the
    identifier of a fallback regulation travels.
    """
    return (
        pl.when(pl.col("panneau_value").is_null())
        .then(pl.col("panneau_type"))
        .otherwise(pl.col("panneau_type") + pl.lit("-") + sign_value().str.replace(r"\.", "-"))
    )


def measure_label() -> pl.Expr:
    """`Limitation de tonnage 3.5 t` — the same measure, spelled out for a human."""
    label = (
        pl.col("panneau_type")
        .replace_strict(PANNEAUX, default=None, return_dtype=pl.Utf8)
        .fill_null(pl.col("panneau_type"))
    )
    unit = pl.col("panneau_type").replace_strict(MEASURE_UNITS, default=None, return_dtype=pl.Utf8)
    return (
        pl.when(pl.col("panneau_value").is_null() | unit.is_null())
        .then(label)
        .otherwise(label + pl.lit(" ") + sign_value() + pl.lit(" ") + unit)
    )


def compute_measure_fields(df: pl.DataFrame):
    """Every row left by filter_unrelevant carries a sign, so every measure is a
    prohibition. This source holds no speed limit, and no measure_max_speed."""
    return df.with_columns(
        pl.lit(MeasureTypeEnum.NOENTRY.value).alias("measure_type_"),
        measure_group_key().alias("measure_group_key"),
    )


def compute_period_fields(df: pl.DataFrame):
    """Permanent, every day, no end; the start is the day the arrete was signed, or null.

    The producer writes dd/mm/yyyy, with a few dd/mm/yy. Inferring the format lets polars
    read "12/01/18" as year 18, so both formats are stated explicitly.

    **A measure carries one period and N emprises (R-28)**, so the date has to be uniform
    over the group:

    - a *numbered* arrete has one signature date, and the group takes the earliest of its
      rows: when the producer writes two, the earliest is the day from which the measure
      has demonstrably applied;
    - a *grouped fallback* gathers stretches from different acts (one spans 2003 to 2019).
      Picking one member's date would assert a commencement date for the other N-1 that
      nothing supports. R-39: no date at all.

    Rows with no readable date are left undated either way: signed, signposted and in
    force, only the day of signature is missing. `integrations/sync/dating.py` resolves
    the null when DiaLog is written: the day of the run on creation ("this applies now"),
    the date DiaLog already holds on update. Dating them the day of the run here would
    make them look modified each morning.
    """
    parsed = (
        pl.when(pl.col("date_darre").str.len_chars() <= 8)
        .then(pl.col("date_darre").str.to_date("%d/%m/%y", strict=False))
        .otherwise(pl.col("date_darre").str.to_date("%d/%m/%Y", strict=False))
    )
    signed = (
        pl.when(pl.col("_has_reference"))
        .then(parsed.min().over(["regulation_identifier", "measure_group_key"]))
        .otherwise(None)
    )

    n_dateless = df.select(signed.is_null().sum()).item()
    if n_dateless > 0:
        logger.info(
            f"Leaving {n_dateless}/{df.height} rows undated: no readable date_darre, or a "
            "grouped fallback whose members come from different acts"
        )

    df = df.with_columns(signed.alias("_start_date"))
    return df.with_columns(
        [
            start_of_local_day(df, "_start_date").alias("period_start_date"),
            pl.lit(None).alias("period_end_date"),
            pl.lit("everyDay").alias("period_recurrence_type"),
            pl.lit(True).alias("period_is_permanent"),
        ]
    )


def compute_location_fields(df: pl.DataFrame):
    """Departmental-road location: `route` 12_D98 -> D98, PR from prd/abd to prf/abf.

    `geo_shape` is not sent: DiaLog geocodes the stretch from its milestones.
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

# See `limitations_vitesse` for why the batch is prefixed (R-29). `GB` is this source.
IDENTIFIER_PREFIX = "AV-GB"
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
    """Identifier, title and category of the permanent regulation (R-28; R-20 level 3,
    written out here per R-23).

        AV-GB-{numero_dar}   when the producer gives an arrete number — every stretch
                             citing it becomes an emprise of that arrete
        AV-GB-{B13-3-5}      otherwise, one departmental arrete per sign-and-value,
                             carrying N emprises

    Some rows carry no number, or write "0 arrete" to say there is none. Identifying
    those by the stretch itself fabricates one legal act per section of road, a falsehood
    that travels to the satnavs that rebroadcast us. Both forms stay under the
    60-character cap, and neither carries the road or the commune: grouping
    geographically would move the identifier the first time the producer redraws a
    trace, orphaning the arrete and duplicating it.

    Nothing is dropped here. One arrete can carry several panneaux, so the title lists
    every prescription of the group: the API keeps the title of the first row only, and a
    single panneau would misdescribe the rest. `prescripti` is too long to serve as
    otherCategoryText once joined (the API caps it at 100), so that field states the
    subject instead.

    `_has_reference` is carried out of here for `compute_period_fields`, which dates a
    numbered arrete and a grouped fallback differently. The underscore keeps it out of
    the pivot: `select_regulation_measure_fields` drops it.
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

    df = df.with_columns(
        has_reference.alias("_has_reference"),
        (
            pl.lit(f"{IDENTIFIER_PREFIX}-")
            + pl.when(has_reference).then(reference).otherwise(measure_group_key())
        ).alias("regulation_identifier"),
    )

    n_too_long = df.select(
        (pl.col("regulation_identifier").str.len_chars() > MAX_IDENTIFIER_LENGTH).sum()
    ).item()
    if n_too_long > 0:
        logger.error(f"{n_too_long} identifiers exceed {MAX_IDENTIFIER_LENGTH} characters")

    prescriptions = pl.col("prescripti").fill_null("").unique().sort().str.join(" ; ")
    sections = pl.format(
        "{} section{}", pl.len(), pl.when(pl.len() > 1).then(pl.lit("s")).otherwise(pl.lit(""))
    )
    title = (
        pl.when(has_reference)
        .then(reference + pl.lit(" - ") + prescriptions.over("regulation_identifier"))
        # A grouped fallback is named after what it does: every stretch the department
        # signs with this sign at this value, without citing an arrete. Naming one of its
        # stretches would describe one emprise and misdescribe the other N-1.
        .otherwise(
            measure_label()
            + pl.lit(" - Département de l'Aveyron (")
            + sections.over("regulation_identifier")
            + pl.lit(")")
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
    """Every measure targets a category of vehicle, named by its sign (see PANNEAUX)."""
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
