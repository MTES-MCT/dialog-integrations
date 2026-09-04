"""Data source integration for Métropole de Lyon: chaussées et trottoirs.

The layer is a GIS inventory of the road network — 37 569 segments, one row per segment.
A row is **not** an order: it is a piece of road, and it carries whatever is in force
there. Turning each row into a DiaLog regulation would fabricate 37 569 administrative
acts that do not exist, so rows are grouped instead (R-28):

- when the free-text field yields an order number, every row citing it becomes an
  emprise of the same regulation, whatever municipality it falls in;
- otherwise, rows carrying the *same measure* become a single metropolitan-wide
  regulation — one "30 km/h across the Métropole de Lyon" holding N emprises.

A row can feed two measures at once: its speed limit and its dimension limit. They are
qualified separately, so discarding one never discards the other, and the same segment
legitimately appears in two regulations.

Measured on the 2026-08-12 snapshot: 25 171 emprises → 990 measures → 677 regulations,
of which 599 carry a real order number and 78 are metropolitan-wide fallbacks.
"""

import datetime
import json

import polars as pl
from loguru import logger

from api.dia_log_client.models import (
    MeasureTypeEnum,
    PostApiRegulationsAddBodyCategory,
    PostApiRegulationsAddBodySubject,
    RoadTypeEnum,
)
from api.dia_log_client.models import (
    PostApiRegulationsAddBodyMeasuresItemVehicleSetType0RestrictedTypesType0Item as VehicleRestrictedTypeEnum,  # noqa: E501
)
from integrations.base_data_source_integration import BaseDataSourceIntegration
from integrations.co_lyon.chaussees_trottoirs.regulation_key import (
    is_project,
    mentions_dimensions,
    order_key,
    order_number,
)
from integrations.co_lyon.chaussees_trottoirs.schema import LyonChausseesTrottoirsRawDataSchema
from integrations.shared.wfs import LYON_BBOX, assert_lon_lat_bbox, fetch_wfs_features

WFS_LAYER = "pvo_patrimoine_voirie.pvochausseetrottoir"

# Every identifier we create is prefixed, so our batch stays recognisable and removable
# in one go. The organisation already holds 813 Lyon orders pushed by a channel absent
# from this repository, whose identifiers use 52 municipality prefixes for ~40
# municipalities — there is no convention to align with (R-29, P-04 still open).
IDENTIFIER_PREFIX = "MDL-CT"

# 50 km/h is the default speed in a French built-up area, not a measure anyone decided.
# It covers 14 638 segments; publishing it would nearly double the volume with an
# information no order carries.
DEFAULT_URBAN_SPEED = "50"

# The source files a pedestrian area as a 5 km/h speed limit. A pedestrian area is first
# a ban on driving, and "5 km/h" would read as a speed advisory on a GPS. Business call
# of 2026-09-04: publish it as `noEntry`, pending the product team's answer on the
# exemptions (residents, deliveries, emergency services) which the source does not carry.
PEDESTRIAN_AREA_SPEED = "5"

# Does an order number read off the free-text field also cover the dimension limit of
# the same row? The field describes the calmed-traffic zone, so by default it does not:
# only 46 of the 1 191 numbered dimension rows mention a dimension at all. Flip this to
# True to attach every dimension measure to the row's order number instead.
ATTACH_DIMENSION_LIMITS_TO_PARSED_ORDER = False

# Ceiling on the locations of one POST. Measured on staging on 2026-09-04: 1 500
# locations are accepted in 39 s, 2 000 die on a server-side timeout after 43 s — and
# splitting the same 2 000 across four measures fails identically, so the ceiling is per
# regulation, not per measure. 1 000 is the working margin agreed with the team; it is a
# stopgap until the API can take a larger batch.
MAX_LOCATIONS_PER_REGULATION = 1000

# Size of the latitude bands used to keep a split regulation geographically coherent:
# 0.01° is roughly 1.1 km north-south.
GEOGRAPHIC_CELL_DEGREES = 0.01

# First coordinate pair of a serialized GeoJSON geometry, used as the segment's
# representative point. Cheaper than decoding 37 569 geometries, and precise enough to
# sort segments by neighbourhood.
FIRST_COORDINATE = r"\[\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)"

DIMENSION_COLUMNS = {
    "limitationtonnage": "T",
    "limitationhauteur": "H",
    "limitationlargeur": "L",
    "limitationlongueur": "G",
}


class DataSourceIntegration(BaseDataSourceIntegration):
    """Data source for the Métropole de Lyon roadway inventory."""

    name = "chaussees_trottoirs"
    raw_data_schema = LyonChausseesTrottoirsRawDataSchema

    # One measure carries every segment it applies to, rather than one measure per
    # segment: that is what makes "30 km/h across the metropolitan area" a single act.
    group_locations_by_measure = True
    max_locations_per_regulation = MAX_LOCATIONS_PER_REGULATION

    def fetch_raw_data(self) -> pl.DataFrame:
        df = fetch_wfs_features(WFS_LAYER)
        assert_lon_lat_bbox(df, LYON_BBOX)
        return df

    def compute_clean_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        return (
            raw_data.pipe(read_order_number)
            .pipe(explode_into_measures)
            .pipe(compute_measure_fields)
            .pipe(compute_vehicle_fields)
            .pipe(compute_period_fields)
            .pipe(compute_location_fields)
            .pipe(compute_regulation_fields)
            .pipe(compute_geographic_split_order)
        )


def read_order_number(df: pl.DataFrame) -> pl.DataFrame:
    """Read the order number, its grouping key, and the two annotations that qualify it.

    Rows annotated "Proposition zone apaisable" lose their attachment — the number they
    cite belongs to a project — but they lose nothing else: their measures are real and
    join the metropolitan-wide fallbacks.
    """
    text = pl.col("precisionreglementation")
    df = df.with_columns(
        text.map_elements(order_number, return_dtype=pl.String).alias("order_number"),
        text.map_elements(is_project, return_dtype=pl.Boolean).alias("is_project"),
        text.map_elements(mentions_dimensions, return_dtype=pl.Boolean).alias("cites_dimensions"),
    ).with_columns(
        pl.when(pl.col("is_project"))
        .then(None)
        .otherwise(pl.col("order_number").map_elements(order_key, return_dtype=pl.String))
        .alias("order_key")
    )

    logger.info(
        f"Order numbers: {df.filter(pl.col('order_key').is_not_null()).height} rows attached to "
        f"{df['order_key'].n_unique()} distinct orders, "
        f"{df.filter(pl.col('is_project')).height} rows detached as projects"
    )
    return df


def explode_into_measures(df: pl.DataFrame) -> pl.DataFrame:
    """Turn one row per road segment into one row per (segment, measure).

    A segment limited to 30 km/h *and* closed to vehicles over 3.5 t states two measures.
    Qualifying them separately is what lets us drop the 50 km/h without losing the
    tonnage limit that sits on the same segment.
    """
    speeds = df.filter(
        pl.col("limitationvitesse").is_not_null()
        & (pl.col("limitationvitesse") != DEFAULT_URBAN_SPEED)
    ).with_columns(pl.lit("speed").alias("measure_kind"))

    dimensions = df.filter(
        pl.any_horizontal(pl.col(column).is_not_null() for column in DIMENSION_COLUMNS)
    ).with_columns(
        pl.lit("dimensions").alias("measure_kind"),
        # The order number describes the calmed-traffic zone; it only covers the
        # dimension limit when the text says so.
        pl.when(pl.lit(ATTACH_DIMENSION_LIMITS_TO_PARSED_ORDER) | pl.col("cites_dimensions"))
        .then(pl.col("order_key"))
        .otherwise(None)
        .alias("order_key"),
    )

    discarded = df.height - speeds.height
    logger.info(
        f"Exploded {df.height} segments into {speeds.height + dimensions.height} measures "
        f"({speeds.height} speed, {dimensions.height} dimension); "
        f"{discarded} speed measures discarded as the default {DEFAULT_URBAN_SPEED} km/h "
        f"or an empty speed"
    )
    return pl.concat([speeds, dimensions], how="vertical")


def compute_measure_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Measure type, speed, and the key that collapses identical measures into one.

    - `measure_type_`: `speedLimitation`, except for pedestrian areas and dimension
      limits which are `noEntry` (R-33 for the dimensions).
    - `measure_max_speed`: the source speed, on speed limits only.
    - `measure_group_key`: the measure's signature. Two rows sharing it inside the same
      regulation become one measure carrying both locations.
    """
    is_pedestrian_area = (pl.col("measure_kind") == "speed") & (
        pl.col("limitationvitesse") == PEDESTRIAN_AREA_SPEED
    )
    is_speed_limit = (pl.col("measure_kind") == "speed") & ~is_pedestrian_area

    return df.with_columns(
        pl.when(is_speed_limit)
        .then(pl.lit(MeasureTypeEnum.SPEEDLIMITATION.value))
        .otherwise(pl.lit(MeasureTypeEnum.NOENTRY.value))
        .alias("measure_type_"),
        pl.when(is_speed_limit)
        .then(pl.col("limitationvitesse").cast(pl.Int64))
        .otherwise(None)
        .alias("measure_max_speed"),
        pl.when(is_speed_limit)
        .then(pl.format("V{}", pl.col("limitationvitesse")))
        .when(is_pedestrian_area)
        .then(pl.lit("AIRE_PIETONNE"))
        .otherwise(dimension_signature())
        .alias("measure_group_key"),
    )


def dimension_signature() -> pl.Expr:
    """`GABARIT_T3_5_H4_1` — the dimension limits of the row, in a stable order.

    Two segments share it only when they carry exactly the same limits, which is what
    "the same measure" means for a vehicle dimension restriction.
    """
    parts = [
        pl.when(pl.col(column).is_not_null())
        .then(
            pl.format(
                "_{}{}",
                pl.lit(letter),
                pl.col(column).cast(pl.String).str.replace(r"\.", "_"),
            )
        )
        .otherwise(pl.lit(""))
        for column, letter in DIMENSION_COLUMNS.items()
    ]
    return pl.concat_str([pl.lit("GABARIT"), *parts])


def compute_vehicle_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Vehicle set of each measure.

    Speed limits and pedestrian areas apply to every vehicle. Dimension limits restrict
    heavy goods vehicles (with their tonnage) and/or the dimensions themselves — never
    invented, only carried over (R-35).
    """
    is_dimension = pl.col("measure_kind") == "dimensions"
    has_tonnage = pl.col("limitationtonnage").is_not_null()
    has_size = pl.any_horizontal(
        pl.col(column).is_not_null()
        for column in ("limitationhauteur", "limitationlargeur", "limitationlongueur")
    )

    restricted_types = (
        pl.when(has_tonnage & has_size)
        .then(
            pl.lit(
                [
                    VehicleRestrictedTypeEnum.HEAVYGOODSVEHICLE.value,
                    VehicleRestrictedTypeEnum.DIMENSIONS.value,
                ]
            )
        )
        .when(has_tonnage)
        .then(pl.lit([VehicleRestrictedTypeEnum.HEAVYGOODSVEHICLE.value]))
        .otherwise(pl.lit([VehicleRestrictedTypeEnum.DIMENSIONS.value]))
    )

    return df.with_columns(
        (~is_dimension).alias("vehicle_all_vehicles"),
        pl.when(is_dimension)
        .then(restricted_types)
        .otherwise(None)
        .alias("vehicle_restricted_types"),
        pl.when(is_dimension)
        .then(pl.col("limitationtonnage"))
        .alias("vehicle_heavyweight_max_weight"),
        pl.when(is_dimension).then(pl.col("limitationhauteur")).alias("vehicle_max_height"),
        pl.when(is_dimension).then(pl.col("limitationlargeur")).alias("vehicle_max_width"),
        pl.when(is_dimension).then(pl.col("limitationlongueur")).alias("vehicle_max_length"),
    )


def compute_period_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Permanent period starting on the day of the integration.

    The source carries no commencement date for most rows, and the one hiding in the
    free text is the date of a calmed-traffic zone, not of the measure. R-39: we never
    presume a past date. An invented date is *false* — it claims an order applied when
    nothing says it did — where today's date is merely imprecise, and reads for what it
    is: in force when we published it.
    """
    today = datetime.date.today().strftime("%Y-%m-%dT00:00:00Z")
    return df.with_columns(
        pl.lit(today).alias("period_start_date"),
        pl.lit(None).alias("period_end_date"),
        pl.lit("everyDay").alias("period_recurrence_type"),
        pl.lit(True).alias("period_is_permanent"),
    )


def compute_location_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Location of each measure, as a raw GeoJSON LineString.

    `rawGeoJSON` is the only road type that accepts a geometry of our own. It has a
    cost: `SaveRawGeoJSONDTO` carries a label and a geometry, and nothing else, so
    `senscirculation` cannot be transmitted — there is no `direction` on this road type.
    Harmless for a speed limit, which applies both ways; it would be blocking the day a
    one-way ban has to be expressed (R-32).
    """
    df = df.filter(pl.col("geometry").is_not_null())
    return df.with_columns(
        pl.lit(RoadTypeEnum.RAWGEOJSON.value).alias("location_road_type"),
        pl.concat_str(
            [pl.col("nomvoie1").fill_null("Voie sans nom"), pl.col("commune1").fill_null("")],
            separator=" – ",
        )
        .str.slice(0, 255)
        .alias("location_label"),
        pl.col("geometry").alias("location_geometry"),
    )


def compute_regulation_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Identifier, category, subject and title of each regulation.

    The identifier rule, in full (R-23):

        MDL-CT-{order key}          when the free-text field yields an order number that
                                    covers this measure — `MDL-CT-2024RP44520`
        MDL-CT-{measure signature}  otherwise, one metropolitan-wide regulation per
                                    distinct measure — `MDL-CT-V30`, `MDL-CT-GABARIT_T3_5`

    The municipality is deliberately absent from the key. Lyon's "Ville 30" order spills
    onto 12 neighbouring municipalities for a handful of boundary segments; prefixing by
    municipality would split one real order into 13. The price is that 61 short numbers
    (`845`, `956`) could merge two orders from two municipalities — one such case is
    known, and the arbitration of 2026-09-04 is to merge rather than to split.
    """
    identifier = (
        pl.when(pl.col("order_key").is_not_null())
        .then(pl.format(f"{IDENTIFIER_PREFIX}-{{}}", pl.col("order_key")))
        .otherwise(pl.format(f"{IDENTIFIER_PREFIX}-{{}}", pl.col("measure_group_key")))
    )

    return df.with_columns(
        identifier.alias("regulation_identifier"),
        pl.lit(PostApiRegulationsAddBodyCategory.PERMANENTREGULATION.value).alias(
            "regulation_category"
        ),
        pl.lit(PostApiRegulationsAddBodySubject.OTHER.value).alias("regulation_subject"),
        pl.lit("Circulation").alias("regulation_other_category_text"),
    ).with_columns(regulation_title())


def regulation_title() -> pl.Expr:
    """Human-readable title, never blocking, always fabricated (R-24).

    A numbered regulation is named after its order; a fallback is named after what it
    does, because that is literally all it is: every 30 km/h limit the metropolitan area
    publishes without an order number, gathered into one act.
    """
    numbered = pl.format("Arrêté n°{} – Métropole de Lyon", pl.col("order_number"))
    fallback = (
        pl.when(pl.col("measure_type_") == pl.lit(MeasureTypeEnum.SPEEDLIMITATION.value))
        .then(
            pl.format(
                "Limitation de vitesse à {} km/h – Métropole de Lyon",
                pl.col("limitationvitesse"),
            )
        )
        .when(pl.col("measure_group_key") == pl.lit("AIRE_PIETONNE"))
        .then(pl.lit("Aire piétonne – Métropole de Lyon"))
        .otherwise(pl.format("Restriction de gabarit {} – Métropole de Lyon", dimension_label()))
    )

    return (
        pl.when(pl.col("order_key").is_not_null())
        .then(numbered)
        .otherwise(fallback)
        .str.slice(0, 255)
        .alias("regulation_title")
    )


def dimension_label() -> pl.Expr:
    """`3.5 t · 4.1 m de hauteur` — the dimension limits, spelled out for a human."""
    labels = {
        "limitationtonnage": "{} t",
        "limitationhauteur": "{} m de hauteur",
        "limitationlargeur": "{} m de largeur",
        "limitationlongueur": "{} m de longueur",
    }
    parts = [
        pl.when(pl.col(column).is_not_null())
        .then(pl.format(" · " + template, pl.col(column)))
        .otherwise(pl.lit(""))
        for column, template in labels.items()
    ]
    return pl.concat_str(parts).str.strip_chars(" ·")


def compute_geographic_split_order(df: pl.DataFrame) -> pl.DataFrame:
    """Rank the rows so that a regulation too large for one POST splits into neighbours.

    A regulation above `MAX_LOCATIONS_PER_REGULATION` is cut into slices. Cutting it in
    source order would scatter each slice across the whole metropolitan area; cutting it
    along this ranking keeps a slice inside a municipality, and inside a municipality
    inside a ~1 km latitude band.

    The ranking is deliberately cheap: the first coordinate of the geometry stands for
    the segment, municipalities are ordered by their own centre, and segments are ordered
    within a municipality by band then longitude. It is a bin-packing heuristic, not a
    territorial statement — the slices carry no administrative meaning.
    """
    df = df.with_columns(
        pl.col("location_geometry")
        .str.extract(FIRST_COORDINATE, 1)
        .cast(pl.Float64)
        .alias("_longitude"),
        pl.col("location_geometry")
        .str.extract(FIRST_COORDINATE, 2)
        .cast(pl.Float64)
        .alias("_latitude"),
    ).with_columns(
        (pl.col("_latitude") / GEOGRAPHIC_CELL_DEGREES).floor().alias("_latitude_band"),
        pl.col("commune1").fill_null("").alias("_municipality"),
    )

    municipalities = (
        df.group_by("_municipality")
        .agg(
            (pl.col("_latitude").mean() / GEOGRAPHIC_CELL_DEGREES).floor().alias("_centre_band"),
            pl.col("_longitude").mean().alias("_centre_longitude"),
        )
        .sort(["_centre_band", "_centre_longitude"], nulls_last=True)
        .with_row_index("_municipality_rank")
    )

    return (
        df.join(municipalities.select("_municipality", "_municipality_rank"), on="_municipality")
        .sort(
            ["_municipality_rank", "_latitude_band", "_longitude"],
            nulls_last=True,
        )
        .with_row_index("regulation_split_order")
        .with_columns(pl.col("regulation_split_order").cast(pl.Int64))
    )


def summarize(clean_data: pl.DataFrame) -> str:
    """One-line funnel summary, handy in a notebook or a review."""
    measures = clean_data.group_by(["regulation_identifier", "measure_group_key"]).len().height
    regulations = clean_data["regulation_identifier"].n_unique()
    return (
        f"{clean_data.height} emprises → {measures} mesures → {regulations} arrêtés "
        f"(avant découpage à {MAX_LOCATIONS_PER_REGULATION} emprises)"
    )


def preview_payload(clean_data: pl.DataFrame, identifier: str) -> str:
    """JSON preview of one regulation, without touching the API."""
    rows = clean_data.filter(pl.col("regulation_identifier") == identifier)
    return json.dumps(
        {
            "identifier": identifier,
            "title": rows.row(0, named=True)["regulation_title"],
            "measures": rows["measure_group_key"].unique().to_list(),
            "locations": rows.height,
        },
        ensure_ascii=False,
        indent=2,
    )
