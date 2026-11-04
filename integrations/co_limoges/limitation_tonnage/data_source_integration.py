"""Limoges: weight limits on the city's streets (ArcGIS layer `WS_publication_limitation_tonnage`).

A row is a stretch citing its arrete. The arrete is kept as `LIMOGES-LT-{number}` when its
number looks like an act, with every weight it sets; every other row joins the city-wide
regulation of its weight, `LIMOGES-LT-T3.5`.
"""

import polars as pl

from api.dia_log_client.models import MeasureTypeEnum, RoadTypeEnum
from canonical.source import BaseDataSourceIntegration
from integrations.co_limoges.limitation_tonnage.schema import (
    LimogesLimitationTonnageRawDataSchema,
)
from integrations.co_limoges.limitation_tonnage.street_names import from_referential
from toolkit.connectors import arcgis
from toolkit.rules.location import (
    GEOMETRY_MISSING,
    OUTSIDE_PERIMETER,
    drawn_twice,
    extends_outside_perimeter,
    twin_trace,
)
from toolkit.rules.measure import contradicted_value
from toolkit.rules.vehicles import HEAVYWEIGHT_BELOW_HEAVY_GOODS
from toolkit.transforms import overlaps
from toolkit.transforms.copies import drop_copies
from toolkit.transforms.generic_regulations import looks_like_act, numbered_or_generic
from toolkit.transforms.geometry import to_geojson
from toolkit.transforms.perimeter import Perimeter, extends_outside, refused
from toolkit.transforms.periods import permanent_period
from toolkit.transforms.titles import truncate
from toolkit.transforms.vehicle_types import (
    limit_text,
    one_measure_per_limit,
    restricted_types_from_limits,
)

URL = (
    "https://geo.limoges.fr/services2/rest/services/OpenData/"
    "WS_publication_limitation_tonnage/FeatureServer/0/query"
)
IDENTIFIER_PREFIX = "LIMOGES-LT-"
PERIMETER = ("insee", "87085")
OTHER_CATEGORY_TEXT = "Limitation de tonnage"

# Numbers of 2 or 3 digits (10 of 76 on 2026-10-28, `128`, `46`) read as a rank, not as an
# act; the others run 6 to 9 characters (`944050`, `2013P140`, `202500389`).
ACT_MIN_LENGTH = 6

ROW_RULES = (
    # Here rather than with the contract rules: a row without its trace must not count as
    # retained.
    GEOMETRY_MISSING,
    # 2.5 t (`201800209`, which keeps its 7.5 t) and 1.5 t (`20021506`) on 2026-11-04.
    HEAVYWEIGHT_BELOW_HEAVY_GOODS,
    extends_outside_perimeter(pl.col("_extends_outside")),
    contradicted_value(pl.col("_contradicted")),
    drawn_twice(pl.col("_drawn_twice")),
    twin_trace(pl.col("_twin")),
)


class DataSourceIntegration(BaseDataSourceIntegration):
    raw_data_schema = LimogesLimitationTonnageRawDataSchema
    name = "limitation_tonnage"
    identifier_prefix = IDENTIFIER_PREFIX

    group_locations_by_measure = True
    max_locations_per_regulation = 1000

    perimeter: Perimeter | None = None

    def fetch_raw_data(self) -> pl.DataFrame:
        df = arcgis.read_layer(URL)
        streets = arcgis.coded_values(URL, "id_voie")
        self.perimeter = Perimeter.fetch(*PERIMETER)
        return df.with_columns(
            nom_voie=pl.col("id_voie")
            .cast(pl.Int64)
            .replace_strict(streets, default=None, return_dtype=pl.String)
        )

    def compute_clean_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        if self.perimeter is None:
            raise RuntimeError("fetch_raw_data reads the perimeter: call it first")
        return (
            # On 2026-10-31 the layer held nearly every stretch twice (189 of 390 rows, mostly
            # at `objectid` + 199); one copy lacked its road code.
            drop_copies(raw_data, ["num_arrete", "tonnage", "geometry"], prefer_filled="id_voie")
            # ArcGIS promises no order; the nightly comparison reads a measure's emprises in
            # order.
            .sort("num_arrete", "tonnage", "geometry", nulls_last=True)
            .pipe(self.count_dataset_restrictions)
            .pipe(read_weight)
            .pipe(read_number_and_geometry)
            .pipe(flag_overlaps)
            .pipe(flag_perimeter, self.perimeter)
            .pipe(self.apply_rules, OUTSIDE_PERIMETER, *ROW_RULES)
            .pipe(self.count_retained_restrictions)
            .pipe(one_measure_per_limit)
            .pipe(compute_fields)
            .pipe(
                numbered_or_generic,
                keeps_number=pl.col("_keeps_number"),
                number=pl.col("num_arrete"),
                prefix=IDENTIFIER_PREFIX,
                numbered_title=numbered_title(),
                generic_title=weight_label(),
                other_category_text=OTHER_CATEGORY_TEXT,
            )
        )


def read_weight(df: pl.DataFrame) -> pl.DataFrame:
    """The weight as a number, a non-positive or unreadable one as none."""
    weight = pl.col("tonnage").cast(pl.Float64, strict=False)
    return df.with_columns(
        vehicle_heavyweight_max_weight=pl.when(weight > 0).then(weight),
        # Every limit the threshold rule reads, so that it judges the row here.
        **{
            size: pl.lit(None, dtype=pl.Float64)
            for size in ("vehicle_max_height", "vehicle_max_width", "vehicle_max_length")
        },
    ).with_columns(
        vehicle_all_vehicles=pl.lit(False),
        vehicle_restricted_types=restricted_types_from_limits(
            pl.col("vehicle_heavyweight_max_weight"), pl.col("vehicle_max_height")
        ),
    )


def read_number_and_geometry(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        num_arrete=pl.col("num_arrete").str.strip_chars(),
        location_geometry=to_geojson(df["geometry"], "wkt"),
        location_road_type=pl.lit(RoadTypeEnum.RAWGEOJSON.value),
    ).with_columns(_keeps_number=looks_like_act(pl.col("num_arrete"), ACT_MIN_LENGTH))


def flag_overlaps(df: pl.DataFrame) -> pl.DataFrame:
    """Twins only along one road code: parallel streets of the old town lie within 20 m of
    each other (5 such rows on 2026-11-04). Two acts over one stretch both stay: that day,
    `128` and `944050` shared 7 stretches at the same weight."""
    redrawn = overlaps.drawn_twice(df, "location_geometry", by="num_arrete")
    return df.with_columns(
        _contradicted=overlaps.contradicted_geometry(
            df, "vehicle_heavyweight_max_weight", "location_geometry"
        ),
        _drawn_twice=redrawn,
        _twin=overlaps.twin_traces(df, "location_geometry", name="id_voie", ignore=redrawn),
    )


def flag_perimeter(df: pl.DataFrame, perimeter: Perimeter) -> pl.DataFrame:
    geometry = df["location_geometry"]
    outside = refused(geometry, perimeter)
    return df.with_columns(
        _outside_perimeter=outside,
        _extends_outside=extends_outside(geometry, perimeter) & ~outside,
    )


def weight_label() -> pl.Expr:
    weight = limit_text(pl.col("vehicle_heavyweight_max_weight")).str.replace(
        ".", ",", literal=True
    )
    return pl.format("Circulation interdite aux véhicules de plus de {} t – Limoges", weight)


def numbered_title() -> pl.Expr:
    """The act and its weight; an act setting several names none, its title being one for
    all its measures."""
    number = pl.col("num_arrete")
    several = pl.col("measure_group_key").n_unique().over("num_arrete") > 1
    return truncate(
        pl.when(several)
        .then(pl.format("Arrêté n° {} – Limitations de tonnage – Limoges", number))
        .otherwise(pl.format("Arrêté n° {} – {}", number, weight_label()))
    )


def location_label() -> pl.Expr:
    """`Rue des Arts, Limoges`, from `ARTS (rue des)`; the road code when the layer's
    referential lacks it."""
    road = pl.col("id_voie").cast(pl.Int64).cast(pl.String)
    street = pl.col("nom_voie").map_elements(from_referential, return_dtype=pl.String)
    return (
        pl.when(pl.col("nom_voie").is_not_null())
        .then(pl.format("{}, Limoges", street))
        .when(road.is_not_null())
        .then(pl.format("Limoges (code voie {})", road))
        .otherwise(pl.lit("Limoges"))
    )


def compute_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Measure, period and location of each stretch, and only the columns kept downstream."""
    return df.select(
        "num_arrete",
        "_keeps_number",
        "id_voie",
        "measure_group_key",
        "location_geometry",
        "vehicle_heavyweight_max_weight",
        "vehicle_all_vehicles",
        "vehicle_restricted_types",
        pl.lit(MeasureTypeEnum.NOENTRY.value).alias("measure_type_"),
        "location_road_type",
        location_label().alias("location_label"),
        *permanent_period(),
    )
