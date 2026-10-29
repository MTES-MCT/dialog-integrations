"""Métropole de Lyon: chaussées et trottoirs, the GIS inventory of the road network.

A row is a road segment (37 569), not an order, so rows are grouped (R-28): under the
order number they cite for the measures present on all its segments, otherwise into one
metropolitan-wide regulation per distinct measure. A segment's measures are qualified
separately, so one segment can sit in two regulations.
"""

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
    PostApiRegulationsAddBodyMeasuresItemVehicleSetType0ExemptedTypesType0Item as VehicleExemptedTypeEnum,  # noqa: E501
)
from api.dia_log_client.models import (
    PostApiRegulationsAddBodyMeasuresItemVehicleSetType0RestrictedTypesType0Item as VehicleRestrictedTypeEnum,  # noqa: E501
)
from canonical.source import BaseDataSourceIntegration
from dialog.calls import DialogApi
from integrations.co_lyon.chaussees_trottoirs.regulation_key import (
    foreign_order_keys,
    is_project,
    order_key,
    order_number,
)
from integrations.co_lyon.chaussees_trottoirs.schema import LyonChausseesTrottoirsRawDataSchema
from integrations.co_lyon.grand_lyon import fetch_layer
from toolkit.transforms.perimeter import Perimeter, discard_outside_perimeter

WFS_LAYER = "pvo_patrimoine_voirie.pvochausseetrottoir"

# Prefixed so our batch stays removable as a block (R-29). The ~800 orders another channel
# pushed to the organisation follow no convention to align with (52 municipality prefixes).
IDENTIFIER_PREFIX = "MGL-CT"
# Any identifier outside the stem belongs to that other channel (`foreign_order_keys`).
ORGANISATION_STEM = "MGL-"

# Roads of a manager we are sure is not the Métropole's to regulate, unpublished without an
# agreement (R-73; scope settled on 2026-10-10): the State, 747 segments on 2026-10-10, and
# 1 segment of a third public body. The Rhône département (80 segments) is kept: on this
# territory the two are nearly one authority. On 2026-10-05 the Métropole said it does not
# vouch for the State's data.
ROAD_MANAGERS_WITHOUT_AGREEMENT = ("État", "Métropole gestion tiers public")

# The source files a pedestrian area as 5 km/h, which alone would read as an advisory: it
# goes out as `noEntry` except local access (R-71) plus the walking-pace limit.
PEDESTRIAN_AREA_SPEED = "5"
PEDESTRIAN_AREA_SPEED_KEY = "AIRE_PIETONNE_V5"

# Only the producer's label makes a 5 km/h segment a pedestrian area: inferring it from the
# speed would ban driving on streets such as Rue du Pavé (R-35, R-71; 3 332 unlabelled
# segments on 2026-09-22).
PEDESTRIAN_AREA_LABEL = "Aire Piétonne"

# API cap, the French legal maximum for a heavy goods vehicle: anything above is a
# structure's load capacity that landed in the same column.
MAX_HEAVYWEIGHT_TONNES = 44

# Speed kept next to the ZTL ban, like a pedestrian area's walking pace (R-72, decided on
# 2026-10-01 with the DiaLog team, 338 segments).
ZTL_REPLACES_SPEED = False

# DiaLog refuses an emprise outside the organisation's territory, and one refusal takes the
# whole regulation down (R-77). Neither the road's name nor `domanialite` predicts it.
ORGANIZATION_PERIMETER = ("epci", "200046977")  # Métropole de Lyon, SIREN code of the EPCI

# The source files towpaths, park promenades, private lanes and nameless segments as
# pedestrian areas, which mean nothing to a satnav off a drivable road. Read on the name,
# a weak but arguable predictor; it keeps real ones (Rue Saint Jean, Rue Victor Hugo) and
# lets `Chemin de la Digue` through.
PEDESTRIAN_AREA_PRIVATE = r"(?i)priv"
PEDESTRIAN_AREA_NAMELESS = r"(?i)sans d[ée]nomination|sans nom"
PEDESTRIAN_AREA_NOT_A_ROAD = (
    r"(?i)^(chemin rural|promenade|parc |jardin|square |esplanade|berge|voie sans"
    r"|contre.all[ée]e|passerelle|halage)"
)

# The API times out between 1 500 and 2 000 locations per regulation (probes of 2026-09-04,
# R-76); 1 000 is the margin agreed with the team until it takes larger batches.
MAX_LOCATIONS_PER_REGULATION = 1000

# Latitude bands that keep a split regulation together: 0.01° is about 1.1 km.
GEOGRAPHIC_CELL_DEGREES = 0.01

# Stands for the segment: cheaper than decoding 37 569 geometries, enough to sort by
# neighbourhood.
FIRST_COORDINATE = r"\[\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)"

# Typed after the Feyzin heavy goods order on 517 segments (2026-10-10). No public-service
# type in the API: it goes out as `other` with its text, on the dimension measure only.
PUBLIC_SERVICE_EXEMPTION = r"(?i)\bsauf\s+services?\s+publics?\b"
PUBLIC_SERVICE_EXEMPTION_TEXT = "Services publics"
PUBLIC_SERVICE_EXEMPTION_KEY = "_SAUF_SP"
# A direction or a part of the segment, which DiaLog cannot say (R-78): 10 segments on
# 2026-10-10, all speed limits.
UNEXPRESSED_QUALIFIER = (
    r"(?i)\bsens\s+(?:NS|SN|EO|OE|nord|sud|est|ouest)\b|\bentre\s+n\s*°|\bdu\s+n\s*°\s*\d"
)

DIMENSION_COLUMNS = {
    "limitationtonnage": "T",
    "limitationhauteur": "H",
    "limitationlargeur": "L",
    "limitationlongueur": "G",
}


class DataSourceIntegration(BaseDataSourceIntegration):
    name = "chaussees_trottoirs"
    identifier_prefix = f"{IDENTIFIER_PREFIX}-"
    raw_data_schema = LyonChausseesTrottoirsRawDataSchema

    # Makes "30 km/h across the metropolitan area" a single act.
    group_locations_by_measure = True
    max_locations_per_regulation = MAX_LOCATIONS_PER_REGULATION

    perimeter: Perimeter | None = None
    # None when they could not be read: no number is held back.
    foreign_order_keys: frozenset[str] | None = None

    def fetch_raw_data(self) -> pl.DataFrame:
        df = fetch_layer(WFS_LAYER)
        self.perimeter = Perimeter.fetch(*ORGANIZATION_PERIMETER)
        self.foreign_order_keys = self.fetch_foreign_order_keys()
        return df

    def fetch_foreign_order_keys(self) -> frozenset[str] | None:
        """Order keys the other channel already published, or None when unreadable.

        A courtesy to that channel, not a condition of the run: offline or on an API error
        the pipeline goes on without it.
        """
        try:
            identifiers = DialogApi(self.client).identifiers()
        except Exception as error:
            logger.warning(
                f"Foreign identifier check skipped: the organisation's identifiers could not "
                f"be read ({error!r}) — numbers already published by another channel will "
                "not be held back"
            )
            return None
        keys = foreign_order_keys(identifiers, ORGANISATION_STEM)
        logger.info(
            f"{len(keys)} order keys already published in the organisation by another "
            f"channel, out of {len(identifiers)} identifiers"
        )
        return keys

    def discard_outside_perimeter(self, df: pl.DataFrame) -> pl.DataFrame:
        """Drop the segments DiaLog would refuse: those that do not touch the territory."""
        if self.perimeter is None:
            self.perimeter = Perimeter.fetch(*ORGANIZATION_PERIMETER)
        return discard_outside_perimeter(df, self.perimeter)

    def compute_clean_data(self, raw_data: pl.DataFrame) -> pl.DataFrame:
        return (
            raw_data.pipe(read_order_number)
            .pipe(discard_impossible_tonnages)
            .pipe(explode_into_measures)
            .pipe(self.count_dataset_restrictions)
            # After the counter, so the segments outside count as stated and not retained.
            .pipe(self.discard_outside_perimeter)
            # Before R-28 judges the orders: a dropped segment must not keep an order from
            # carrying a measure present on all the others.
            .pipe(discard_roads_of_other_managers)
            .pipe(discard_unexpressed_qualifiers)
            .pipe(compute_measure_fields)
            .pipe(discard_unlabelled_pedestrian_areas)
            .pipe(discard_pedestrian_areas_off_the_road_network)
            .pipe(compute_vehicle_fields)
            .pipe(compute_period_fields)
            .pipe(compute_location_fields)
            # Before R-28 judges the orders, like the discards above.
            .pipe(self.apply_rules)
            .pipe(compute_regulation_fields, self.foreign_order_keys)
            .pipe(self.count_retained_restrictions)
            # After the counters (not a restriction of its own) and the identifier (it
            # joins the regulation of its ban).
            .pipe(add_pedestrian_area_speed)
            # Groups by (regulation, measure, street), so after the identifier.
            .pipe(merge_contiguous_segments)
            .pipe(compute_geographic_split_order)
        )


def read_order_number(df: pl.DataFrame) -> pl.DataFrame:
    """Read the order number, its grouping key, and whether it cites a project.

    A project's rows lose their order key only: their measures are real and join the
    metropolitan-wide fallbacks.
    """
    text = pl.col("precisionreglementation")
    df = df.with_columns(
        text.map_elements(order_number, return_dtype=pl.String).alias("order_number"),
        text.map_elements(is_project, return_dtype=pl.Boolean).alias("is_project"),
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


def discard_unlabelled_pedestrian_areas(df: pl.DataFrame) -> pl.DataFrame:
    """Keep the pedestrian areas the producer labels as such, drop the rest.

    An unlabelled 5 km/h segment may be an area or a road limited to walking pace, and the
    error is not symmetric: `noEntry` on an open street sends a GPS elsewhere, silence only
    omits a restriction. Only `AIRE_PIETONNE` is touched, not the segment's other measures.
    """
    is_area = pl.col("measure_group_key") == pl.lit("AIRE_PIETONNE")
    labelled = pl.col("reglementationzca") == pl.lit(PEDESTRIAN_AREA_LABEL)
    discarded = is_area & ~labelled.fill_null(False)

    n = df.select(discarded.sum()).item()
    if n:
        logger.info(
            f"Discarding {n} segments filed at {PEDESTRIAN_AREA_SPEED} km/h that the source "
            f"does not call a « {PEDESTRIAN_AREA_LABEL} »; keeping "
            f"{df.select((is_area & labelled.fill_null(False)).sum()).item()} labelled ones"
        )
    return df.filter(~discarded)


def discard_pedestrian_areas_off_the_road_network(df: pl.DataFrame) -> pl.DataFrame:
    """Drop the labelled pedestrian areas that are not on a road.

    Decided on 2026-09-17: a ban on a private lane or in a park is noise for a satnav, even
    when labelled (Esplanade Fernand Rude, Jardin de la Grande Côte fall out with it).
    """
    is_area = pl.col("measure_group_key") == pl.lit("AIRE_PIETONNE")
    name = pl.col("nomvoie1").fill_null("")
    private = pl.col("domanialite").fill_null("").str.contains(PEDESTRIAN_AREA_PRIVATE)
    nameless = (pl.col("nomvoie1").is_null()) | name.str.contains(PEDESTRIAN_AREA_NAMELESS)
    not_a_road = name.str.contains(PEDESTRIAN_AREA_NOT_A_ROAD)
    discarded = is_area & (private | nameless | not_a_road)

    n = df.select(discarded.sum()).item()
    if n:
        logger.info(
            f"Discarding {n} pedestrian-area segments off the road network "
            f"({df.select((is_area & private).sum()).item()} private, "
            f"{df.select((is_area & nameless & ~private).sum()).item()} nameless, "
            f"{df.select((is_area & not_a_road & ~private & ~nameless).sum()).item()} "
            f"named as something other than a road)"
        )
    return df.filter(~discarded)


def discard_roads_of_other_managers(df: pl.DataFrame) -> pl.DataFrame:
    """Drop every measure on the roads of a manager the Métropole cannot publish for (R-73).

    They stay counted as stated, so the retention rate shows what the rule costs.
    """
    discarded = pl.col("domanialite").is_in(ROAD_MANAGERS_WITHOUT_AGREEMENT).fill_null(False)
    dropped = df.filter(discarded)
    if dropped.height:
        counts = dict(
            dropped.group_by("domanialite").agg(pl.col("codetroncon").n_unique()).iter_rows()
        )
        logger.info(
            f"Discarding {dropped.height} measures on {dropped['codetroncon'].n_unique()} "
            f"segments of another road manager, not published without its agreement "
            f"(R-73): {counts}"
        )
    return df.filter(~discarded)


def discard_unexpressed_qualifiers(df: pl.DataFrame) -> pl.DataFrame:
    """Drop the segments whose free text states a direction or a part of the segment (R-78).

    Published both ways and end to end, they would state more than the source.
    """
    discarded = (
        pl.col("precisionreglementation").str.contains(UNEXPRESSED_QUALIFIER).fill_null(False)
    )
    dropped = df.filter(discarded)
    if dropped.height:
        logger.info(
            f"Discarding {dropped.height} measures on {dropped['codetroncon'].n_unique()} "
            f"segments whose free text states a direction or a part of the segment (R-78): "
            f"{sorted(dropped['precisionreglementation'].unique().to_list())}"
        )
    return df.filter(~discarded)


def exempts_public_services() -> pl.Expr:
    """The free text says "sauf service(s) public(s)" and the measure is a dimension limit."""
    return (pl.col("_measure_kind") == "dimensions") & pl.col(
        "precisionreglementation"
    ).str.contains(PUBLIC_SERVICE_EXEMPTION).fill_null(False)


def discard_impossible_tonnages(df: pl.DataFrame) -> pl.DataFrame:
    """Drop the tonnages no vehicle can carry (50 to 230 t): a structure's load capacity.

    The API refused 6 regulations over them on 2026-09-07. Only the tonnage goes, so the
    row keeps its other dimension limits (R-35).
    """
    impossible = pl.col("limitationtonnage") > MAX_HEAVYWEIGHT_TONNES
    n_impossible = df.select(impossible.fill_null(False).sum()).item()
    if n_impossible:
        values = sorted(
            set(df.filter(impossible)["limitationtonnage"].to_list())  # type: ignore[arg-type]
        )
        logger.warning(
            f"Dropping the tonnage of {n_impossible} segments above "
            f"{MAX_HEAVYWEIGHT_TONNES} t — a structure's load capacity, not a vehicle "
            f"limit: {values}"
        )
    return df.with_columns(
        pl.when(impossible)
        .then(None)
        .otherwise(pl.col("limitationtonnage"))
        .alias("limitationtonnage")
    )


def explode_into_measures(df: pl.DataFrame) -> pl.DataFrame:
    """Turn one row per road segment into one row per (segment, measure).

    Every speed is published, default 50 and 80 km/h included: a satnav needs them as much
    as decided ones (R-70). A ZTL is a ban of its own, and its speed is how one drives once
    let in (R-72).
    """
    is_ztl = pl.col("ztl").fill_null(False)

    ztl = df.filter(is_ztl).with_columns(pl.lit("ztl").alias("_measure_kind"))

    speeds = df.filter(
        pl.col("limitationvitesse").is_not_null()
        & (~is_ztl if ZTL_REPLACES_SPEED else pl.lit(True))
    ).with_columns(pl.lit("speed").alias("_measure_kind"))

    # Keeps the order number: whether the order carries it is decided on all the segments
    # citing it (`measures_carried_by_each_order`).
    dimensions = df.filter(
        pl.any_horizontal(pl.col(column).is_not_null() for column in DIMENSION_COLUMNS)
    ).with_columns(pl.lit("dimensions").alias("_measure_kind"))

    total = speeds.height + dimensions.height + ztl.height
    logger.info(
        f"Exploded {df.height} segments into {total} measures "
        f"({speeds.height} speed, {dimensions.height} dimension, {ztl.height} ZTL); "
        f"{df.select(pl.col('limitationvitesse').is_null().sum()).item()} segments carry "
        "no speed"
    )
    return pl.concat([speeds, dimensions, ztl], how="vertical")


def compute_measure_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Measure type, speed, and the key that collapses identical measures into one.

    Dimension limits are `noEntry` (R-33). Inside a ZTL, 5 km/h is the zone's speed, not a
    pedestrian area: the ZTL already bans entry (R-71, R-72).
    """
    is_ztl = pl.col("_measure_kind") == "ztl"
    is_pedestrian_area = (
        (pl.col("_measure_kind") == "speed")
        & (pl.col("limitationvitesse") == PEDESTRIAN_AREA_SPEED)
        & ~pl.col("ztl").fill_null(False)
    )
    is_speed_limit = (pl.col("_measure_kind") == "speed") & ~is_pedestrian_area

    return df.with_columns(
        pl.when(is_speed_limit)
        .then(pl.lit(MeasureTypeEnum.SPEEDLIMITATION.value))
        .otherwise(pl.lit(MeasureTypeEnum.NOENTRY.value))
        .alias("measure_type_"),
        pl.when(is_speed_limit)
        .then(pl.col("limitationvitesse").cast(pl.Int64))
        .otherwise(None)
        .alias("measure_max_speed"),
        pl.when(is_ztl)
        .then(pl.lit("ZTL"))
        .when(is_speed_limit)
        .then(pl.format("V{}", pl.col("limitationvitesse")))
        .when(is_pedestrian_area)
        .then(pl.lit("AIRE_PIETONNE"))
        .when(exempts_public_services())
        .then(dimension_signature() + pl.lit(PUBLIC_SERVICE_EXEMPTION_KEY))
        .otherwise(dimension_signature())
        .alias("measure_group_key"),
    )


def dimension_signature() -> pl.Expr:
    """`GABARIT_T3_5_H4_1`: the row's dimension limits, in a stable order."""
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
    """Vehicle set of each measure: dimension limits are carried over, never invented (R-35)."""
    is_dimension = pl.col("_measure_kind") == "dimensions"
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

    # One enters a ZTL or a pedestrian area to reach an address: without this exemption the
    # street would read as barred to residents, deliveries and services.
    is_ztl = pl.col("_measure_kind") == "ztl"
    is_pedestrian_area = pl.col("measure_group_key") == "AIRE_PIETONNE"
    exempted = (
        pl.when(is_ztl | is_pedestrian_area)
        .then(pl.lit([VehicleExemptedTypeEnum.DESSERTELOCALE.value]))
        .when(exempts_public_services())
        .then(pl.lit([VehicleExemptedTypeEnum.OTHER.value]))
        .otherwise(None)
    )

    return df.with_columns(
        (~is_dimension).alias("vehicle_all_vehicles"),
        exempted.alias("vehicle_exempted_types"),
        # The API reads the text only next to `other`.
        pl.when(exempts_public_services())
        .then(pl.lit(PUBLIC_SERVICE_EXEMPTION_TEXT))
        .otherwise(None)
        .alias("vehicle_other_exempted_type_text"),
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
    """Permanent period, left undated.

    The date in the free text is the calmed-traffic zone's, not the measure's, and a past
    date is never presumed (R-39). Not the run day either: the sync compares dates, so every
    order would look modified each morning. `sync/dating.py` fills it when writing.
    """
    return df.with_columns(
        pl.lit(None, dtype=pl.String).alias("period_start_date"),
        pl.lit(None).alias("period_end_date"),
        pl.lit("everyDay").alias("period_recurrence_type"),
        pl.lit(True).alias("period_is_permanent"),
    )


def compute_location_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Location of each measure, as a raw GeoJSON LineString.

    `rawGeoJSON` carries no direction: harmless for a speed limit, blocking the day a
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


def measures_carried_by_each_order(df: pl.DataFrame) -> pl.DataFrame:
    """One row per `(order_key, measure_group_key)` present on **all** the order's segments.

    The segments citing a number also carry whatever else applies there, and a majority
    rule credited acts with measures they never decided (decided 2026-09-22, R-28). A set
    intersection gives the same answer from one run to the next (R-20).
    """
    numbered = df.filter(pl.col("order_key").is_not_null())
    segments = numbered.group_by("order_key").agg(
        pl.col("codetroncon").n_unique().alias("_segments")
    )
    covered = numbered.group_by(["order_key", "measure_group_key"]).agg(
        pl.col("codetroncon").n_unique().alias("_covered")
    )
    carried = (
        covered.join(segments, on="order_key")
        .filter(pl.col("_covered") == pl.col("_segments"))
        .select("order_key", "measure_group_key", pl.lit(True).alias("_is_own_measure"))
    )

    emptied = segments.filter(~pl.col("order_key").is_in(carried["order_key"].implode()))
    logger.info(
        f"{carried['order_key'].n_unique()} numbered orders carry "
        f"{carried.height} measure(s) present on all their segments; "
        f"{emptied.height} order(s) carry none and go whole to the metropolitan "
        "regulations of their measures"
    )
    for row in (
        emptied.sort(["_segments", "order_key"], descending=[True, False])
        .head(5)
        .iter_rows(named=True)
    ):
        logger.info(f"  {row['order_key']}: {row['_segments']} segments, no measure on all")
    return carried


def compute_regulation_fields(
    df: pl.DataFrame, foreign_order_keys: frozenset[str] | None = None
) -> pl.DataFrame:
    """Identifier, category, subject and title of each regulation.

    `MGL-CT-{order key}` when the order carries the measure and no other channel holds the
    number, else `MGL-CT-{measure signature}`, one metropolitan-wide regulation (R-23).
    No municipality in the key: Lyon's "Ville 30" would split into 13 orders. 61 short
    numbers (`845`) could merge two municipalities' orders; merging was chosen on 2026-09-04.
    """
    df = df.join(
        measures_carried_by_each_order(df), on=["order_key", "measure_group_key"], how="left"
    ).with_columns(pl.col("_is_own_measure").fill_null(False))

    if foreign_order_keys:
        foreign = pl.col("order_key").is_in(list(foreign_order_keys))
        held_back = df.filter(foreign & pl.col("_is_own_measure"))
        if held_back.height:
            logger.warning(
                f"{held_back['order_key'].n_unique()} order number(s) already published in "
                f"the organisation by another channel are not created again; their "
                f"{held_back.height} emprises join the metropolitan regulations (P-04): "
                f"{sorted(held_back['order_key'].unique().to_list())}"
            )
        # Also drives the title, or a metropolitan regulation would read "Arrêté n°…".
        df = df.with_columns((pl.col("_is_own_measure") & ~foreign).alias("_is_own_measure"))

    displaced = df.filter(pl.col("order_key").is_not_null() & ~pl.col("_is_own_measure"))
    if displaced.height:
        logger.info(
            f"{displaced.height} emprises leave their numbered order for the metropolitan "
            f"regulation of their own measure ({displaced['measure_group_key'].n_unique()} "
            f"distinct measures over {displaced['order_key'].n_unique()} orders)"
        )

    identifier = (
        pl.when(pl.col("_is_own_measure"))
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


def add_pedestrian_area_speed(df: pl.DataFrame) -> pl.DataFrame:
    """Give every published pedestrian area its walking-pace limit, in the same regulation.

    The Code de la route (R. 110-2) defines the area by both (team decision, 2026-09-14).
    A twin of the ban, it inherits its filters; no exemption, as those let in still walk.
    """
    bans = df.filter(pl.col("measure_group_key") == pl.lit("AIRE_PIETONNE"))
    if not bans.height:
        return df
    speeds = bans.with_columns(
        pl.lit(MeasureTypeEnum.SPEEDLIMITATION.value).alias("measure_type_"),
        pl.lit(int(PEDESTRIAN_AREA_SPEED), dtype=pl.Int64).alias("measure_max_speed"),
        pl.lit(PEDESTRIAN_AREA_SPEED_KEY).alias("measure_group_key"),
        pl.lit(None, dtype=pl.List(pl.String)).alias("vehicle_exempted_types"),
    )
    logger.info(f"{speeds.height} pedestrian-area bans get their {PEDESTRIAN_AREA_SPEED} km/h twin")
    return pl.concat([df, speeds], how="vertical")


def regulation_title() -> pl.Expr:
    """Human-readable title, never blocking, always fabricated (R-24)."""
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
        .when(pl.col("measure_group_key") == pl.lit("ZTL"))
        .then(pl.lit("Zone à trafic limité, sauf desserte locale – Métropole de Lyon"))
        .when(pl.col("measure_group_key").str.ends_with(PUBLIC_SERVICE_EXEMPTION_KEY))
        .then(
            pl.format(
                "Restriction de gabarit {}, sauf services publics – Métropole de Lyon",
                dimension_label(),
            )
        )
        .otherwise(pl.format("Restriction de gabarit {} – Métropole de Lyon", dimension_label()))
    )

    return (
        pl.when(pl.col("_is_own_measure"))
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


def chain_segments(segments: list[list]) -> list[list]:
    """Join the segments that meet end to end: `AB` + `BC` becomes `AC`.

    Only where exactly two meet: at a junction, which branch continues would be arbitrary.
    Endpoints are compared exactly: on 2026-09-07, 13 533 shared endpoints whether rounded
    to 5 or 8 decimals.
    """
    alive = {index: list(segment) for index, segment in enumerate(segments)}
    ends: dict[tuple, set[int]] = {}
    for index, segment in alive.items():
        for end in (tuple(segment[0]), tuple(segment[-1])):
            ends.setdefault(end, set()).add(index)

    changed = True
    while changed:
        changed = False
        for point, indexes in list(ends.items()):
            indexes = {index for index in indexes if index in alive}
            ends[point] = indexes
            if len(indexes) != 2:
                continue

            first, second = sorted(indexes)
            head, tail = alive[first], alive[second]
            if tuple(head[0]) == point:
                head = head[::-1]
            if tuple(tail[-1]) == point:
                tail = tail[::-1]
            # A loop meets itself and has nothing to be joined to.
            if tuple(head[-1]) != point or tuple(tail[0]) != point:
                continue

            merged = head + tail[1:]
            del alive[second]
            alive[first] = merged
            for end in (tuple(merged[0]), tuple(merged[-1])):
                ends.setdefault(end, set()).discard(second)
                ends[end].add(first)
            ends[point] = set()
            changed = True

    return list(alive.values())


def _chain_geometries(geometries: list[str]) -> list[str]:
    """`chain_segments`, on serialized GeoJSON. Anything not a LineString passes through."""
    parsed = [json.loads(geometry) for geometry in geometries]
    if any(geometry.get("type") != "LineString" for geometry in parsed):
        return list(geometries)
    chained = chain_segments([geometry["coordinates"] for geometry in parsed])
    return [
        json.dumps({"type": "LineString", "coordinates": coordinates}) for coordinates in chained
    ]


MERGE_KEY = ["regulation_identifier", "measure_group_key", "location_label"]


def merge_contiguous_segments(df: pl.DataFrame) -> pl.DataFrame:
    """Collapse the touching segments of one street under one measure into one emprise.

    The other fields are identical within a group by construction, so the first row's are
    kept.
    """
    others = [column for column in df.columns if column not in MERGE_KEY + ["location_geometry"]]
    merged = (
        df.group_by(MERGE_KEY, maintain_order=True)
        .agg(
            pl.col("location_geometry"),
            *[pl.col(column).first() for column in others],
        )
        .with_columns(
            pl.col("location_geometry").map_elements(
                _chain_geometries, return_dtype=pl.List(pl.String)
            )
        )
        .explode("location_geometry")
    )

    logger.info(
        f"Chained contiguous segments: {df.height} emprises become {merged.height} "
        f"({100 * (1 - merged.height / df.height):.0f} % fewer) over "
        f"{df.select(MERGE_KEY).n_unique()} (regulation, measure, street) groups"
    )
    return merged


def compute_geographic_split_order(df: pl.DataFrame) -> pl.DataFrame:
    """Rank the rows so that a regulation too large for one POST splits into neighbours.

    By municipality, then ~1 km latitude band, then longitude: a cheap bin-packing
    heuristic, the slices carry no administrative meaning.
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
