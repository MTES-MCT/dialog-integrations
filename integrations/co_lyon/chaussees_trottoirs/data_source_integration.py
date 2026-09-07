"""Data source integration for Métropole de Lyon: chaussées et trottoirs.

The layer is a GIS inventory of the road network — 37 569 segments, one row per segment.
A row is **not** an order: it is a piece of road, and it carries whatever is in force
there. Turning each row into a DiaLog regulation would fabricate 37 569 administrative
acts that do not exist, so rows are grouped instead (R-28):

- when the free-text field yields an order number, the rows citing it become emprises of
  that order — but only for **the one measure the order decides**, whatever municipality
  they fall in;
- everything else groups *by measure*: one metropolitan-wide regulation per distinct
  measure — one "30 km/h across the Métropole de Lyon" holding N emprises.

"Everything else" covers two populations that behave alike: segments carrying no order
number, and the other measures of a segment whose order decides something else. An order
number is typed on every segment of its zone, and those segments also carry whatever else
applies there. Keeping it all made `MGL-CT-2024RP44520` a single regulation holding
30 km/h *and* a pedestrian area *and* two tonnage limits *and* 20 km/h — an act saying
seven things it never said (measured 2026-09-07: 48 of 596 numbered orders were like
that, and they held half the corpus).

A row can feed two measures at once: its speed limit and its dimension limit. They are
qualified separately, so discarding one never discards the other, and the same segment
legitimately appears in two regulations — its speed under its order, its tonnage under
the metropolitan regulation of that tonnage. Two distinct restrictions at one place.

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
    PostApiRegulationsAddBodyMeasuresItemVehicleSetType0ExemptedTypesType0Item as VehicleExemptedTypeEnum,  # noqa: E501
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
from integrations.local_time import start_of_local_day
from integrations.shared.wfs import LYON_BBOX, assert_lon_lat_bbox, fetch_wfs_features

WFS_LAYER = "pvo_patrimoine_voirie.pvochausseetrottoir"

# Every identifier we create is prefixed, so our batch stays recognisable and removable
# in one go. The organisation already holds 812 Lyon orders pushed by a channel absent
# from this repository (Litteralis), whose identifiers use 52 municipality prefixes for
# ~40 municipalities — there is no convention to align with (R-29, P-04 still open).
#
# `MGL` is the stem the whole organisation uses: the work sites of
# `chantiers_perturbants` are `MGL-CHP-`, and these are `MGL-CT-`. One stem means one
# handle — R-29's point is that a prefixed batch is isolable and removable as a block,
# and two stems for one organisation would be two blocks to chase.
IDENTIFIER_PREFIX = "MGL-CT"

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

# `vehicleSet.heavyweightMaxWeight` is capped at 44 by the API — the French legal maximum
# for a heavy goods vehicle. Anything above is a structure's load capacity that landed in
# the same column.
MAX_HEAVYWEIGHT_TONNES = 44

# A zone à trafic limité bans driving through, with an exemption for local access. The
# source also files a speed for those segments — 5, 20 or 30 km/h across the Presqu'île —
# which describes driving *inside* the zone rather than the restriction on entering it.
# True publishes the ban alone, which is what the sign at the entrance states.
ZTL_REPLACES_SPEED = True

# Segments the API refuses whatever regulation they are put in — 271 of the 23 896 we
# post, measured on the preprod on 2026-09-07 by `ai/tools/probe_refused_segments.py`
# (3 309 probes, deliberately not versioned). Keyed on `codetroncon`, which comes from the
# road reference system rather than from the export, so an entry survives a re-export.
#
# **Do not try to replace this list with a rule.** Two were tested against it and both
# fail: naming (190 refused segments are called "Autoroute", but 81 are not — RD 342,
# Rocade Est, RN 6, "Rue du Stade" — while 315 segments named "Autoroute" are accepted),
# and `domanialite` (202 refused are État, but 15 are Métropole, 7 Commune). Nothing in
# the source predicts the refusal, which is why it was asked of the API rather than
# guessed. See `discard_refused_segments` for what this list is and is not.
REFUSED_SEGMENTS: frozenset[str] = frozenset(
    {
        "T11548",
        "T11874",
        "T12096",
        "T12097",
        "T18699",
        "T19042",
        "T20074",
        "T23195",
        "T23395",
        "T23396",
        "T23398",
        "T23399",
        "T23400",
        "T29118",
        "T29119",
        "T29120",
        "T29121",
        "T29122",
        "T29123",
        "T29124",
        "T29125",
        "T29126",
        "T29127",
        "T29128",
        "T29129",
        "T29130",
        "T29131",
        "T29132",
        "T29133",
        "T29134",
        "T29135",
        "T29136",
        "T29137",
        "T29181",
        "T29182",
        "T29184",
        "T29185",
        "T29186",
        "T29190",
        "T29191",
        "T29195",
        "T29196",
        "T29197",
        "T29198",
        "T29199",
        "T29200",
        "T29201",
        "T29209",
        "T29213",
        "T29223",
        "T29228",
        "T29229",
        "T29261",
        "T29262",
        "T29266",
        "T29338",
        "T29340",
        "T29341",
        "T29343",
        "T29356",
        "T29360",
        "T29364",
        "T29375",
        "T29376",
        "T29382",
        "T29383",
        "T29384",
        "T29385",
        "T29387",
        "T29388",
        "T29389",
        "T29390",
        "T29391",
        "T29392",
        "T29393",
        "T29394",
        "T29395",
        "T29396",
        "T29404",
        "T29405",
        "T29406",
        "T29407",
        "T29408",
        "T29409",
        "T29410",
        "T29411",
        "T29412",
        "T29413",
        "T29414",
        "T29415",
        "T29416",
        "T29417",
        "T29418",
        "T29419",
        "T29421",
        "T29423",
        "T29425",
        "T29426",
        "T29427",
        "T29428",
        "T29429",
        "T29430",
        "T29431",
        "T29432",
        "T29433",
        "T29434",
        "T29435",
        "T29439",
        "T29440",
        "T29441",
        "T29442",
        "T29444",
        "T29445",
        "T29446",
        "T29447",
        "T29450",
        "T29451",
        "T29470",
        "T29472",
        "T29474",
        "T29475",
        "T29477",
        "T29479",
        "T29481",
        "T29483",
        "T29485",
        "T29514",
        "T29516",
        "T29520",
        "T29835",
        "T29836",
        "T31049",
        "T31050",
        "T31051",
        "T31052",
        "T31053",
        "T35890",
        "T35895",
        "T35923",
        "T35925",
        "T35927",
        "T35929",
        "T35931",
        "T35933",
        "T35935",
        "T35937",
        "T35940",
        "T35947",
        "T35953",
        "T35956",
        "T36064",
        "T36065",
        "T38684",
        "T39446",
        "T39448",
        "T39451",
        "T39452",
        "T41433",
        "T42917",
        "T43102",
        "T43104",
        "T43105",
        "T43106",
        "T43107",
        "T43108",
        "T43109",
        "T43110",
        "T43111",
        "T43112",
        "T43114",
        "T43115",
        "T43116",
        "T43117",
        "T43118",
        "T43119",
        "T43120",
        "T43121",
        "T43122",
        "T43123",
        "T43130",
        "T43132",
        "T43134",
        "T43136",
        "T43138",
        "T43140",
        "T43142",
        "T43146",
        "T43149",
        "T43150",
        "T43151",
        "T43155",
        "T43157",
        "T43159",
        "T43160",
        "T43161",
        "T43162",
        "T43165",
        "T43166",
        "T45430",
        "T45566",
        "T45960",
        "T45965",
        "T45971",
        "T45972",
        "T45973",
        "T45974",
        "T46308",
        "T46309",
        "T46310",
        "T46311",
        "T46319",
        "T46320",
        "T46321",
        "T46333",
        "T46336",
        "T46341",
        "T46356",
        "T46373",
        "T46374",
        "T46375",
        "T46376",
        "T46378",
        "T46379",
        "T46380",
        "T46381",
        "T46417",
        "T46644",
        "T46676",
        "T46694",
        "T46695",
        "T46697",
        "T46698",
        "T46706",
        "T46707",
        "T46781",
        "T46981",
        "T47324",
        "T47331",
        "T47332",
        "T47395",
        "T47547",
        "T47553",
        "T47561",
        "T47562",
        "T47569",
        "T47590",
        "T47799",
        "T47800",
        "T47801",
        "T47802",
        "T48862",
        "T48864",
        "T50832",
        "T54355",
        "T54356",
        "T54357",
        "T54431",
        "T55109",
        "T55110",
        "T55112",
        "T55113",
        "T55114",
        "T55116",
        "T55118",
        "T55119",
        "T55120",
        "T5534",
        "T7699",
        "T8121",
        "T8493",
        "T9540",
    }
)

# A pedestrian area only means something to a satnav if it sits on a road a vehicle could
# otherwise have driven on. The source files as "aire piétonne" a great many things that are
# not roads: towpaths, rural tracks, park promenades, private condominium lanes, and 295
# nameless segments. Measured on 2026-09-07: 1 440 of the 3 635 pedestrian-area segments,
# 40 %, fall under one of the three motives below.
#
# What is deliberately **kept**: `Rue Saint Jean` (Vieux Lyon), `Quai Rambaud`,
# `Rue Victor Hugo`, `Rue Moncey`, `Place de la Mairie` — real pedestrian streets a driver
# must not enter. Dropping the whole measure to be rid of the noise would have cost 2 786
# emprises over 54 regulations, four fifths of which describe genuine restrictions.
#
# The filter reads the road's name, and a name is a weak predictor — the same shape of rule
# failed twice today on the API's refusals. It is defensible here because it qualifies
# business content rather than guessing an API behaviour, and because each motive is
# separately arguable. It stays coarse: `Chemin de la Digue` and `Voie Communale 6 des
# Carrières` survive it, and may well deserve to go too.
PEDESTRIAN_AREA_PRIVATE = r"(?i)priv"
PEDESTRIAN_AREA_NAMELESS = r"(?i)sans d[ée]nomination|sans nom"
PEDESTRIAN_AREA_NOT_A_ROAD = (
    r"(?i)^(chemin rural|promenade|parc |jardin|square |esplanade|berge|voie sans"
    r"|contre.all[ée]e|passerelle|halage)"
)

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
            raw_data.pipe(discard_refused_segments)
            .pipe(read_order_number)
            .pipe(discard_impossible_tonnages)
            .pipe(explode_into_measures)
            .pipe(compute_measure_fields)
            .pipe(discard_pedestrian_areas_off_the_road_network)
            .pipe(compute_vehicle_fields)
            .pipe(compute_period_fields)
            .pipe(compute_location_fields)
            .pipe(compute_regulation_fields)
            # Après l'identifiant : le chaînage regroupe par (arrêté, mesure, voie).
            .pipe(merge_contiguous_segments)
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


def discard_refused_segments(df: pl.DataFrame) -> pl.DataFrame:
    """Drop the segments the API refuses to accept anywhere.

    DiaLog validates a regulation as a whole: one emprise it will not take makes the whole
    POST fail, and with it every other emprise of that act. On 2026-09-07 that cost 17
    regulations and 6 376 emprises out of 25 170 — a quarter of the corpus — for a refusal
    the API states as « L'organisation ne semble pas avoir les compétences pour intervenir
    sur ce linéaire de route ».

    We do not know what that check tests. The obvious hypothesis — the source's own
    `domanialite` column — was measured and **does not hold**: segments of the État domain
    sit in 9 accepted regulations as well as in 7 refused ones. So the list below is not
    derived from a rule; it is the empirical answer of the API itself, obtained by probing
    the preprod segment by segment (`ai/tools/probe_refused_segments.py`, deliberately not
    versioned — probing production would be neither safe nor welcome).

    That makes it a **snapshot, not a truth**: if DiaLog widens the organisation's
    perimeter, entries here silently keep good segments out. Re-run the probe when the
    corpus moves, and treat a growing list as a signal rather than a fix.
    """
    if not REFUSED_SEGMENTS:
        return df
    refused = pl.col("codetroncon").is_in(list(REFUSED_SEGMENTS))
    n_refused = df.select(refused.sum()).item()
    if n_refused:
        logger.info(
            f"Discarding {n_refused} segments the API refuses (blocklist of "
            f"{len(REFUSED_SEGMENTS)} entries, measured on the preprod)"
        )
    return df.filter(~refused)


def discard_pedestrian_areas_off_the_road_network(df: pl.DataFrame) -> pl.DataFrame:
    """Drop the pedestrian areas that are not on a road, keep those that are.

    Applied **after** the measures are qualified, so it only ever touches
    `AIRE_PIETONNE`: a segment's speed limit or tonnage limit is unaffected by its
    pedestrian status. See `PEDESTRIAN_AREA_*` for what the three motives are and why the
    measure is filtered rather than dropped whole.
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


def discard_impossible_tonnages(df: pl.DataFrame) -> pl.DataFrame:
    """Drop the tonnage values no vehicle can carry — they are not vehicle limits.

    `limitationtonnage` mixes two grandeurs. Most values are what they look like, a limit
    on the weight of a lorry; a handful read 50, 70, 100, 120, 200 and 230 t, which is the
    load-bearing capacity of a structure, not a restriction on traffic. The API says the
    same thing in its own words — `vehicleSet.heavyweightMaxWeight` must be ≤ 44, the
    French legal maximum for a heavy goods vehicle — and refused 6 regulations over it on
    2026-09-07.

    Only the tonnage is dropped, never the row: a segment limited to 200 t *and* to 4 m of
    height keeps its height limit. A row left with no dimension at all simply stops being
    a dimension measure. R-35: we do not invent a threshold, and we do not publish a false
    one either.
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

    A segment limited to 30 km/h *and* closed to vehicles over 3.5 t states two measures.
    Qualifying them separately is what lets us drop the 50 km/h without losing the
    tonnage limit that sits on the same segment.

    A **zone à trafic limité** is the exception: it replaces the speed of its segments
    instead of sitting beside it. The source files the ZTL of the Presqu'île as 5, 20 or
    30 km/h depending on the street, which describes how one drives there and not what
    the sign at its entrance says — no entry, except local access. Publishing both would
    put two measures on the same segment, and for the 162 segments filed at 5 km/h the
    second would be a redundant `noEntry`. Set `ZTL_REPLACES_SPEED` to False to publish
    the speed alongside instead.
    """
    is_ztl = pl.col("ztl").fill_null(False)

    ztl = df.filter(is_ztl).with_columns(pl.lit("ztl").alias("measure_kind"))

    speeds = df.filter(
        pl.col("limitationvitesse").is_not_null()
        & (pl.col("limitationvitesse") != DEFAULT_URBAN_SPEED)
        & (~is_ztl if ZTL_REPLACES_SPEED else pl.lit(True))
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

    total = speeds.height + dimensions.height + ztl.height
    logger.info(
        f"Exploded {df.height} segments into {total} measures "
        f"({speeds.height} speed, {dimensions.height} dimension, {ztl.height} ZTL); "
        f"{df.height - speeds.height - ztl.height} speed measures discarded as the default "
        f"{DEFAULT_URBAN_SPEED} km/h or an empty speed"
    )
    return pl.concat([speeds, dimensions, ztl], how="vertical")


def compute_measure_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Measure type, speed, and the key that collapses identical measures into one.

    - `measure_type_`: `speedLimitation`, except for pedestrian areas and dimension
      limits which are `noEntry` (R-33 for the dimensions).
    - `measure_max_speed`: the source speed, on speed limits only.
    - `measure_group_key`: the measure's signature. Two rows sharing it inside the same
      regulation become one measure carrying both locations.
    """
    is_ztl = pl.col("measure_kind") == "ztl"
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
        pl.when(is_ztl)
        .then(pl.lit("ZTL"))
        .when(is_speed_limit)
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

    # `desserteLocale` est ce qui distingue une ZTL d'une interdiction sèche : on n'y
    # entre pas, sauf pour s'y rendre. Sans elle, on publierait une rue barrée là où la
    # Métropole autorise les riverains, les livraisons et les services.
    is_ztl = pl.col("measure_kind") == "ztl"
    exempted = (
        pl.when(is_ztl).then(pl.lit([VehicleExemptedTypeEnum.DESSERTELOCALE.value])).otherwise(None)
    )

    return df.with_columns(
        (~is_dimension).alias("vehicle_all_vehicles"),
        exempted.alias("vehicle_exempted_types"),
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

    The day goes through `start_of_local_day` rather than a hand-written `Z` suffix:
    DiaLog reads the offset it is given, so `T00:00:00Z` would place the start two hours
    before French midnight — on the previous day, all summer.
    """
    df = df.with_columns(pl.lit(datetime.date.today()).alias("_start_date"))
    return df.with_columns(
        start_of_local_day(df, "_start_date").alias("period_start_date"),
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


def principal_measure_of_each_order(df: pl.DataFrame) -> pl.DataFrame:
    """The one measure each order number actually decides.

    A numbered order regulates *one thing*: "Ville 30" limits to 30 km/h. But its number
    is typed in `precisionreglementation` on every segment of the zone, and those segments
    also carry whatever else applies there — a pedestrian area, a 7.5 t limit, a 20 km/h
    stretch. Keeping them all made `MGL-CT-2024RP44520` a regulation holding 30 km/h *and*
    noEntry *and* two tonnage limits *and* 20 km/h: an administrative act saying seven
    things it never said.

    The order's own measure is the one covering the most of its emprises. Measured on the
    2026-09-07 draw: 46 of the 48 multi-measure orders have a strict majority, and it is
    overwhelming where it matters — 5 462 of 5 701 emprises for 2024RP44520 (96 %). Ties
    and the two orders below 50 % (`2025ZTL001` at 41 %, `0AR20180018` at 47 %) are broken
    on emprise count then on the signature itself, so the choice never depends on row
    order and stays identical from one run to the next (R-20).
    """
    return (
        df.filter(pl.col("order_key").is_not_null())
        .group_by(["order_key", "measure_group_key"])
        .agg(pl.len().alias("_n"))
        .sort(["order_key", "_n", "measure_group_key"], descending=[False, True, False])
        .group_by("order_key", maintain_order=True)
        .first()
        .select("order_key", pl.col("measure_group_key").alias("_principal_measure"))
    )


def warn_ambiguous_principals(df: pl.DataFrame) -> None:
    """Report the orders where no measure is clearly the one the order decides.

    Two shapes deserve a human: a tie, where the tie-break picks a winner among equals,
    and a plurality below half, where the "principal" measure covers a minority of the
    order's emprises. The rule stays deterministic in both cases — but which measure a
    given order really decides is a business reading of the act, not something the
    emprise count can settle, so it is surfaced rather than silently arbitrated.
    """
    numbered = df.filter(pl.col("order_key").is_not_null())
    if not numbered.height:
        return

    per_measure = numbered.group_by(["order_key", "measure_group_key"]).agg(pl.len().alias("n"))
    per_order = per_measure.group_by("order_key").agg(
        pl.col("n").max().alias("top"),
        pl.col("n").sum().alias("total"),
        pl.len().alias("distinct_measures"),
        (pl.col("n") == pl.col("n").max()).sum().alias("tied"),
    )
    ambiguous = per_order.filter(
        (pl.col("distinct_measures") > 1)
        & ((pl.col("tied") > 1) | (pl.col("top") / pl.col("total") < 0.5))
    )
    if not ambiguous.height:
        return

    logger.warning(
        f"{ambiguous.height} numbered order(s) have no clear principal measure — the "
        "tie-break decided. Which measure the act really carries is a business call:"
    )
    for row in ambiguous.sort("total", descending=True).head(10).iter_rows(named=True):
        reason = "égalité" if row["tied"] > 1 else f"majorité relative {row['top']}/{row['total']}"
        logger.warning(f"  {row['order_key']}: {row['distinct_measures']} mesures, {reason}")


def compute_regulation_fields(df: pl.DataFrame) -> pl.DataFrame:
    """Identifier, category, subject and title of each regulation.

    The identifier rule, in full (R-23):

        MGL-CT-{order key}          when the free-text field yields an order number *and*
                                    this is the measure that order decides —
                                    `MGL-CT-2024RP44520`
        MGL-CT-{measure signature}  everything else, one metropolitan-wide regulation per
                                    distinct measure — `MGL-CT-V30`, `MGL-CT-GABARIT_T3_5`

    The second line covers two populations that behave alike: segments carrying no order
    number at all, and the *other* measures of a segment whose order number decides
    something else. Both are real measures with no act of their own to belong to, so both
    are gathered by what they do. A segment legitimately appears twice — once in its
    order's regulation for the speed, once in a metropolitan regulation for its tonnage
    limit — because those are two distinct restrictions at the same place.

    The municipality is deliberately absent from the key. Lyon's "Ville 30" order spills
    onto 12 neighbouring municipalities for a handful of boundary segments; prefixing by
    municipality would split one real order into 13. The price is that 61 short numbers
    (`845`, `956`) could merge two orders from two municipalities — one such case is
    known, and the arbitration of 2026-09-04 is to merge rather than to split.
    """
    df = df.join(principal_measure_of_each_order(df), on="order_key", how="left")

    # `_is_own_measure` also drives the title: a measure pushed out of its order must not
    # keep the order's name, or a metropolitan regulation would read "Arrêté n°…".
    df = df.with_columns(
        (
            pl.col("order_key").is_not_null()
            & (pl.col("measure_group_key") == pl.col("_principal_measure"))
        ).alias("_is_own_measure")
    )

    displaced = df.filter(pl.col("order_key").is_not_null() & ~pl.col("_is_own_measure"))
    if displaced.height:
        logger.info(
            f"{displaced.height} emprises leave their numbered order for the metropolitan "
            f"regulation of their own measure: they sit on a segment whose order number "
            f"decides something else ({displaced['measure_group_key'].n_unique()} distinct "
            f"measures over {displaced['order_key'].n_unique()} orders)"
        )
    warn_ambiguous_principals(df)

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
        .when(pl.col("measure_group_key") == pl.lit("ZTL"))
        .then(pl.lit("Zone à trafic limité, sauf desserte locale – Métropole de Lyon"))
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

    The layer cuts a street into as many rows as it has stretches of pavement, so one
    30 km/h street arrives as 43 emprises describing one continuous line. Joining them
    changes nothing about what is covered and everything about how many objects say it.

    Joining happens **only at a point where exactly two segments meet**. Where three or
    more meet, the point is a junction and choosing which branch continues which would be
    arbitrary — so the chain stops there. Two disjoint stretches of the same street never
    join, because no endpoint is shared: this is a topological merge, not a merge by name.

    Endpoints are compared exactly. Measured on the 2026-09-07 draw: 13 533 shared
    endpoints, and the count is identical whether coordinates are rounded to 5 or to 8
    decimals — the producer's geometries already close on each other, so no tolerance is
    invented here.
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
            # A segment whose two ends are the same point is a loop: it meets itself and
            # has nothing to be joined to.
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

    The key is `(regulation, measure, street)`: two segments only ever join if they state
    the same measure, on the same named street, inside the same regulation. Every other
    field of the group is identical by construction — the measure signature fixes the
    vehicle set and the measure type, and the period is the day of the run — so the first
    row carries them for the whole chain.
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
