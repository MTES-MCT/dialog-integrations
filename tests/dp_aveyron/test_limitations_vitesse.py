"""Tests for Aveyron limitations de vitesse preprocessing."""

from urllib.parse import quote

import polars as pl

from api.dia_log_client.models import DirectionEnum
from integrations.dp_aveyron.limitations_vitesse.data_source_integration import (
    MAX_IDENTIFIER_LENGTH,
    REFUSED_SEGMENTS,
    DataSourceIntegration,
    compute_location_fields,
    compute_period_fields,
    compute_regulation_fields,
    compute_split_order,
    discard_directional_stretches,
    discard_refused_segments,
    measure_group_key,
)


def stretches(**overrides):
    """A frame already carrying the location and measure fields, as the pipe produces them."""
    data = {
        "num_arrete": ["A21R0233"],
        "agglo": [None],
        "measure_max_speed": [70],
        "location_road_number": ["D22"],
        "location_from_point_number": ["12"],
        "location_from_abscissa": [290],
        "location_to_point_number": ["12"],
        "location_to_abscissa": [550],
    }
    data.update(overrides)
    # An all-null column would infer as Null; pandera coerces these two to String.
    return (
        pl.DataFrame(data)
        .with_columns(pl.col("num_arrete").cast(pl.Utf8), pl.col("agglo").cast(pl.Utf8))
        # As compute_measure_fields leaves it, so the grouping key is never hand-typed
        # out of step with the speed a test overrides.
        .with_columns(measure_group_key().alias("measure_group_key"))
    )


def test_identifier_comes_from_the_arrete_number():
    result = compute_regulation_fields(stretches())

    assert result["regulation_identifier"].to_list() == ["AV-LV-A21R0233"]


def test_identifier_normalises_what_the_producer_writes():
    """Aveyron writes slashes, spaces and trailing hyphens in num_arrete."""
    df = stretches(
        num_arrete=["143/2025 Conques", "A21R0212 - A21R0213", "A21R0233 -", "AR_2025_059"],
        agglo=[None] * 4,
        measure_max_speed=[70] * 4,
        location_road_number=["D22"] * 4,
        location_from_point_number=["1", "2", "3", "4"],
        location_from_abscissa=[1, 2, 3, 4],
        location_to_point_number=["1", "2", "3", "4"],
        location_to_abscissa=[9, 9, 9, 9],
    )

    result = compute_regulation_fields(df)

    assert result["regulation_identifier"].to_list() == [
        "AV-LV-143-2025-Conques",
        "AV-LV-A21R0212-A21R0213",
        "AV-LV-A21R0233",
        "AV-LV-AR-2025-059",
    ]


def test_a_stretch_without_an_arrete_joins_the_departmental_regulation_of_its_speed():
    """R-28: a source row is not an arrete. Numberless stretches group by measure."""
    result = compute_regulation_fields(stretches(num_arrete=[None]))

    assert result["regulation_identifier"].to_list() == ["AV-LV-V70"]


def test_numberless_stretches_of_one_speed_collapse_into_a_single_regulation():
    """Four real measures instead of one fabricated act per section of road."""
    df = stretches(
        num_arrete=[None] * 4,
        agglo=[None] * 4,
        measure_max_speed=[70, 70, 30, 110],
        location_road_number=["D22", "D840", "D22", "D911"],
        location_from_point_number=["12", "3", "40", "7"],
        location_from_abscissa=[290, 10, 100, 5],
        location_to_point_number=["12", "4", "41", "8"],
        location_to_abscissa=[550, 20, 200, 15],
    )

    result = compute_regulation_fields(df)

    assert result.height == 4
    assert sorted(set(result["regulation_identifier"])) == [
        "AV-LV-V110",
        "AV-LV-V30",
        "AV-LV-V70",
    ]


def test_the_fallback_key_ignores_the_road_and_the_commune():
    """Grouping geographically would move the identifier the day a trace is redrawn."""
    df = stretches(
        num_arrete=[None, None],
        agglo=[None, "RODEZ"],
        measure_max_speed=[30, 30],
        location_road_number=["D22", "D993"],
        location_from_point_number=["1", "88"],
        location_from_abscissa=[0, 400],
        location_to_point_number=["2", "89"],
        location_to_abscissa=[100, 500],
    )

    result = compute_regulation_fields(df)

    assert result["regulation_identifier"].unique().to_list() == ["AV-LV-V30"]


def test_identifier_is_url_path_safe():
    """Identifiers travel in the path of DELETE and publish: no slash, space or accent."""
    df = stretches(
        num_arrete=["143/2025 Conques", None],
        agglo=[None, None],
        measure_max_speed=[70, 30],
        location_road_number=["D22", "D22"],
        location_from_point_number=["12", "13"],
        location_from_abscissa=[290, 10],
        location_to_point_number=["12", "13"],
        location_to_abscissa=[550, 20],
    )

    identifiers = compute_regulation_fields(df)["regulation_identifier"].to_list()

    for identifier in identifiers:
        assert quote(identifier) == identifier
        assert identifier.isascii()


def test_identifier_stays_within_the_api_cap():
    """Both forms fit. The producer reference is the only half that can grow.

    Nothing here truncates — an over-long reference is reported as an error, because a
    silently shortened identifier would orphan the arrete at the next run.
    """
    df = stretches(
        num_arrete=["A21R0188 - A21R0232", None],
        agglo=[None, None],
        measure_max_speed=[70, 110],
        location_road_number=["D22", "D911"],
        location_from_point_number=["12", "7"],
        location_from_abscissa=[290, 5],
        location_to_point_number=["12", "8"],
        location_to_abscissa=[550, 15],
    )

    identifiers = compute_regulation_fields(df)["regulation_identifier"].to_list()

    assert identifiers == ["AV-LV-A21R0188-A21R0232", "AV-LV-V110"]
    assert all(len(identifier) <= MAX_IDENTIFIER_LENGTH for identifier in identifiers)


def test_default_limits_without_an_arrete_are_kept_and_grouped_by_speed():
    """50 km/h in an agglomeration and 90 km/h outside one are published (R-70).

    They restate the law, but a satnav needs them all the same. Each speed gathers
    under one grouped fallback.
    """
    df = stretches(
        num_arrete=[None] * 4,
        agglo=["RODEZ", None, None, "RODEZ"],
        measure_max_speed=[50, 90, 50, 90],
        location_road_number=["D1", "D2", "D3", "D4"],
        location_from_point_number=["1", "2", "3", "4"],
        location_from_abscissa=[1, 2, 3, 4],
        location_to_point_number=["1", "2", "3", "4"],
        location_to_abscissa=[9, 9, 9, 9],
    )

    result = compute_regulation_fields(df)

    assert result["location_road_number"].to_list() == ["D1", "D2", "D3", "D4"]
    assert result["regulation_identifier"].to_list() == ["AV-LV-V50", "AV-LV-V90"] * 2


def test_a_default_limit_carrying_an_arrete_is_kept():
    df = stretches(num_arrete=["A21R0233"], agglo=["RODEZ"], measure_max_speed=[50])

    assert compute_regulation_fields(df).height == 1


def test_both_directions_of_one_stretch_are_a_single_measure():
    """The producer lists a stretch once per direction; we publish neither side."""
    df = stretches(
        num_arrete=["A21R0233", "A21R0233"],
        agglo=[None, None],
        measure_max_speed=[70, 70],
        location_road_number=["D22", "D22"],
        location_from_point_number=["12", "12"],
        location_from_abscissa=[290, 290],
        location_to_point_number=["12", "12"],
        location_to_abscissa=[550, 550],
    )

    assert compute_regulation_fields(df).height == 1


def test_a_stretch_limited_differently_each_way_keeps_both_measures():
    """Aveyron's D920 at PR 39+719: 90 one way, 50 the other."""
    df = stretches(
        num_arrete=["A21R0191", "A21R0191"],
        agglo=[None, None],
        measure_max_speed=[90, 50],
        location_road_number=["D920", "D920"],
        location_from_point_number=["39", "39"],
        location_from_abscissa=[719, 719],
        location_to_point_number=["39", "39"],
        location_to_abscissa=[880, 880],
    )

    result = compute_regulation_fields(df)

    assert result.height == 2
    assert result["regulation_identifier"].n_unique() == 1
    assert sorted(result["measure_max_speed"].to_list()) == [50, 90]


def test_the_arrete_row_wins_over_the_same_measure_without_one():
    df = stretches(
        num_arrete=[None, "A26R0042"],
        agglo=[None, None],
        measure_max_speed=[70, 70],
        location_road_number=["D888", "D888"],
        location_from_point_number=["68", "68"],
        location_from_abscissa=[900, 900],
        location_to_point_number=["69", "69"],
        location_to_abscissa=[374, 374],
    )

    result = compute_regulation_fields(df)

    assert result["regulation_identifier"].to_list() == ["AV-LV-A26R0042"]


def test_title_of_a_grouped_fallback_names_the_measure_not_one_of_its_stretches():
    """The API keeps the first row's title, and the group holds N stretches."""
    result = compute_regulation_fields(stretches(num_arrete=[None]))

    assert result["regulation_title"][0] == (
        "Limitation de vitesse 70 km/h - Département de l'Aveyron (1 section)"
    )


def test_title_of_an_arrete_covers_the_whole_group():
    """The API keeps the title of the first row, so it must describe every section."""
    df = stretches(
        num_arrete=["A21R0233", "A21R0233"],
        agglo=[None, None],
        measure_max_speed=[70, 30],
        location_road_number=["D22", "D22A"],
        location_from_point_number=["12", "13"],
        location_from_abscissa=[290, 10],
        location_to_point_number=["12", "13"],
        location_to_abscissa=[550, 20],
    )

    result = compute_regulation_fields(df)

    assert result["regulation_title"].unique().to_list() == [
        "A21R0233 - Limitation de vitesse - D22, D22A (2 sections)"
    ]


def test_preprocess_drops_the_stray_header_line():
    """The export carries a row whose prd reads "Début"; coercion would fail on it."""
    df = pl.DataFrame(
        {
            "prd": ["12", "Début"],
            "prf": ["12", "13"],
            "abd": ["290", "0"],
            "abf": ["550", "0"],
        }
    )

    # preprocess_raw_data touches neither the settings nor the client.
    result = DataSourceIntegration(None, None).preprocess_raw_data(df)  # type: ignore[arg-type]

    assert result.height == 1
    assert result["prd"].to_list() == ["12"]


def test_abscissas_are_integers_as_the_api_types_them():
    """The producer ships decimals on a few abscissas; SaveNumberedRoadDTO wants int."""
    result = compute_location_fields(road(route=["12_D22"], abd=[469.0866]))

    assert result.schema["location_from_abscissa"] == pl.Int64
    assert result["location_from_abscissa"].to_list() == [469]


def road(**overrides):
    """A raw frame carrying what compute_location_fields reads."""
    data = {
        "route": ["12_D920"],
        "cote": ["droite"],
        "prd": [39],
        "abd": [719.0],
        "prf": [39],
        "abf": [880.0],
        "measure_max_speed": [90],
    }
    data.update(overrides)
    return pl.DataFrame(data)


def test_a_stretch_signposted_on_both_sides_applies_both_ways():
    df = road(
        cote=["droite", "gauche"],
        **{k: v * 2 for k, v in road().to_dict(as_series=False).items() if k != "cote"},
    )

    result = compute_location_fields(df)

    assert result["location_direction"].to_list() == [DirectionEnum.BOTH.value] * 2


def test_a_stretch_signposted_on_the_right_only_applies_towards_increasing_pr():
    """`droite` faces traffic going towards increasing PR, which is A_TO_B."""
    result = compute_location_fields(road(cote=["droite"]))

    assert result["location_direction"].to_list() == [DirectionEnum.A_TO_B.value]


def test_a_stretch_signposted_on_the_left_only_applies_the_other_way():
    result = compute_location_fields(road(cote=["gauche"]))

    assert result["location_direction"].to_list() == [DirectionEnum.B_TO_A.value]


def test_opposite_limits_each_keep_their_own_direction():
    """Aveyron's D920 at PR 39+719: 90 towards increasing PR, 50 the other way."""
    df = road(
        cote=["droite", "gauche"],
        route=["12_D920"] * 2,
        prd=[39, 39],
        abd=[719.0, 719.0],
        prf=[39, 39],
        abf=[880.0, 880.0],
        measure_max_speed=[90, 50],
    )

    result = compute_location_fields(df)

    assert result["location_direction"].to_list() == [
        DirectionEnum.A_TO_B.value,
        DirectionEnum.B_TO_A.value,
    ]


def test_an_unknown_side_falls_back_to_both_ways():
    """Overstating the restriction beats pointing it the wrong way."""
    result = compute_location_fields(road(cote=["centre"]))

    assert result["location_direction"].to_list() == [DirectionEnum.BOTH.value]


def test_the_period_is_permanent_and_undated():
    """R-39: the source carries no date, so none is presumed.

    The start used to be the day of the run; that made every limit look modified each
    morning, the date being part of what the synchronization compares. It is now left
    null and dated by `integrations/sync/dating.py` when DiaLog is written.
    """
    result = compute_period_fields(stretches())

    assert result["period_start_date"].to_list() == [None]
    assert result["period_end_date"].to_list() == [None]
    assert result["period_is_permanent"].to_list() == [True]


# --- Stretches the API refuses to geolocate ------------------------------------------


def refusable(**overrides):
    """A frame carrying the location fields the blocklist keys on."""
    data = {
        "location_road_number": ["D911"],
        "location_from_point_number": ["6"],
        "location_from_abscissa": [636],
        "location_to_point_number": ["15"],
        "location_to_abscissa": [52],
    }
    data.update(overrides)
    return pl.DataFrame(data)


def test_a_refused_stretch_is_dropped_before_it_sinks_its_regulation():
    """The API validates a regulation as a whole: one refused stretch sinks it."""
    assert "D911-de-6+636-a-15+52" in REFUSED_SEGMENTS

    assert discard_refused_segments(refusable()).height == 0


def test_a_neighbouring_stretch_of_the_same_road_is_kept():
    """It is stretches that are refused, not roads — D911 has 44 emprises, 2 refused."""
    kept = discard_refused_segments(
        refusable(location_from_point_number=["20"], location_to_point_number=["21"])
    )

    assert kept.height == 1


def test_the_blocklist_matches_on_the_whole_stretch_not_a_prefix():
    """`D911-de-6+636-a-15+520` must not be caught by the entry ending in `15+52`."""
    kept = discard_refused_segments(refusable(location_to_abscissa=[520]))

    assert kept.height == 1


def test_only_what_applies_both_ways_is_sent_while_production_ignores_the_direction():
    """D-19: a one-way limit would be broadcast both ways. Lifted by deleting one pipe."""
    df = pl.DataFrame(
        {
            "location_road_number": ["D1", "D2", "D3"],
            "location_direction": [
                DirectionEnum.BOTH.value,
                DirectionEnum.A_TO_B.value,
                DirectionEnum.B_TO_A.value,
            ],
        }
    )

    assert discard_directional_stretches(df)["location_road_number"].to_list() == ["D1"]


def test_split_order_follows_the_road_then_the_milestones():
    df = pl.DataFrame(
        {
            "location_road_number": ["D920", "D12", "D920", "D12"],
            "location_from_point_number": ["39", "10", "4", "9"],
            "location_from_abscissa": [719, 0, 100, 500],
        }
    )

    ranked = compute_split_order(df).sort("regulation_split_order")

    assert ranked["location_road_number"].to_list() == ["D12", "D12", "D920", "D920"]
    assert ranked["location_from_point_number"].to_list() == ["9", "10", "4", "39"]
    assert ranked["regulation_split_order"].to_list() == [0, 1, 2, 3]
