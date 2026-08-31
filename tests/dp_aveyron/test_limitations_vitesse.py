"""Tests for Aveyron limitations de vitesse preprocessing."""

from urllib.parse import quote

import polars as pl

from api.dia_log_client.models import DirectionEnum
from integrations.dp_aveyron.limitations_vitesse.data_source_integration import (
    MAX_IDENTIFIER_LENGTH,
    DataSourceIntegration,
    compute_location_fields,
    compute_regulation_fields,
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
    return pl.DataFrame(data).with_columns(
        pl.col("num_arrete").cast(pl.Utf8), pl.col("agglo").cast(pl.Utf8)
    )


def test_identifier_comes_from_the_arrete_number():
    result = compute_regulation_fields(stretches())

    assert result["regulation_identifier"].to_list() == ["lim-vitesse-A21R0233"]


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
        "lim-vitesse-143-2025-Conques",
        "lim-vitesse-A21R0212-A21R0213",
        "lim-vitesse-A21R0233",
        "lim-vitesse-AR-2025-059",
    ]


def test_identifier_is_reconstructed_from_the_stretch_when_no_arrete():
    """The speed stays out of the key: changing a limit must update, not orphan."""
    result = compute_regulation_fields(stretches(num_arrete=[None]))

    assert result["regulation_identifier"].to_list() == ["lim-vitesse-D22-de-12+290-a-12+550"]


def test_identifier_is_url_path_safe_apart_from_the_plus():
    """Identifiers travel in the path of DELETE and publish.

    Everything must already be URL-safe, so no slash, space or accent survives. The `+`
    of the PR notation is the one deliberate exception: it percent-encodes to %2B, which
    is unambiguous in a path segment, where an encoded slash is not.
    """
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
        assert quote(identifier, safe="+") == identifier
        assert identifier.isascii()


def test_identifier_stays_within_the_api_cap():
    df = stretches(
        location_road_number=["D1088B1"],
        location_from_point_number=["999"],
        location_from_abscissa=[9999],
        location_to_point_number=["999"],
        location_to_abscissa=[9999],
        num_arrete=[None],
    )

    identifier = compute_regulation_fields(df)["regulation_identifier"][0]

    assert len(identifier) <= MAX_IDENTIFIER_LENGTH


def test_only_default_limits_without_an_arrete_are_dropped():
    """50 km/h in an agglomeration and 90 km/h outside one restate the law.

    The test is per row, so a 90 inside an agglomeration and a 50 outside one are kept.
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

    assert result["location_road_number"].to_list() == ["D3", "D4"]


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

    assert result["regulation_identifier"].to_list() == ["lim-vitesse-A26R0042"]


def test_title_of_a_reconstructed_stretch_reads_as_french():
    result = compute_regulation_fields(stretches(num_arrete=[None]))

    assert result["regulation_title"][0] == (
        "Limitation de vitesse 70 km/h - D22, du PR 12+290 au PR 12+550"
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
