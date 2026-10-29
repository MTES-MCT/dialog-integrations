import polars as pl

from toolkit.transforms.pedestrian_area import add_pedestrian_area_speed


def test_each_ban_gets_a_walking_pace_twin_without_exemption():
    df = pl.DataFrame(
        {
            "regulation_identifier": ["A", "A"],
            "measure_type_": ["noEntry", "noEntry"],
            "measure_group_key": ["AREA", "OTHER"],
            "measure_max_speed": [None, None],
            "vehicle_exempted_types": [["desserteLocale", "other"], None],
            "vehicle_other_exempted_type_text": ["navettes", None],
        },
        schema_overrides={"measure_max_speed": pl.Int32},
    )

    result = add_pedestrian_area_speed(
        df, is_ban=pl.col("measure_group_key") == "AREA", twin_group_key=pl.lit("AREA-V5")
    )

    twin = result.filter(pl.col("measure_group_key") == "AREA-V5").row(0, named=True)
    assert result.height == 3
    assert twin["regulation_identifier"] == "A"
    assert twin["measure_type_"] == "speedLimitation"
    assert twin["measure_max_speed"] == 5
    assert twin["vehicle_exempted_types"] is None
    assert twin["vehicle_other_exempted_type_text"] is None


def test_without_ban_nothing_is_added():
    df = pl.DataFrame({"measure_group_key": ["OTHER"]})

    assert add_pedestrian_area_speed(
        df, is_ban=pl.col("measure_group_key") == "AREA", twin_group_key=pl.lit("x")
    ).equals(df)
