import polars as pl

from toolkit.transforms.copies import drop_copies


def test_a_copy_goes_and_the_one_keeping_its_road_code_stays():
    df = pl.DataFrame(
        {
            "act": ["944050", "944050", "128"],
            "geometry": ["g1", "g1", "g1"],
            "road": [None, 108, 108],
        }
    )

    result = drop_copies(df, ["act", "geometry"], prefer_filled="road")

    assert sorted(result.rows()) == [("128", "g1", 108), ("944050", "g1", 108)]


def test_copies_keep_the_source_order_and_the_first_filled_one():
    df = pl.DataFrame(
        {
            "act": ["z", "x", "y", "x", "x"],
            "geometry": ["g0", "g1", "g2", "g1", "g1"],
            "road": [None, None, 1, 5, 3],
        }
    )

    result = drop_copies(df, ["act", "geometry"], prefer_filled="road")

    assert result.rows() == [("z", "g0", None), ("x", "g1", 5), ("y", "g2", 1)]
