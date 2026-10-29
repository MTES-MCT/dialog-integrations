import polars as pl
import pytest

from toolkit.transforms.titles import ELLIPSIS, truncate, truncate_text


@pytest.mark.parametrize(
    "value, expected",
    [
        ("a" * 10, "a" * 10),
        ("a" * 11, "a" * 7 + ELLIPSIS),
        (None, None),
    ],
)
def test_truncate_keeps_the_limit_and_marks_the_cut(value, expected):
    df = pl.DataFrame({"text": [value]}, schema={"text": pl.Utf8})

    assert df.select(truncate(pl.col("text"), 10)).item() == expected


@pytest.mark.parametrize("value", ["a" * 10, "a" * 11, "a" * 300])
def test_truncate_text_matches_the_expression(value):
    df = pl.DataFrame({"text": [value]})

    assert truncate_text(value, 10) == df.select(truncate(pl.col("text"), 10)).item()
