import polars as pl

from toolkit.transforms.generic_regulations import (
    generic_regulations,
    looks_like_act,
    numbered_or_generic,
)


def test_one_regulation_per_measure_titled_with_its_sections():
    df = pl.DataFrame({"measure_group_key": ["V30", "V30", "V90"]})

    result = generic_regulations(
        df, prefix="CD53-LV-", title=pl.lit("Limitation"), other_category_text="Limitation"
    )

    assert result.select("regulation_identifier", "regulation_title").rows() == [
        ("CD53-LV-V30", "Limitation (2 sections)"),
        ("CD53-LV-V30", "Limitation (2 sections)"),
        ("CD53-LV-V90", "Limitation (1 section)"),
    ]
    assert result["regulation_category"].unique().to_list() == ["permanentRegulation"]


def test_an_act_keeps_its_number_with_every_measure_it_sets():
    df = pl.DataFrame(
        {
            "arrete": ["201800209", "201800209", "128", None],
            "measure_group_key": ["T7.5", "T2.5", "T3.5", "T3.5"],
        }
    )

    result = numbered_or_generic(
        df,
        keeps_number=looks_like_act(pl.col("arrete"), min_length=6),
        number=pl.col("arrete"),
        prefix="LIMOGES-LT-",
        numbered_title=pl.lit("Arrêté"),
        generic_title=pl.lit("Limitation"),
        other_category_text="Limitation",
    )

    assert sorted(result.select("regulation_identifier", "measure_group_key").rows()) == [
        ("LIMOGES-LT-201800209", "T2.5"),
        ("LIMOGES-LT-201800209", "T7.5"),
        ("LIMOGES-LT-T3.5", "T3.5"),
        ("LIMOGES-LT-T3.5", "T3.5"),
    ]


def test_a_number_looks_like_an_act_from_its_length_and_characters():
    numbers = pl.Series(["944050", "2013P140", " 08001442 ", "128", "2019-1", None])

    result = pl.select(looks_like_act(pl.lit(numbers), min_length=6)).to_series()

    assert result.to_list() == [True, True, True, False, False, False]
