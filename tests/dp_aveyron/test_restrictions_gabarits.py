"""Tests for Aveyron prescriptions routieres: grouping, identifiers and dating.

The source publishes one row per (stretch, sign). R-28 says a row is not an arrete, so
these tests pin the two ways rows become one: by the number the producer cites, and
otherwise by the measure itself, departmentally.
"""

from urllib.parse import quote

import polars as pl

from integrations.dp_aveyron.restrictions_gabarits.data_source_integration import (
    MAX_IDENTIFIER_LENGTH,
    compute_period_fields,
    compute_regulation_fields,
    measure_group_key,
)


def signs(**overrides):
    """A frame carrying what compute_regulation_fields reads, as the pipe leaves it."""
    data = {
        "numero_dar": ["A18R0380"],
        "panneau_type": ["B13"],
        "panneau_value": [3.5],
        "prescripti": ["Limitation de tonnage"],
        "date_darre": ["19/11/2003"],
    }
    data.update(overrides)
    return pl.DataFrame(data).with_columns(
        pl.col("numero_dar").cast(pl.Utf8),
        pl.col("date_darre").cast(pl.Utf8),
        pl.col("panneau_value").cast(pl.Float64),
        # As compute_measure_fields leaves it, so the key never drifts from the sign a
        # test overrides.
        measure_group_key().alias("measure_group_key"),
    )


# --- Identifiers -------------------------------------------------------------------


def test_identifier_comes_from_the_arrete_number():
    result = compute_regulation_fields(signs())

    assert result["regulation_identifier"].to_list() == ["AV-GB-A18R0380"]


def test_a_sign_without_an_arrete_joins_the_departmental_regulation_of_its_measure():
    """R-28: a source row is not an arrete. Numberless rows group by sign and value."""
    result = compute_regulation_fields(signs(numero_dar=[None]))

    assert result["regulation_identifier"].to_list() == ["AV-GB-B13-3-5"]


def test_the_producer_placeholders_count_as_no_arrete_at_all():
    """ "0 arrete" and "arlot" say there is none; taken literally they build a catch-all."""
    result = compute_regulation_fields(
        signs(
            numero_dar=["0 arrete", "ARLOT"],
            panneau_type=["B13", "B12"],
            panneau_value=[3.5, 4.1],
            prescripti=["Limitation de tonnage", "Limitation de hauteur"],
            date_darre=[None, None],
        )
    )

    assert result["regulation_identifier"].to_list() == ["AV-GB-B13-3-5", "AV-GB-B12-4-1"]


def test_the_same_sign_on_different_roads_makes_one_departmental_regulation():
    """One regulation per measure, not one fabricated act per section of road."""
    result = compute_regulation_fields(
        signs(
            numero_dar=[None] * 3,
            panneau_type=["B13", "B13", "B12"],
            panneau_value=[19.0, 19.0, 4.0],
            prescripti=["Limitation de tonnage"] * 2 + ["Limitation de hauteur"],
            date_darre=[None] * 3,
        )
    )

    assert result["regulation_identifier"].to_list() == [
        "AV-GB-B13-19",
        "AV-GB-B13-19",
        "AV-GB-B12-4",
    ]


def test_a_sign_carrying_no_value_keys_on_the_sign_alone():
    """B9i (caravans) and B18c (hazardous goods) are the whole measure by themselves."""
    result = compute_regulation_fields(
        signs(
            numero_dar=[None, None],
            panneau_type=["B9i", "B18c"],
            panneau_value=[None, None],
            prescripti=["Interdiction caravanes", "Interdiction TMD"],
            date_darre=[None, None],
        )
    )

    assert result["regulation_identifier"].to_list() == ["AV-GB-B9i", "AV-GB-B18c"]


def test_identifiers_are_url_path_safe_and_within_the_api_cap():
    """They travel in the path of DELETE and of the publish endpoint."""
    result = compute_regulation_fields(
        signs(
            numero_dar=["143/2025 Conques", None],
            panneau_type=["B13", "B12"],
            panneau_value=[3.5, 4.1],
            prescripti=["Limitation de tonnage", "Limitation de hauteur"],
            date_darre=[None, None],
        )
    )

    for identifier in result["regulation_identifier"]:
        assert quote(identifier) == identifier
        assert identifier.isascii()
        assert len(identifier) <= MAX_IDENTIFIER_LENGTH


# --- Titles ------------------------------------------------------------------------


def test_title_of_a_grouped_fallback_names_the_measure_not_one_of_its_stretches():
    """The API keeps the first row's title, and the group holds N stretches."""
    result = compute_regulation_fields(
        signs(
            numero_dar=[None, None],
            panneau_type=["B13", "B13"],
            panneau_value=[19.0, 19.0],
            prescripti=["Limitation de tonnage"] * 2,
            date_darre=[None, None],
        )
    )

    assert result["regulation_title"].unique().to_list() == [
        "Limitation de tonnage 19 t - Département de l'Aveyron (2 sections)"
    ]


def test_title_of_a_numbered_arrete_lists_every_prescription_of_the_group():
    result = compute_regulation_fields(
        signs(
            numero_dar=["A18R0380", "A18R0380"],
            panneau_type=["B13", "B18c"],
            panneau_value=[3.5, None],
            prescripti=["Limitation de tonnage", "Interdiction TMD"],
            date_darre=["19/11/2003", "19/11/2003"],
        )
    )

    assert result["regulation_title"].unique().to_list() == [
        "A18R0380 - Interdiction TMD ; Limitation de tonnage"
    ]


# --- Dating a measure that now carries N emprises ----------------------------------


def test_a_numbered_arrete_is_dated_from_the_earliest_day_its_rows_carry():
    """A measure carries one period; the producer sometimes writes two dates for one act.

    Measured case: `AV-GB-02-207`, whose two B12 rows are dated a day apart.
    """
    df = compute_regulation_fields(
        signs(
            numero_dar=["02-207", "02-207"],
            panneau_type=["B12", "B12"],
            panneau_value=[4.0, 4.0],
            prescripti=["Limitation de hauteur"] * 2,
            date_darre=["20/02/2002", "19/02/2002"],
        )
    )

    result = compute_period_fields(df)

    assert result["period_start_date"].unique().to_list() == ["2002-02-19T00:00:00+01:00"]
    assert result["period_is_permanent"].to_list() == [True, True]
    assert result["period_end_date"].to_list() == [None, None]


def test_a_grouped_fallback_is_dated_from_the_run_not_from_one_of_its_members():
    """R-39: the group gathers stretches from different acts, so no member's date fits.

    One real signature spans three dates from 2003 to 2019; picking one would assert a
    commencement date for the other stretches that nothing supports.
    """
    df = compute_regulation_fields(
        signs(
            numero_dar=[None, None, None],
            panneau_type=["B13", "B13", "B13"],
            panneau_value=[19.0, 19.0, 19.0],
            prescripti=["Limitation de tonnage"] * 3,
            date_darre=["19/11/2003", None, "29/11/2019"],
        )
    )

    result = compute_period_fields(df)

    assert result["period_start_date"].unique().to_list() == [None]


def test_an_undated_numbered_arrete_stays_undated():
    """Signed, signposted and in force; only the day it was signed is missing.

    Left null rather than dated the day of the run: `integrations/sync/dating.py`
    dates it when DiaLog is written, so the comparison never sees a moving date.
    """
    df = compute_regulation_fields(signs(date_darre=[None]))

    result = compute_period_fields(df)

    assert result["period_start_date"].to_list() == [None]
    assert result["period_is_permanent"].to_list() == [True]


def test_the_two_digit_year_the_producer_sometimes_writes_is_read_as_this_century():
    """Inferring the format lets polars read "12/01/18" as year 18."""
    df = compute_regulation_fields(signs(date_darre=["12/01/18"]))

    result = compute_period_fields(df)

    assert result["period_start_date"].to_list() == ["2018-01-12T00:00:00+01:00"]
