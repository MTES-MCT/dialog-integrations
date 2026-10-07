"""Tests for the Nantes work-site layer: the identifier must survive a reload.

The producer reloads the layer every working day and renumbers `gid` and `objectid`.
The regulation is keyed on `numero_chantier`, and its rows are sorted, so that a reload
changes neither the identifiers nor the order of the measures.
"""

import polars as pl

from integrations.co_nantes.circulation_chantier.data_source_integration import (
    compute_regulation_fields,
)


def streets(**overrides):
    """Two streets of one work-site order, as the pipe leaves them before this step."""
    data = {
        "gid": [3690536, 3690537],
        "numero_chantier": ["DAET26_03173DAT1", "DAET26_03173DAT1"],
        "voie": ["Rue de Strasbourg", "Allée Baco"],
        "commune": ["Nantes", "Nantes"],
        "motif": ["Travaux réseau", "Travaux réseau"],
        "nature": ["Gaz", "Gaz"],
        "type_chantier": ["Programmé", "Programmé"],
    }
    data.update(overrides)
    return pl.DataFrame(data)


def test_identifier_is_the_order_number_not_the_gid():
    df = compute_regulation_fields(streets())

    assert df["regulation_identifier"].to_list() == ["NM-CC-DAET26_03173DAT1"] * 2


def test_renumbered_gid_keeps_the_identifier():
    before = compute_regulation_fields(streets())
    after = compute_regulation_fields(streets(gid=[3692001, 3692002]))

    assert before["regulation_identifier"].to_list() == after["regulation_identifier"].to_list()


def test_rows_come_out_in_the_same_order_whatever_the_order_served():
    df = streets()
    forward = compute_regulation_fields(df)
    backward = compute_regulation_fields(df.reverse())

    assert (
        forward["voie"].to_list()
        == backward["voie"].to_list()
        == [
            "Allée Baco",
            "Rue de Strasbourg",
        ]
    )
