import polars as pl
import pytest
from shapely.geometry import box

from integrations.co_limoges.integration import Integration
from integrations.co_limoges.limitation_tonnage import data_source_integration
from integrations.co_limoges.limitation_tonnage.data_source_integration import (
    DataSourceIntegration,
)
from toolkit.transforms.perimeter import Perimeter


def line(x0: float, y0: float, x1: float, y1: float) -> str:
    return f"LINESTRING ({x0} {y0}, {x1} {y1})"


# (geometry, num_arrete, id_voie, tonnage); the street, when named, is set below
ROWS = [
    # Kept under the act, each stretch its own emprise; and a copy without its road.
    (line(1.250, 45.83, 1.252, 45.83), "944050", 10.0, "3.5"),
    (line(1.252, 45.83, 1.254, 45.83), "944050", 10.0, "3.5"),
    (line(1.250, 45.83, 1.252, 45.83), "944050", None, "3.5"),
    # Kept in the city-wide regulation of its weight: a short number.
    (line(1.260, 45.84, 1.262, 45.84), "128", 20.0, "7.5"),
    # Kept under its act, without the weight below a heavy goods vehicle.
    (line(1.270, 45.85, 1.272, 45.85), "201800209", 30.0, "7.5"),
    (line(1.280, 45.86, 1.282, 45.86), "201800209", 31.0, "2.5"),
    # Kept under its act, with both weights it sets.
    (line(1.340, 45.92, 1.342, 45.92), "202000001", 35.0, "7.5"),
    (line(1.350, 45.93, 1.352, 45.93), "202000001", 36.0, "3.5"),
    # Kept under its act, without a road code; and a whole number of tonnes.
    (line(1.330, 45.91, 1.332, 45.91), "201701097", None, "7.5"),
    (line(1.360, 45.94, 1.362, 45.94), "201907548", 37.0, "10"),
    # No act: in the city-wide regulation of its weight.
    (line(1.285, 45.86, 1.287, 45.86), None, 32.0, "3.5"),
    # Runs 390 m into the next commune; and one wholly there.
    (line(1.395, 45.87, 1.405, 45.87), "20001335", 33.0, "3.5"),
    (line(2.000, 45.90, 2.002, 45.90), "20001336", 34.0, "3.5"),
    # Two weights over 39 m of one stretch.
    (line(1.2900, 45.875, 1.2940, 45.875), "2013P142", 40.0, "7.5"),
    (line(1.2935, 45.875, 1.2975, 45.875), "20053537", 41.0, "3.5"),
    # Two acts over 39 m of one street: both kept.
    (line(1.2900, 45.877, 1.2940, 45.877), "128", 42.0, "3.5"),
    (line(1.2935, 45.877, 1.2975, 45.877), "944050", 42.0, "3.5"),
    # One stretch drawn twice under one act: the longer trace stays.
    (line(1.300, 45.88, 1.302, 45.88), "09002172", 50.0, "3.5"),
    (line(1.300, 45.88, 1.302, 45.88001), "09002172", 51.0, "3.5"),
    # Twin traces of one street 13 m apart; and two streets 13 m apart, both kept.
    (line(1.310, 45.89, 1.312, 45.89), "202500389", 60.0, "3.5"),
    (line(1.310, 45.89012, 1.312, 45.89012), "202500389", 60.0, "3.5"),
    (line(1.370, 45.95, 1.372, 45.95), "202500390", 62.0, "3.5"),
    (line(1.370, 45.95012, 1.372, 45.95012), "202500390", 63.0, "3.5"),
    # No readable weight, one of them over a kept stretch, which stays.
    (line(1.320, 45.90, 1.322, 45.90), "201615390", 70.0, "x"),
    (line(1.330, 45.91, 1.332, 45.91), "201615391", 71.0, ""),
    # No trace, or an unreadable one.
    (None, "201615392", 72.0, "3.5"),
    ("LINESTRING (1.3 45.9", "201615393", 73.0, "3.5"),
]
COLUMNS = ["geometry", "num_arrete", "id_voie", "tonnage"]
STREETS = {10.0: "ARTS (rue des)"}


@pytest.fixture
def source() -> DataSourceIntegration:
    source = DataSourceIntegration(None, None)  # type: ignore[arg-type]
    source.perimeter = Perimeter.build("insee", "87085", [box(1.1, 45.7, 1.4, 46.0)])
    raw = pl.DataFrame(ROWS, schema=COLUMNS, orient="row").with_columns(
        nom_voie=pl.col("id_voie").replace_strict(STREETS, default=None, return_dtype=pl.String)
    )
    source.fetch_raw_data = lambda: raw  # type: ignore[method-assign]
    return source


@pytest.fixture
def clean(source: DataSourceIntegration) -> pl.DataFrame:
    return source.compute_data_regulations()


def test_the_order_the_layer_comes_in_changes_nothing(source, clean):
    raw = source.fetch_raw_data()
    source.fetch_raw_data = lambda: raw.reverse()  # type: ignore[method-assign]

    assert source.compute_data_regulations().equals(clean)


def test_an_act_keeps_its_number_with_every_weight_it_sets(clean):
    integration = Integration.__new__(Integration)
    regulations = integration.create_regulations(clean, DataSourceIntegration)

    locations = {
        str(r.identifier): [len(m.locations) for m in r.measures]  # type: ignore[union-attr, arg-type]
        for r in regulations
    }
    assert locations == {
        "LIMOGES-LT-944050": [3],
        "LIMOGES-LT-201701097": [1],
        "LIMOGES-LT-201907548": [1],
        "LIMOGES-LT-201800209": [1],
        "LIMOGES-LT-202000001": [1, 1],
        "LIMOGES-LT-09002172": [1],
        "LIMOGES-LT-T7.5": [1],
        "LIMOGES-LT-T3.5": [2],
        "LIMOGES-LT-202500390": [2],
    }


def test_each_left_out_row_is_counted_under_its_rule(source, clean):
    counts = {key.split(" (")[0]: n for key, n in source.rule_counts.items()}

    assert counts == {
        "hors du périmètre de l'organisation": 1,
        "tonnage sous 3,5 t": 1,
        "tracé qui déborde du périmètre": 1,
        "valeurs différentes pour un même sens d'un même tronçon": 2,
        "même tronçon tracé deux fois": 1,
        "tracé doublé en parallèle": 2,
        "gabarit ou poids lourds sans seuil": 2,
        "géométrie manquante": 2,
    }
    assert (source.dataset_restrictions, source.retained_restrictions) == (26, 14)


def test_a_regulation_names_its_act_and_its_weight(clean):
    titles = dict(clean.select("regulation_identifier", "regulation_title").unique().iter_rows())

    assert titles == {
        "LIMOGES-LT-944050": (
            "Arrêté n° 944050 – Circulation interdite aux véhicules de plus de 3,5 t – Limoges"
        ),
        "LIMOGES-LT-201701097": (
            "Arrêté n° 201701097 – Circulation interdite aux véhicules de plus de 7,5 t – Limoges"
        ),
        "LIMOGES-LT-201907548": (
            "Arrêté n° 201907548 – Circulation interdite aux véhicules de plus de 10 t – Limoges"
        ),
        "LIMOGES-LT-201800209": (
            "Arrêté n° 201800209 – Circulation interdite aux véhicules de plus de 7,5 t – Limoges"
        ),
        "LIMOGES-LT-202000001": "Arrêté n° 202000001 – Limitations de tonnage – Limoges",
        "LIMOGES-LT-09002172": (
            "Arrêté n° 09002172 – Circulation interdite aux véhicules de plus de 3,5 t – Limoges"
        ),
        "LIMOGES-LT-T7.5": (
            "Circulation interdite aux véhicules de plus de 7,5 t – Limoges (1 section)"
        ),
        "LIMOGES-LT-T3.5": (
            "Circulation interdite aux véhicules de plus de 3,5 t – Limoges (2 sections)"
        ),
        "LIMOGES-LT-202500390": (
            "Arrêté n° 202500390 – Circulation interdite aux véhicules de plus de 3,5 t – Limoges"
        ),
    }


def test_every_regulation_is_permanent_from_its_first_writing(clean):
    periods = clean.select(
        "period_is_permanent", "period_start_date", "period_end_date", "period_recurrence_type"
    ).unique()

    assert periods.rows() == [(True, None, None, "everyDay")]


def test_a_location_names_its_street_or_else_its_road_code(clean):
    numbered = clean.filter(
        pl.col("regulation_identifier").is_in(["LIMOGES-LT-944050", "LIMOGES-LT-201701097"])
    )

    assert sorted(numbered["location_label"].to_list()) == [
        "Limoges",
        "Limoges (code voie 42)",
        "Rue des Arts, Limoges",
        "Rue des Arts, Limoges",
    ]


def test_the_layer_is_read_with_the_names_of_its_road_codes(monkeypatch):
    layer = pl.DataFrame({"id_voie": [108.0, 7.0, None]})
    perimeter = Perimeter.build("insee", "87085", [box(1.1, 45.7, 1.4, 46.0)])
    arcgis = data_source_integration.arcgis
    monkeypatch.setattr(arcgis, "read_layer", lambda url: layer)
    monkeypatch.setattr(arcgis, "coded_values", lambda url, field: {108: "ARTS (rue des)"})
    monkeypatch.setattr(Perimeter, "fetch", classmethod(lambda cls, *code: perimeter))
    source = DataSourceIntegration(None, None)  # type: ignore[arg-type]

    raw = source.fetch_raw_data()

    assert raw["nom_voie"].to_list() == ["ARTS (rue des)", None, None]
    assert source.perimeter is perimeter
