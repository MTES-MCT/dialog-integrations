from integrations.co_paris.eudonet.data_source_integration import DataSourceIntegration
from integrations.co_paris.eudonet.free_text_filters import unread_act_texts
from integrations.co_paris.eudonet.prefilter import (
    measures_whose_articles_to_read,
    measures_whose_locations_to_read,
)
from integrations.co_paris.eudonet.published_elsewhere import (
    index_by_eudonet_number,
    load_frozen_elsewhere,
)
from integrations.co_paris.eudonet.raw_rows import Expansion, build_raw_dataframe
from settings import OrganizationSettings
from tests.co_paris.acts import everything, make_frame, make_row
from tests.co_paris.conftest import OfflineSettings
from tests.co_paris.eudonet_fixture import load_fixture


def clean(raw):
    settings = OrganizationSettings(OfflineSettings(), "co_paris")  # type: ignore[arg-type]
    source = DataSourceIntegration(settings, None)  # type: ignore[arg-type]
    source.numbers_published_elsewhere = index_by_eudonet_number(load_frozen_elsewhere())
    return source.select_regulation_measure_fields(
        source._apply_rules_to_canonical(source.compute_clean_data(raw))
    )


def test_the_prefilter_reads_less_and_publishes_the_same():
    tables = load_fixture()
    articled = measures_whose_articles_to_read(tables)
    numbers = index_by_eudonet_number(load_frozen_elsewhere())
    located = measures_whose_locations_to_read(tables, articled, numbers)
    full = build_raw_dataframe(tables, everything(tables))
    prefiltered = build_raw_dataframe(tables, Expansion(located, articled))
    assert prefiltered["l_file_id"].n_unique() < full["l_file_id"].n_unique()
    assert clean(prefiltered).equals(clean(full))


def test_an_act_text_is_refused_before_its_locations_only_when_no_location_could_explain_it():
    # A street named after "entre" is understood only on a location narrower than the street:
    # read leniently, the act is not refused before its locations are read.
    narrowing = make_row(a_articles=["Entre la rue de Rivoli et la rue Saint-Honoré."])
    unread = make_row(a_file_id=2, a_identifier="2024P10002", a_articles=["Les jours de pluie."])
    assert set(unread_act_texts(make_frame(narrowing, unread))) == {2}
