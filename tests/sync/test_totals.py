import pytest

from integrations.sync.totals import Totals, TotalsStore, total_shift


@pytest.mark.parametrize(
    ("previous", "current"),
    [
        pytest.param((1534, 774), (2342, 808), id="identifiers renewed every day"),
        pytest.param((877, 880), (3, 880), id="instance emptied"),
    ],
)
def test_a_total_moving_without_production_is_flagged(previous, current):
    shift = total_shift(Totals(*previous), Totals(*current))

    assert shift == {"dialog": [previous[0], current[0]], "produced": [previous[1], current[1]]}


@pytest.mark.parametrize(
    ("previous", "current"),
    [
        pytest.param((3189, 792), (3189, 747), id="temporary dataset shrinking"),
        pytest.param((1617, 1589), (1515, 1485), id="deleted and no longer produced"),
        pytest.param((1045, 903), (1089, 907), id="under the share"),
        pytest.param(None, (10, 10), id="no previous run"),
    ],
)
def test_a_total_that_production_explains_is_not_flagged(previous, current):
    assert total_shift(previous and Totals(*previous), Totals(*current)) is None


def test_an_unreadable_totals_file_is_ignored(tmp_path):
    store = TotalsStore("co_test", base_dir=tmp_path)
    store.path.parent.mkdir(parents=True)
    store.path.write_text("{", encoding="utf-8")

    assert store.load() is None
