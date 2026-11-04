import pytest

from integrations.co_limoges.limitation_tonnage.street_names import from_referential


@pytest.mark.parametrize(
    "filed, written",
    [
        ("SAINT AURELIEN (rue)", "Rue Saint Aurelien"),
        ("ALLOIS (rue des)", "Rue des Allois"),
        (
            "ANCIENNE ECOLE NORMALE D'INSTITUTEURS (rue de l')",
            "Rue de l'Ancienne Ecole Normale d'Instituteurs",
        ),
        ("DURKHEIM (rue Emile)", "Rue Emile Durkheim"),
        ("GAULLE (avenue General de)", "Avenue General de Gaulle"),
        ("LA ROCHEFOUCAULD (rue)", "Rue La Rochefoucauld"),
        ("PIERRE AU BOIS (rue de la)", "Rue de la Pierre au Bois"),
        ("JOLIOT-CURIE (rue Irene et Frederic)", "Rue Irene et Frederic Joliot-Curie"),
        ("KAHN (allee emile)", "Allee emile Kahn"),
        ("FILLES-NOTRE-DAME (rue des)", "Rue des Filles-Notre-Dame"),
        ("Rue des Arts", "Rue des Arts"),
        ("ECOLE (rue de l’)", "Rue de l'Ecole"),
    ],
)
def test_a_filed_street_name_reads_as_on_a_sign(filed, written):
    assert from_referential(filed) == written
