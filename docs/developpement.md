# Développement : tests et qualité

## Outils

* [ruff](https://docs.astral.sh/ruff/) : lint et formatage
* [pytest](https://docs.pytest.org/) : tests
* [pyright](https://github.com/microsoft/pyright) : types

## Commandes

* `make app:prepare-commit` : prépare le code avant un commit
* `make app:test` : lance la suite de tests
* `make app:test-watch` : lance la suite de tests en « watch »

## Les tests

* `tests/{organisation}/` : les tests d'une source, avec son jeu de données figé (`{source}.csv`) ;
* `tests/contract/` : ce que toute organisation et toute source doivent respecter, paramétré sur les dossiers d'`integrations/` et sur `integrations.toml` : une source nouvelle est couverte dès qu'elle est déclarée ;
* `tests/sync/` : la synchronisation ;
* `tests/toolkit/` : les connecteurs et transformations partagés ;
* `tests/test_*.py` : le reste du moteur (exécution, contrôles, corps d'écriture, API, rapport, notifications).
