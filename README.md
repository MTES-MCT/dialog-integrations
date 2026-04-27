# DiaLog Integrations

Environnement d'exploration et d'intégration de la donnée publique des arrêtés prefectoraux disponible en open-data pour intégration dans la base [DiaLog](https://dialog.beta.gouv.fr/)

## Technologies

* [Python](https://www.python.org/) `>=3.11`
* [Polars](https://pola.rs/)


## Environnement et installation


### Installation de l'environnement python

Installer uv (package manager et gestionnaire d'environnement python)

* [uv](https://docs.astral.sh/uv/)

Si ce n'est pas déjà le cas, installer python 3.11 pour uv :

```shell
uv python install 3.11
```

Puis, dans le repo du projet

```shell
uv sync
```

À chaque fois que l'on travaille dans le projet, activer l'env

```shell
source .venv/bin/activate
```

> [!NOTE]
> `alias uvenv="source .venv/bin/activate"`

Une fois dans l'environnement, la commande `dialog` est disponible.

> [!NOTE]
> Pour quitter l'env actif
> `deactivate`

### Installation du module `api`

```shell
make api:fetch-spec
make api:generate-client
```

> [!NOTE]
> Raccourci :  
> `make api:update`

### Ça marche ?

Vérifier que la ligne de commande `dialog` fonctionne :

```shell
dialog --help
```


## CLI Usage

`dialog --help`

```text
Usage: dialog [OPTIONS] COMMAND [ARGS]...

 Dialog CLI

╭─ Options ───────────────────────────────────────────────────────────────────────────────╮
│ --install-completion          Install completion for the current shell.                 │
│ --show-completion             Show completion for the current shell, to copy it or      │
│                               customize the installation.                               │
│ --help                        Show this message and exit.                               │
╰─────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ──────────────────────────────────────────────────────────────────────────────╮
│ integrate  Sync data for a specific organization to Dialog API.                         │
│ publish    Publish all measures                                                         │
╰─────────────────────────────────────────────────────────────────────────────────────────╯
```

`dialog integrate --help`

```text
Usage: dialog integrate [OPTIONS]
                         ORGANIZATION:{dp_aveyron|co_brest|...}
 
 Sync data for a specific organization to Dialog API.

╭─ Arguments ─────────────────────────────────────────────────────────────────────────────╮
│ *    organization      ORGANIZATION:{dp_aveyron|co_bre  [required]                      │
│                        st|dp_sarthe|co_dijon}                                           │
╰─────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Options ───────────────────────────────────────────────────────────────────────────────╮
│ --identifiers                                COMMA_LIST  List of ids to restrict to.    │
│ --update-existing    --no-update-existing                Update existing regulations    │
│ --env                                        TEXT        Environment: dev or prod       │
│                                                          [default: dev]                 │
│ --help                                                   Show this message and exit.    │
╰─────────────────────────────────────────────────────────────────────────────────────────╯
```

Exemples :
* `dialog integrate dp_aveyron --env=prod --update-existing --identifiers "25067/RESTRICTION-GABARIT"`
* `dialog integrate co_brest --env=dev`
* `dialog integrate dp_sarthe --env=prod --update-existing --identifiers=1,280,8,459,478,17`


## .env

Pour exécuter une intégration en local, créer un fichier d'environnement à la racine correspondant à l'organisation :

`.env.co_maville.dev` ou `.env.co_maville.prod`

```text
DIALOG_BASE_URL="https://dialog.beta.gouv.fr"
DIALOG_CLIENT_ID="12345678-abcd-9876-5432-10abcdef1234"
DIALOG_CLIENT_SECRET="XXXXXXXXXXXXXXXX-abcdefghijklmnopqrstuvwxyz"
```

## Organisation des dossiers

* `api` : dossier non-versionné contenant le sdk généré pour l'api
* `integrations` : Chaque intégration est dans un dossier portant le nom de l'organisation. Ex `co_brest` ou `dp_sarthes` (ce sera le nom à spécifier dans la CLI pour l'intégration)

```text
co_maville
├── __init__.py
├── integration.py
├── chantiers_routiers
│   ├── __init__.py
│   ├── schema.py
│   └── data_source_integration.py
├── limitations_vitesstes
│   ├── __init__.py
│   ├── schema.py
│   └── data_source_integration.py
```

* `integration.py` : fichier déclarant les intégrations actives et le statut par défaut de publication.
* `schema.py` : schéma du fichier d'entrée attendu par le script d'intégration au format [Pandera](https://pandera.readthedocs.io/).
* `data_source_integration.py` : fichier d'intégration pour 1 data-source donnée.


## CI et qualité

* [ruff](https://docs.astral.sh/ruff/)
* [pytest](https://docs.pytest.org/)
* [pyright](https://github.com/microsoft/pyright)

### Utilitaires

* `make app:prepare-commit` : prépare le code avant un commit
* `make app:test` : lance la test suite
* `make app:test-watch` : lance la test suite en "watch"