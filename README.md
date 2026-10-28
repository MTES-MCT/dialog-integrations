# DiaLog Integrations

Environnement d'exploration et d'intégration des arrêtés de circulation que les collectivités et les départements publient en open data, pour intégration dans la base [DiaLog](https://dialog.beta.gouv.fr/)

Pour comprendre ce que deviennent les données, du producteur jusqu'à DiaLog : [Architecture](docs/architecture.md).

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

Pour exécuter une intégration en local, il faut aussi un fichier `.env` : voir [Configuration](docs/configuration.md).

## CLI Usage

`dialog --help` et `dialog <commande> --help` donnent les options à jour.

* `dialog integrate ORGANISATION` : `--env` (`dev` par défaut, ou `prod`), `--identifiers` (liste séparée par des virgules ; ce run ne supprime ni ne clôt rien et laisse l'instantané intact), `--update-existing`, `--dry-run`, `--force-deletions`, `--json`, `--summary FICHIER` ;
* `dialog publish ORGANISATION` : publie tous les arrêtés de l'organisation ;
* `dialog notify --results JSON [--dry-run]` : poste le rapport de la nuit dans Tchap.

L'organisation est le nom d'un dossier de `integrations/`.

Exemples :
* `dialog integrate co_brest --env=dev`
* `dialog integrate dp_sarthe --env=prod --update-existing --identifiers=1,280,8,459,478,17`
* `dialog integrate co_lyon --env=dev --dry-run` (voir [Synchronisation](docs/synchronisation.md#--dry-run))

## Documentation

* [Architecture : ce que deviennent les données](docs/architecture.md)
* [Le modèle canonique et les contrôles](docs/modele-canonique.md)
* [Synchronisation : créer, mettre à jour, clore, supprimer](docs/synchronisation.md)
* [Configuration : .env, integrations.toml, CI](docs/configuration.md)
* [Ajouter une source](docs/ajouter-une-source.md)
* [Développement : tests et qualité](docs/developpement.md)
