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
│ --dry-run                                                Compute everything, write      │
│                                                          nothing, print the report.     │
│ --force-deletions                                        Release a deletion batch held  │
│                                                          by its cap.                    │
│ --json                                                   Print the run result as JSON   │
│                                                          on stdout (for CI).            │
│ --help                                                   Show this message and exit.    │
╰─────────────────────────────────────────────────────────────────────────────────────────╯
```

Exemples :
* `dialog integrate dp_aveyron --env=prod --update-existing --identifiers "25067/RESTRICTION-GABARIT"`
* `dialog integrate co_brest --env=dev`
* `dialog integrate dp_sarthe --env=prod --update-existing --identifiers=1,280,8,459,478,17`
* `dialog integrate co_paris --env=dev --dry-run` (voir « Synchronisation » ci-dessous)


## Synchronisation

Par défaut la pipeline est **additive** : elle crée les arrêtés absents de DiaLog, ne met rien à jour et ne supprime rien. Une organisation peut activer les trois opérations en surchargeant des attributs de classe dans son `integration.py` :

```python
class Integration(BaseIntegration):
    identifier_prefix = "PARIS-EUDO-"   # borne toute opération destructrice
    delete_missing = True               # supprimer ce qui a disparu de la source
    update_changed = True               # republier ce qui a changé depuis l'envoi précédent
    max_deletions_per_run = 50
    max_updates_per_run = 300
    max_creations_per_run = None        # plafond non armé
```

| Opération | Source de vérité |
|---|---|
| Création | `GET /api/organization/identifiers` — identifiant absent de DiaLog |
| Suppression | même endpoint — identifiant présent, **dans notre préfixe**, absent de la production du jour |
| Mise à jour | l'**instantané** de ce qu'on a envoyé la fois précédente, via `PUT /api/regulations` |

### Le préfixe, garde-fou principal

Une organisation DiaLog reçoit souvent des arrêtés par d'autres canaux que ce dépôt. `identifier_prefix` borne le rayon d'action :

* **sans préfixe, la suppression est refusée** (exception), pas seulement désactivée ;
* un identifiant produit qui ne commence pas par le préfixe **arrête l'exécution** avant toute écriture : c'est le garde-fou contre un préfixe appliqué deux fois ou oublié dans une branche de la transformation.

### Les plafonds et les lots retenus

Un lot au-dessus de son plafond est **retenu en entier** — rien n'est appliqué — et signalé « à revoir manuellement ». Son ancienne empreinte reste dans l'instantané : il est redétecté à l'identique le lendemain, jusqu'à ce qu'on le relâche.

```shell
uv run dialog integrate co_paris --env=prod --dry-run           # lire le rapport
uv run dialog integrate co_paris --env=prod --force-deletions   # relâcher les suppressions
```

`--force-deletions` ne relâche que le lot de suppressions.

### L'instantané

`state/{organisation}/{source}.json.gz` : un digest par arrêté (titre, catégorie, objet, et par mesure le type, la vitesse, la période, le jeu de véhicules et la localisation). On compare à cette empreinte de **notre propre envoi**, jamais à une relecture de DiaLog : l'API réécrit une partie de ce qu'elle reçoit et une relecture republierait tout le corpus chaque jour.

* `DIALOG_STATE_DIR` déplace le dossier (en CI il vit dans le cache GitHub Actions).
* **Instantané absent = aucune mise à jour**, et il est reconstruit à la fin de l'exécution. Un cache perdu coûte une journée de mises à jour, jamais une réécriture de masse.
* Seuls les arrêtés effectivement écrits y entrent ; les supprimés en sortent.

### `--dry-run`

```shell
uv run dialog integrate co_paris --env=dev --dry-run
```

Calcule tout — extraction, transformation, lots — et **n'écrit rien**, ni dans DiaLog ni dans l'instantané. Seule requête réseau vers DiaLog : le `GET /api/organization/identifiers`, en lecture. Le rapport donne l'entonnoir (lignes brutes, lignes nettoyées, arrêtés, mesures), les trois lots avec leurs plafonds, les identifiants concernés, un diff champ par champ pour chaque mise à jour, et les lots retenus. Le même rapport est journalisé avant écriture en exécution réelle.

`--json` imprime le résultat de l'exécution sur la sortie standard (`{"success": true, "created": n, "updated": n, "deleted": n, "held": {…}, "source": {…}}`) ; les journaux restent sur la sortie d'erreur, donc `uv run dialog integrate co_paris --json > result.json` produit un fichier propre. C'est ce que consomme la CI.

### Volumétries source

Une source de données peut publier ses propres volumétries en renseignant `self.metrics: dict[str, int]` pendant `fetch_raw_data` (par exemple `{"arrêtés du périmètre": 5297, "mesures": 11230}`). `BaseIntegration` les lit si elles existent, les affiche dans le rapport et les fait remonter dans le message Tchap. Les sources qui n'en déclarent pas ne sont pas concernées.


## .env

Pour exécuter une intégration en local, créer un fichier d'environnement à la racine correspondant à l'organisation :

`.env.co_maville.dev` ou `.env.co_maville.prod`

```text
DIALOG_BASE_URL="https://dialog.beta.gouv.fr"
DIALOG_CLIENT_ID="12345678-abcd-9876-5432-10abcdef1234"
DIALOG_CLIENT_SECRET="XXXXXXXXXXXXXXXX-abcdefghijklmnopqrstuvwxyz"
```

### `.env.dev` — l'identité de travail par défaut (dev uniquement)

Un fichier `.env.dev` est chargé **avant** le fichier de l'organisation. Il porte l'identité utilisée par défaut, quelle que soit l'organisation passée à la CLI.

Ordre de priorité en dev, du plus faible au plus fort :

1. `.env.dev` ;
2. `.env.{organization}.dev` ;
3. variables d'environnement du processus.


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
