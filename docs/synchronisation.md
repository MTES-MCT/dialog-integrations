# Synchronisation : créer, mettre à jour, clore, supprimer

Sans réglage, la pipeline serait **additive** : elle créerait les arrêtés absents de DiaLog, sans rien mettre à jour ni toucher à ce qui a disparu de la source. Ce n'est plus permis par omission : une organisation déclare un `identifier_prefix`, ou `legacy_additive = True` pour dire qu'elle ne fait qu'ajouter (Brest, Sarthe, Issy, Rennes, intégrées avant les préfixes) ; sinon l'exécution refuse de démarrer. Une organisation active les autres opérations en surchargeant des attributs de classe dans son `integration.py` :

```python
class Integration(BaseIntegration):
    identifier_prefix = "MGL-CHP-"   # borne toute opération hors création
    update_changed = True               # republier ce qui a changé depuis l'envoi précédent
    close_missing = True                # clore ce qui a disparu de la source (ou delete_missing = True pour le supprimer)
    max_updates_per_run = 50
    max_closures_per_run = 50           # ou max_deletions_per_run
    max_creations_per_run = None        # plafond non armé
```

| Opération | Source de vérité |
|---|---|
| Création | `GET /api/organization/identifiers` — identifiant absent de DiaLog |
| Suppression (`delete_missing`) | même endpoint — identifiant présent, **dans notre préfixe**, absent de la production du jour |
| Clôture (`close_missing`) | même critère, mais l'arrêté **reste** : sa date de fin est ramenée à la veille de l'exécution (fin de journée, Europe/Paris). L'arrêté est relu dans DiaLog, reconstruit en corps d'écriture et renvoyé par `PUT` (`sync/closure.py`). Un arrêté déjà expiré n'est pas réécrit. Un chantier annulé avant d'avoir commencé se termine à sa date de début. Un arrêté que la relecture ne rend pas fidèlement (tonnage ou gabarit, type de véhicule « autre », zone sans géométrie calculée) est **supprimé** : clos, il perdrait sa restriction ; laissé, il resterait en vigueur |
| Mise à jour | l'**instantané** de ce qu'on a envoyé la fois précédente, via `PUT /api/regulations` — sauf pour un arrêté à emprises `zone`, que l'API refuse en PUT (500) : il est supprimé puis recréé par le flux zone |

`delete_missing` et `close_missing` s'excluent. Une source qui est un instantané de chantiers en cours (Lyon) **clôt** : l'équipe veut garder l'historique des chantiers.

Un plafond à `0` (`max_updates_per_run = 0`, `max_deletions_per_run = 0`) fait de la synchronisation un **rapport seul** : le diff est calculé et notifié chaque nuit, lot retenu, rien n'est appliqué tant qu'on ne lève pas le plafond (`--force-deletions` ne relâche que les suppressions et clôtures). L'Aveyron a tourné ainsi sur le staging, puis est passé à 5 pour la production.

## Le préfixe, garde-fou principal

Une organisation DiaLog reçoit souvent des arrêtés par d'autres canaux que ce dépôt. `identifier_prefix` borne le rayon d'action :

* **sans préfixe, la suppression et la clôture sont refusées** (exception), pas seulement désactivées ;
* un identifiant produit qui ne commence pas par le préfixe **arrête l'exécution** avant toute écriture : c'est le garde-fou contre un préfixe appliqué deux fois ou oublié dans une branche de la transformation.

## Une source gelée

Chaque source d'une organisation qui supprime ou clôt déclare son propre préfixe (`identifier_prefix = "AV-LV-"` sur la classe de la source), contenu dans celui de l'organisation. Une source est **gelée** pour la nuit — rien n'est clos ni supprimé sous son préfixe, ses arrêtés restent tels que DiaLog les détient — dans trois cas :

* **elle a planté** : l'exception est journalisée et les autres sources de l'organisation écrivent normalement. La commande sort en erreur et Tchap affiche ❌ ;
* **son volume s'est effondré** : elle produit moins de 70 % des arrêtés de la veille (`state/…/totals.json`). Un export vide ou tronqué se lirait sinon comme la disparition de tout son corpus, et sous le plafond tout serait clos ou supprimé. La référence reste le dernier volume de confiance jusqu'à ce qu'on relâche avec `--force-deletions` ;
* **elle est en pause** dans `integrations.toml` (`paused = ["nom"]`) : elle n'est pas lue. Retirer une source de `data_sources` à la place ferait passer tout ce qu'elle a produit pour disparu.

Une source sans préfixe propre gèle celui de l'organisation, donc toutes ses voisines : le test de contrat (`tests/contract/`) exige un préfixe par source dès qu'une organisation à plusieurs sources supprime ou clôt.

## Les plafonds et les lots retenus

Un lot au-dessus de son plafond est **retenu en entier** — rien n'est appliqué — et signalé « à revoir manuellement ». Son ancienne empreinte reste dans l'instantané : il est redétecté à l'identique le lendemain, jusqu'à ce qu'on le relâche.

Pour relâcher, lire d'abord le rapport de la nuit (résumé du run GitHub), puis lancer le workflow `integrate` à la main, depuis l'onglet Actions : `organization` = l'organisation, `force_deletions` coché. Le run se fait avec les secrets et l'instantané de la CI, et laisse une trace. En local, réservé au staging :

```shell
uv run dialog integrate co_lyon --dry-run           # lire le rapport
uv run dialog integrate co_lyon --force-deletions   # relâcher les suppressions ou les clôtures
```

`--force-deletions` ne relâche que le lot de ce qui a disparu de la source — suppressions ou clôtures, selon l'organisation.

Ce qui a disparu de la source attend aussi, sans plafond atteint, dans deux cas : tant qu'un lot de créations ou de mises à jour est retenu, et la nuit où une création ou une mise à jour est restée sans réponse (panne, 5xx). Ces écritures portent peut-être ses tronçons : le retirer d'abord laisserait la route sans limite. Un refus de l'API (4xx) ne fait pas attendre. Les créations, elles, ne sont jamais retenues par un autre lot. Le rapport dit « en attente des lots ci-dessus », Tchap « lot retenu ». `--force-deletions` relâche dans tous les cas ; le rapport ne le propose que pour un lot au-dessus de son propre plafond.

## L'instantané

`state/{hôte cible}/{organisation}/{source}.json.gz` (l'hôte DiaLog écrit : ce qu'on a envoyé au staging ne dit rien de la prod ; l'ancien chemin `state/{organisation}/` est encore lu s'il manque le nouveau) : un digest par arrêté (titre, catégorie, objet, et par mesure le type, la vitesse, la période, le jeu de véhicules et la localisation). On compare à cette empreinte de **notre propre envoi**, jamais à une relecture de DiaLog : l'API réécrit une partie de ce qu'elle reçoit et une relecture republierait tout le corpus chaque jour.

* `DIALOG_STATE_DIR` déplace le dossier (en CI il vit dans le cache GitHub Actions).
* **Instantané absent = aucune mise à jour**, et il est reconstruit à la fin de l'exécution. Un cache perdu coûte une journée de mises à jour, jamais une réécriture de masse.
* Seuls les arrêtés effectivement écrits y entrent ; les supprimés en sortent. Les arrêtés **clos** y restent, avec leur nouvelle date de fin : c'est ainsi que le lendemain les sait terminés sans les relire. Un arrêté disparu de la source et inconnu de l'instantané est relu dans DiaLog, et n'est réécrit que s'il court encore.

## `--dry-run`

```shell
uv run dialog integrate co_lyon --env=dev --dry-run
```

Calcule tout — extraction, transformation, lots — et **n'écrit rien**, ni dans DiaLog ni dans l'instantané. Seule requête réseau vers DiaLog : le `GET /api/organization/identifiers`, en lecture. Le rapport donne l'entonnoir (lignes brutes, lignes nettoyées, arrêtés, mesures), les lots avec leurs plafonds, les identifiants concernés, un diff champ par champ pour chaque mise à jour, et les lots retenus. Le même rapport est journalisé avant écriture en exécution réelle.

`--json` imprime le résultat de l'exécution sur la sortie standard (`{"success": true, "created": n, "updated": n, "deleted": n, "closed": n, "refused": n, "held": {…}, "source": {…}}` — `refused` compte les arrêtés que la pipeline elle-même a écartés : zone couvrant plusieurs routes parallèles, mesure impossible à construire) ; les journaux restent sur la sortie d'erreur, donc `uv run dialog integrate co_lyon --json > result.json` produit un fichier propre. C'est ce que consomme la CI.

## Volumétries source

Une source de données peut publier ses propres volumétries en renseignant `self.metrics: dict[str, int]` pendant `fetch_raw_data` (par exemple `{"arrêtés du périmètre": 5297, "mesures": 11230}`). `BaseIntegration` les lit si elles existent, les affiche dans le rapport et les fait remonter dans le message Tchap. Aucune source ne s'en sert aujourd'hui.
