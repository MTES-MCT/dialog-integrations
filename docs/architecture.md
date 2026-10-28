# Architecture : ce que deviennent les données

Ce dépôt lit les arrêtés de circulation que des collectivités publient en open data, les
convertit au format de DiaLog et les envoie à son API. Il n'a pas de base de données : tout
passe par l'API HTTP de DiaLog, et l'état gardé d'une nuit à l'autre tient dans quelques
fichiers (`state/`).

## Le trajet d'une donnée

Chaque nuit, la CI lance une exécution par organisation DiaLog (`co_brest`, `dp_aveyron`…).
Une organisation a une ou plusieurs sources ; chaque source suit le même trajet jusqu'au
modèle canonique, puis toutes les sources de l'organisation sont traitées ensemble.

```mermaid
flowchart TD
    subgraph SRC["Une source — integrations/{org}/{source}/"]
        A["Données du producteur<br/>Opendatasoft, ArcGIS, WFS, API…"]
        B["Lecture<br/>fetch_raw_data · toolkit/connectors/"]
        C["Validation du format brut<br/>schema.py (Pandera)"]
        D["Transformation propre à la source<br/>compute_clean_data · toolkit/transforms/<br/>filtres, découpage, règles métier"]
        E["Modèle canonique<br/>canonical/regulation_measure.py<br/>une ligne = une mesure sur une emprise"]
        A --> B --> C --> D --> E
    end

    subgraph ORG["L'organisation — run/night.py"]
        F["Contrôles communs<br/>checks/rows.py"]
        G["Corps d'écriture de l'API<br/>payloads/"]
        H["Décider quoi écrire<br/>sync/plan.py<br/>créer · mettre à jour · clore · supprimer"]
        I["Rapport<br/>report/sync_report.py"]
        J["Écritures vers DiaLog<br/>dialog/"]
        K["Mémoire de la nuit<br/>sync/snapshot.py · sync/totals.py"]
        F --> G --> H --> I --> J --> K
    end

    E --> F
    J --> L[("DiaLog")]
    K -.->|"lu la nuit suivante"| H
    J --> M["Message Tchap et résumé GitHub<br/>report/"]
```

`--dry-run` s'arrête au rapport : tout est calculé, rien n'est écrit.

## Un modèle canonique tardif

Dans une architecture d'intégration classique, chaque source est traduite **tout de suite**
vers le modèle canonique, par une simple correspondance de champs ; tout le reste du
traitement porte sur ce modèle commun. Ici, le format canonique vient plus tard : les
sources de données sont de natures très variées, et chacune a historiquement été
développée indépendamment des autres.

```mermaid
flowchart LR
    subgraph H["Ici"]
        direction LR
        h1["source"] --> h2["traitements propres<br/>à la source"] --> h3["canonique"] --> h4["contrôles"] --> h5["envoi"]
        r["banque de règles métier<br/>toolkit/rules/ (à venir)"] -.->|"appelée par"| h2
    end

    subgraph U["Habituellement"]
        direction LR
        u1["source"] --> u2["DTO → canonique"] --> u3["traitements communs"] --> u4["stockage ou envoi"]
    end
```

Ici, chaque source fait l'essentiel de son travail **sur son propre format** : elle filtre,
découpe, applique les règles métier, regroupe par numéro d'arrêté, puis n'arrive au modèle
canonique qu'à la fin. Après lui ne restent que les contrôles communs et l'envoi. Le modèle
canonique est donc surtout le **contrat de sortie** de chaque source : ce qu'elle doit
produire, quelle que soit la façon dont elle y arrive.

Conséquence pour qui lit le code : une règle métier se cherche aujourd'hui dans la source
qui l'applique (`integrations/{org}/{source}/data_source_integration.py`), pas dans un
module commun. Seuls les contrôles de `checks/rows.py` valent pour toutes les sources.
Les règles qu'appliquent plusieurs sources iront dans `toolkit/rules/` : une banque de
méthodes qu'une source appelle pendant sa transformation, une méthode par règle.

Dans le code, le modèle canonique s'appelle encore souvent le « pivot »
(`unknown_pivot_columns`, `PIVOT_FAMILIES`) : c'est le même objet.

## Où vit chaque étape

```text
cli.py              la commande `dialog` : integrate, publish, notify
settings.py         la configuration d'une organisation (.env)
registry.py         lit integrations.toml : ce qui tourne chaque nuit, et où
integrations.toml

canonical/          le contrat entre une source et la suite
  regulation_measure.py   les colonnes du modèle canonique
  source.py               le gabarit d'une source : lire → valider → transformer → garder le canonique
checks/
  rows.py           les contrôles communs sur les lignes canoniques
payloads/           des lignes canoniques aux corps d'écriture de l'API
  regulations.py    regroupement par arrêté, découpage en tranches
  measures.py       mesure, période, emprise, véhicules
dialog/             parler à DiaLog
  client.py         le client HTTP authentifié
  calls.py          un appel par opération (lire, créer, mettre à jour, supprimer, publier)
  failures.py       refus ou panne, et leurs causes
  zone_flow.py      la création des arrêtés à emprise zone
  zone_sections.py  les mesures de tronçons qu'elle utilise
sync/               décider quoi écrire, se souvenir de ce qui a été envoyé
  plan.py           les lots de la nuit et leurs plafonds
  outcome.py        ce qu'une exécution a fait (CLI, Tchap)
  digest.py         l'empreinte d'un arrêté envoyé
  snapshot.py       l'instantané : les empreintes de la nuit précédente
  totals.py         les volumes d'une nuit à l'autre, et la garde de volume
  closure.py        clore un arrêté sorti de la source
  dating.py         dater un arrêté que la source ne date pas
run/                l'exécution d'une organisation, une étape par fichier
  organization.py   ce que déclare un integration.py, et les points d'entrée
  night.py          l'enchaînement des étapes de la nuit
  sources.py        exécuter chaque source, geler celle qui plante ou est en pause
  freeze.py         geler une source dont le volume s'effondre
  writes.py         créer, mettre à jour, clore, supprimer, et compter les échecs
  state.py          ce que la nuit laisse à la suivante : instantané et volumes
  datasets.py       les lignes par jeu de données du message Tchap
report/             ce que lisent les humains
  sync_report.py    le rapport de synchronisation
  notifier.py  summary.py  tchap_bot.py   message Tchap, résumé GitHub
toolkit/            ce qu'une source peut appeler
  connectors/       lire une plateforme (téléchargement, WFS)
  transforms/       heures locales, créneaux, périmètre de compétence
  rules/            à venir : une méthode par règle métier partagée entre sources
integrations/       un dossier par organisation DiaLog, rien d'autre
api/                client de l'API DiaLog, généré (non versionné)
tests/
```

Les dossiers `integrations/{org}/` ne contiennent que du code propre à l'organisation ; le
détail d'une source est dans [Ajouter une source](ajouter-une-source.md).

## Pour aller plus loin

* [Le modèle canonique et les contrôles](modele-canonique.md)
* [Synchronisation : créer, mettre à jour, clore, supprimer](synchronisation.md)
* [Configuration : .env, integrations.toml, CI](configuration.md)
* [Ajouter une source](ajouter-une-source.md)
* [Développement : tests et qualité](developpement.md)
