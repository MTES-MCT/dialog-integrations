# Configuration : .env, integrations.toml, CI

## `.env`

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

## Ce qui tourne chaque nuit : `integrations.toml`

Le registre liste les organisations du workflow `integrate`, leur cible (`prod` ou `staging`) et leurs sources en pause. Le workflow en génère sa matrice (`python3 registry.py matrix`) : ajouter, promouvoir ou mettre en pause une organisation ne touche plus le YAML. L'environnement GitHub et ses secrets restent à créer à la main (`{org}` pour la prod, `{org}-staging` sinon). La politique de synchronisation, elle, reste dans `integration.py`, à côté du code qui fabrique les identifiants.

## Lancer le workflow à la main

Depuis l'onglet Actions, le workflow `integrate` accepte deux options :

* `organization` : une seule organisation (vide : toutes celles de `integrations.toml`) ;
* `force_deletions` : relâcher les clôtures ou suppressions retenues par un plafond ou une chute de volume ; exige une organisation.

Voir [Synchronisation](synchronisation.md#les-plafonds-et-les-lots-retenus) avant de relâcher.
