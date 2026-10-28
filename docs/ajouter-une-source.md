# Ajouter une source

Chaque organisation DiaLog a son dossier dans `integrations/`, à son nom : `co_brest`,
`dp_sarthe`… C'est aussi le nom passé à la CLI (`dialog integrate co_brest`) et celui des
fichiers `.env.{organisation}.{env}`.

```text
integrations/co_maville
├── __init__.py
├── integration.py
├── chantiers_routiers
│   ├── __init__.py
│   ├── schema.py
│   └── data_source_integration.py
├── limitations_vitesse
│   ├── __init__.py
│   ├── schema.py
│   └── data_source_integration.py
```

* `integration.py` : déclare les sources actives de l'organisation, le statut de publication par défaut et la politique de synchronisation (préfixe, mises à jour, clôture ou suppression, plafonds ; voir [Synchronisation](synchronisation.md)).
* `schema.py` : schéma du fichier d'entrée attendu par la source, au format [Pandera](https://pandera.readthedocs.io/).
* `data_source_integration.py` : la source elle-même, une sous-classe de `BaseDataSourceIntegration` (`canonical/source.py`). Elle lit les données (`fetch_raw_data`, avec `toolkit/connectors/` quand la plateforme y est), puis les transforme jusqu'au [modèle canonique](modele-canonique.md) (`compute_clean_data`, avec `toolkit/transforms/` pour les heures, créneaux et le périmètre).

Du code partagé par les sources d'une même organisation reste dans son dossier (exemple :
`integrations/co_lyon/grand_lyon.py`). Pour qu'une organisation tourne chaque nuit,
l'ajouter à `integrations.toml` (voir [Configuration](configuration.md)).
