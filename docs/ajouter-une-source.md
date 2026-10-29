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
* `data_source_integration.py` : la source elle-même, une sous-classe de `BaseDataSourceIntegration` (`canonical/source.py`). Elle lit les données (`fetch_raw_data`, avec `toolkit/connectors/` quand la plateforme y est), puis les transforme jusqu'au [modèle canonique](modele-canonique.md) (`compute_clean_data`, avec `toolkit/transforms/` pour les heures, créneaux, le périmètre, la troncature des intitulés (`titles.truncate`), la période permanente (`periods.permanent_period`), les types de véhicules déduits des seuils (`vehicle_types.restricted_types_from_limits`) et le jumeau 5 km/h de l'aire piétonne (`pedestrian_area.add_pedestrian_area_speed`)).
  * Opendatasoft : `opendatasoft.read_export(URL)` lit l'export d'un jeu et le compare au
    nombre de lignes annoncé par le portail.
  * ArcGIS : `arcgis.read_layer(URL)` lit toutes les pages d'une couche FeatureServer en
    EPSG:4326 ; une couche plus courte qu'annoncé fait échouer la source, qui est gelée.
  * Écarter des lignes : les règles communes s'appliquent d'office ; les autres règles de
    `toolkit/rules/` se déclarent dans `rules`, une règle propre à la source passe par
    `self.apply_rules(df, règle)`. Toutes sont comptées au rapport (voir
    [les règles métier](modele-canonique.md#les-règles-métier)).

Du code partagé par les sources d'une même organisation reste dans son dossier (exemple :
`integrations/co_lyon/grand_lyon.py`). Pour qu'une organisation tourne chaque nuit,
l'ajouter à `integrations.toml` (voir [Configuration](configuration.md)).
