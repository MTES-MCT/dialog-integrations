# Le modèle canonique et les contrôles

## Le modèle

Le modèle canonique est le `TypedDict` `RegulationMeasure` de
`canonical/regulation_measure.py`. Une ligne décrit **une mesure sur une emprise** : une
limitation à 30 km/h sur un tronçon, une interdiction aux poids lourds sur une rue. Toute
source, quel que soit son format d'origine, doit finir par produire ces colonnes.

Les colonnes se rangent en cinq familles, par leur préfixe :

| Famille | Exemples |
|---|---|
| `regulation_` | identifiant, catégorie, objet, titre de l'arrêté |
| `measure_` | type de mesure, vitesse, clé de regroupement |
| `period_` | dates, récurrence, créneaux horaires |
| `location_` | type d'emprise, géométrie, voie, points de début et de fin |
| `vehicle_` | véhicules concernés ou exemptés, gabarits |

En fin de transformation, `canonical/source.py` ne garde que les colonnes du modèle. Une
colonne nommée comme un champ du modèle (`location_…`, `vehicle_…`) sans y figurer est
supprimée, mais signalée en erreur dans le journal : une faute de frappe, ou un champ que
le modèle ne porte pas encore. Une colonne de travail d'une source commence par `_`
(`_measure_kind`) : elle n'est jamais confondue avec le modèle.

L'équipe l'appelle aussi le « pivot » : c'est le même objet.

## Les contrôles communs

Après la transformation de la source et avant la construction des corps d'écriture, chaque
ligne passe les contrôles de `checks/rows.py` :

* fin avant début ;
* limitation de vitesse sans vitesse ;
* emprise `rawGeoJSON` sans géométrie ;
* coordonnées hors bornes ou en ordre (latitude, longitude).

Un arrêté dont une ligne échoue est laissé de côté **en entier**, et laissé tel que DiaLog
le détient : ni créé, ni mis à jour, ni clos. Il compte parmi les « écartés par la
pipeline » de Tchap ; le motif est dans le journal de l'exécution.

Un arrêté dont une mesure n'a pas pu être construite suit le même chemin, toutes ses
tranches (`-01`, `-02`…) comprises.

## Les règles métier

Une règle (`toolkit/rules/rule.py`) a un code, un libellé, une **nature**, ce qui la
fait s'appliquer :

* **hors périmètre** : la donnée est juste, mais pas à publier ;
* **limite de DiaLog** : DiaLog ne sait pas encore l'exprimer ;
* **défaut de la donnée** : la donnée est cassée chez le producteur ;
* **à vérifier** : plausible, mais à regarder ;

et une **conséquence**, ce que deviennent ses lignes :

* **écarté** : la ligne est retirée ;
* **signalé** : journalisé, rien n'est retiré.

Les deux se choisissent séparément : un défaut de la donnée est écarté, ou seulement
signalé s'il ne rend pas la mesure fausse.

Elle s'écrit une fois, en expression Polars sur les noms canoniques. Une source déclare
dans `rules` les règles de `toolkit/rules/` qu'elle applique, chacune à la place de son
propre code : ajouter une règle à la banque ne change aucune autre source.

Une source qui ne regroupe pas n'a rien d'autre à écrire : la classe de base applique ses
règles à ses lignes canoniques. Une source qui regroupe par numéro d'arrêté les applique
avant, par `self.apply_rules(df)`, puisque retirer une ligne après fausserait la règle des
100 % ; elle déclare comment ses propres colonnes se lisent en noms canoniques
(`rule_inputs`). Chaque règle s'applique à tout le jeu, et une ligne qui relève de deux
règles compte pour les deux.

Le rapport de synchronisation donne, par source, le nombre de lignes de chaque règle ; le
journal, quelques identifiants.
