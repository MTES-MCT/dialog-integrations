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

Elle s'écrit une fois, en expression Polars sur les noms canoniques, comme un
**validateur** : elle dit ce qu'est une ligne valide, et une ligne qu'elle ne peut pas
valider échoue, valeur manquante ou illisible comprise. Une valeur légitimement absente se
dit donc valide dans la règle (une rue nommée n'a pas de géométrie). Ainsi une règle
nourrie d'une mauvaise colonne écarte tout, et la chute de volume se voit, au lieu de ne
rien juger en silence. Les **règles
communes** (`toolkit/rules/common.py`) s'appliquent à toutes les sources sans qu'elles
écrivent rien :

* sur la période, celles des seuls temporaires : arrêté terminé (jour de fin avant
  aujourd'hui, au calendrier de Paris), date de début ou de fin manquante, durée de plus
  d'un an (signalé seulement) ;
* sur l'emprise : emprise ponctuelle hors stationnement, restriction d'un seul sens ;
* sur les véhicules : tonnage au-delà de 44 t, gabarit ou poids lourds sans seuil.

Elles s'appliquent par défaut, et une source s'en écarte explicitement, parce que les deux
oublis ne coûtent pas la même chose : une règle optionnelle oubliée publie une donnée
fausse vers les GPS, sans que rien ne le montre ; une dérogation oubliée écarte des lignes,
comptées au rapport, et la baisse de volume se voit. Une règle n'entre dans
`toolkit/rules/common.py` qu'à trois conditions :

1. elle se lit sur les seules colonnes canoniques ;
2. elle empêche de publier une donnée fausse ou que DiaLog ne sait pas exprimer, pas une
   préférence de qualité ;
3. son effet a été mesuré sur toutes les sources avant l'ajout (lignes écartées, arrêtés
   touchés).

Le reste de `toolkit/rules/` est une boîte à outils : une source s'en sert là où elle en a
besoin.

Une source qui en écarte une le déclare, avec sa raison : `disabled_rules =
{"POINT_NOT_PARKING": "…"}` ; un test refuse une raison vide. Une règle de
`toolkit/rules/` qui n'est pas commune, une source la déclare dans `rules`, à la place de
son propre code. Une règle qui lit une colonne de la source (un libellé, un code de
panneau) est une fonction de la banque, que la source appelle sur sa colonne :
`self.apply_rules(df, one_way_street_named(pl.col("libelle")))`.

Une source qui ne regroupe pas n'a rien d'autre à écrire : la classe de base applique ses
règles à ses lignes canoniques. Une source qui regroupe par numéro d'arrêté les applique
avant, par `self.apply_rules(df)`, puisque retirer une ligne après fausserait la règle des
100 % ; elle déclare comment ses propres colonnes se lisent en noms canoniques
(`rule_inputs`). Chaque règle s'applique à tout le jeu, et une ligne qui relève de deux
règles compte pour les deux.

Le rapport de synchronisation donne, par source, le nombre de lignes de chaque règle ; le
journal, quelques identifiants.

## Le contrat du modèle canonique

Après toute la transformation de la source, la classe de base applique aux lignes
canoniques finales les règles du contrat (`CONTRACT_RULES`, dans
`toolkit/rules/common.py`) :

* fin avant début ;
* limitation de vitesse sans vitesse ;
* emprise `rawGeoJSON` sans géométrie ;
* coordonnées hors bornes ou en ordre (latitude, longitude).

Ce sont des règles comme les autres : nature « défaut de la donnée », lignes écartées,
comptées par règle au rapport. Deux différences : elles passent en dernier, pour qu'aucune
étape de la source ne puisse produire après elles une ligne qui les enfreint, et aucune
source ne peut les désactiver.

Une ligne écartée par une règle n'emporte pas son arrêté : le reste part normalement, on
suit le producteur moins ses lignes invalides. Un arrêté qui n'a plus de ligne est tenu
pour disparu, donc clos ou supprimé.

Un arrêté dont une mesure n'a pas pu être construite, lui, est laissé tel que DiaLog le
détient, toutes ses tranches (`-01`, `-02`…) comprises : ni créé, ni mis à jour, ni clos.
Il compte parmi les « écartés par la pipeline » de Tchap ; le motif est dans le journal
de l'exécution.
