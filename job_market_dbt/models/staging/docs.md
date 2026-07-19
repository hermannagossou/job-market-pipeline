{% docs src_raw_offres %}
Données brutes d'offres d'emploi dans le domaine de la data, issues de deux plateformes :
l'API officielle France Travail (Pôle Emploi) et l'API interne de Welcome To The Jungle.

Chargement quotidien orchestré par Airflow vers BigQuery (projet `job-market-de-492514`, dataset `prod`).
{% enddocs %}


{% docs src_raw_france_travail_offres %}
Offres d'emploi brutes de l'API France Travail. **Une ligne = une offre d'emploi.**

Particularités de cette source :
- Le salaire est fourni en **texte libre** (ex : `"de 35000 à 45000 Annuel"`) et nécessite une extraction par regex dans la couche staging.
- Les formations et langues sont stockées en **tableau de structs** (ARRAY).
- Le lieu de travail contient ville et code département dans un seul champ texte (`lieuTravail.libelle`).
- Le code commune peut être sur 4 ou 5 caractères (zéro-padding nécessaire pour les DOM-TOM).
{% enddocs %}


{% docs src_raw_wttj_offres %}
Offres d'emploi brutes de l'API Welcome To The Jungle, servies via le moteur de recherche Algolia. **Une ligne = une offre d'emploi.**

Particularités de cette source :
- Le salaire est fourni directement en **valeurs numériques** (`salary_minimum`, `salary_maximum`), contrairement à France Travail.
- Le nombre de postes n'est **pas fourni** par WTTJ ; il est fixé à `1` par défaut dans la couche staging.
- Le métier est identifié via `new_profession.pivot_reference`, la taxonomie interne de WTTJ.
- Les localisations (`offices`) et secteurs (`sectors`) sont des **tableaux** traités en aval.
{% enddocs %}


{% docs stg_france_travail_offres %}
Normalisation des offres brutes France Travail. **Une ligne = une offre d'emploi.**

Transformations appliquées :
- Renommage des colonnes vers un standard snake_case francisé.
- Extraction du salaire min/max par **regex** depuis le libellé texte brut (`salaire.libelle`).
- **Zero-padding** du code commune sur 5 caractères pour les DOM-TOM (codes sur 4 chiffres).
- Cast des types (`dateCreation` → `DATE`, `nombrePostes` → `INT64`).
{% enddocs %}


{% docs stg_wttj_offres %}
Normalisation des offres brutes Welcome To The Jungle (WTTJ). **Une ligne = une offre d'emploi.**

Transformations appliquées :
- Renommage des colonnes vers un standard snake_case francisé.
- Cast du niveau d'expérience en `FLOAT64` (`experience_level_minimum`).
- `nbre_postes` fixé à `1` : ce champ n'est pas fourni par l'API WTTJ.
- Les colonnes `offices` (localisations) et `sectors` (secteurs) restent en tableau (ARRAY) pour traitement dans la couche intermediate.
{% enddocs %}


{% docs stg_ville_dept_reg %}
Table de référence géographique INSEE. **Une ligne = une commune.**

Construite par jointure triple `commune ↔ département ↔ région` à partir des seeds INSEE.
Filtrée sur les types `COM` (communes ordinaires) et `ARM` (arrondissements municipaux de Paris, Lyon et Marseille).
Utilisée dans la couche intermediate pour enrichir les offres avec ville, département et région à partir du code commune.
{% enddocs %}
