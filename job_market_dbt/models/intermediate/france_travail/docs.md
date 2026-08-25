{% docs int_france_travail_merge_ville %}
Déduplication des offres France Travail et enrichissement géographique. **Une ligne = une offre.**

- **Déduplication** : en cas d'`id` dupliqué, seule la version la plus récente est conservée (`QUALIFY row_number() OVER (PARTITION BY id ORDER BY date_publication DESC) = 1`).
- **Enrichissement** : jointure sur `stg_ville_dept_reg` (seed INSEE) via `code_commune` pour obtenir `nom_ville`, `nom_departement` et `nom_region`.

Modèle de base consommé par tous les modèles de transformation France Travail.
{% enddocs %}


{% docs int_france_travail_nom_metier %}
Extraction du métier data par **regex sur l'intitulé** du poste (liste de 13 métiers définis).

Retourne `NULL` si l'intitulé ne contient aucun métier reconnu. Les offres avec `nom_metier IS NULL`
sont envoyées vers `int_france_travail_ai_nom_metier` pour classification par Gemini.
{% enddocs %}


{% docs int_france_travail_ai_nom_metier %}
Classification du métier data via **Gemini Flash** (`dbt_hermann.gemini_flash`) pour les offres
dont le regex n'a trouvé aucune correspondance (`nom_metier IS NULL`).

- **Incrémental** (`unique_key = id`) : seules les nouvelles offres sont envoyées au LLM.
- **Paramètres** : `temperature = 0.2`, `max_output_tokens = 40`.
- Retourne l'un des 13 métiers définis, ou `"Non Renseigné"` si l'offre n'est pas un métier data.
{% enddocs %}


{% docs int_france_travail_nom_metier_finale %}
Fusion des deux sources de classification du métier : `COALESCE(regex, Gemini)`.

- Si le regex (`int_france_travail_nom_metier`) a trouvé un métier, il est retenu (prioritaire, plus rapide).
- Sinon, le résultat Gemini (`int_france_travail_ai_nom_metier`) est utilisé.

Les offres classifiées `"Non Renseigné"` par Gemini sont filtrées en aval dans `int_france_travail_offres`.
{% enddocs %}


{% docs int_france_travail_ai_nom_entreprise %}
Extraction du **nom d'entreprise** via Gemini Flash pour les offres où `nom_entreprise IS NULL`
(recruteurs anonymes sur France Travail).

- **Incrémental** (`unique_key = id`) : ne retraite pas les offres déjà traitées.
- **Paramètres** : `temperature = 0.0` (déterministe), `max_output_tokens = 30`.
- Retourne le nom extrait de la description, ou `"Non Renseigné"` si non identifiable.
{% enddocs %}


{% docs int_france_travail_ai_nom_secteur %}
Classification du **secteur d'activité** parmi une liste stricte de 15 secteurs, via Gemini Flash.

- **Incrémental** (`unique_key = id`) : ne retraite pas les offres déjà classifiées.
- **Paramètres** : `temperature = 0.0` (déterministe), `max_output_tokens = 50`.
- Secteurs : Banque & Finance, Conseil & ESN, Startup & Tech, Santé & Pharmaceutique, etc.
- Si aucun secteur ne correspond, retourne `"Autre"`.
{% enddocs %}


{% docs int_france_travail_type_contrat %}
Classification du **type de contrat** en deux passes regex.

- **Passe 1** : détection dans la `description` (prioritaire sur le champ API, plus précis pour les alternances et stages).
- **Passe 2** : normalisation du résultat combiné vers les valeurs canoniques : `CDI`, `CDD`, `Stage`, `Alternance`, `Freelance`, `Intérim`, ou `"Non Renseigné"`.
{% enddocs %}


{% docs int_france_travail_niveau_experience %}
Extraction et classification du **niveau d'expérience** requis.

**Stratégie d'extraction** (priorité décroissante) :
1. Regex `"X ans d'expérience"` depuis la `description` (plus précis que le champ API).
2. Libellé API (`niveau_experience`) en fallback.
Les valeurs > 15 ans sont écartées comme anomalies.

**Mapping vers 4 classes** :
- `Junior` : débutant accepté ou ≤ 2 ans
- `Confirmé` : 3 à 5 ans
- `Senior` : 6 à 10 ans
- `Expert` : > 10 ans ou "expérience exigée"
{% enddocs %}


{% docs int_france_travail_niveau_formation %}
Extraction et classification du **niveau de formation** requis (Bac+2 à Bac+5).

**Stratégie** : extraction regex depuis le tableau `formations[]` (champ API) ET depuis la `description` (fallback).
Le niveau le **plus élevé** trouvé est retenu via `MAX()` sur le rang numérique.
Retourne `"Non Renseigné"` si aucun niveau n'est identifiable.
{% enddocs %}


{% docs int_france_travail_salaire %}
Normalisation et imputation des **salaires annuels bruts**. Matérialisé en `TABLE` (coût de calcul élevé).

**Étape 1 — Normalisation** : conversion vers une base annuelle.
- Horaire → `× 35 × 52`
- Mensuel → `× 12` (seuil 10 000 € pour détecter les erreurs de périodicité)

**Étape 2 — Imputation** : les `NULL` sont remplacés par la médiane (`PERCENTILE_CONT 0.5`) calculée en cascade
sur 6 niveaux de partition (du plus précis au plus large) :
`métier + expérience + département + contrat` → région → sans localisation → `métier + contrat` → `contrat` → global.

**Statut du salaire** : `Déclaré` (min et max fournis), `Incomplet` (l'un des deux manquait), `Estimé` (les deux étaient NULL).
{% enddocs %}


{% docs int_france_travail_localisation %}
Résolution de la **localisation** (ville, département, région) pour chaque offre.

**Deux chemins de résolution** :
1. `code_commune` présent → jointure directe sur le seed INSEE (résolution précise).
2. `code_commune` absent → jointure floue sur `nom_departement` (préfixe ou égalité stricte) pour gérer les abréviations du champ API.

La ville retenue est la plus longue entre le libellé API et le libellé INSEE (heuristique de précision).
{% enddocs %}


{% docs int_france_travail_offres %}
**Assemblage final** des offres France Travail : jointure de tous les modèles de transformation spécialisés.

Sources jointes : `nom_metier_finale`, `entreprise` (API + Gemini), `type_contrat`, `niveau_formation`,
`niveau_experience`, `localisation`, `salaire`, `nom_secteur`.

**Deux filtres appliqués en sortie** :
- `regexp_contains(nom_metier, ...)` → conserve uniquement les 13 métiers data reconnus (élimine les `"Non Renseigné"`).
- `departement IS NOT NULL` → élimine les offres sans localisation exploitable.
{% enddocs %}


{% docs int_france_travail_offres_competences %}
Extraction des **compétences techniques** via Gemini Flash à partir d'une liste fixe (seed `competences`).

**Optimisations coût** :
- Description tronquée à 2 000 caractères (les compétences apparaissent dans le premier tiers).
- Liste injectée depuis le seed via `STRING_AGG` : le LLM choisit dans un vocabulaire contraint.
- `INNER JOIN` post-LLM sur le seed : filtre dur contre les hallucinations.
- `temperature = 0.1`, `max_output_tokens = 200`.
- **Incrémental** sur `(id, competence)` : seules les nouvelles offres sont envoyées au LLM.
{% enddocs %}


{% docs int_france_travail_offres_langues %}
Extraction des **langues demandées** dans les offres France Travail.

**Deux sources combinées** (priorité à l'API) :
1. Champ API `langues[]` (tableau structuré) — libellé de la langue.
2. Regex sur la `description` : capte les mentions implicites (`"anglais courant"`, `"maîtrise du français"`).

Valeurs possibles : `Français`, `Anglais`, `Non Renseigné`.
{% enddocs %}
