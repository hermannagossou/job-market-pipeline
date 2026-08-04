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


{% docs int_france_travail_ai_type_contrat %}
Classification du **type de contrat** via Gemini Flash (`prod.gemini_model`), depuis la
`description` de l'offre.

- **Incrémental** (`unique_key = id`) : seules les nouvelles offres sont envoyées au LLM.
- **Paramètres** : `temperature = 0.0` (déterministe), `max_output_tokens = 15`.
- Le prompt demande explicitement de ne retenir que le contrat qui régit CE poste précis, en
  ignorant les mentions non pertinentes ailleurs dans le texte (façon dont une expérience
  passée a été acquise, sens non contractuel d'un mot comme "alternance" - rotation d'équipe -,
  encadrement de personnes employées sous un autre type de contrat...).
- Retourne l'une des 6 catégories, ou `"Non Renseigné"`.
{% enddocs %}


{% docs int_france_travail_type_contrat %}
Classification du **type de contrat** requis.

**Stratégie** (priorité décroissante) :
1. Classification Gemini (`int_france_travail_ai_type_contrat`).
2. Fallback sur la normalisation du champ API `type_contrat` (ex. `"CDD - 12 Mois"` → `CDD`,
   `"Profession libérale"` → `Freelance`) si Gemini répond "Non Renseigné". Le champ API ne
   peut de toute façon jamais exprimer "Alternance" ou "Stage" nativement (toujours replié en
   "CDD - X Mois" dans les données observées), donc l'IA reste indispensable pour ces deux
   catégories.

Retourne l'une des 6 catégories, ou `"Non Renseigné"`.
{% enddocs %}


{% docs int_france_travail_ai_niveau_experience %}
Extraction du **nombre d'années d'expérience exigées du candidat** via Gemini Flash
(`prod.gemini_model`), depuis la `description` de l'offre.

- **Incrémental** (`unique_key = id`) : seules les nouvelles offres sont envoyées au LLM.
- **Paramètres** : `temperature = 0.0` (déterministe), `max_output_tokens = 20`.
- Le prompt demande explicitement d'ignorer les mentions d'ancienneté de l'ENTREPRISE ou
  de l'agence (ex. "Fort de 20 ans d'expérience, notre cabinet..."), qu'une regex ne peut
  pas distinguer de manière fiable de l'exigence réellement posée au candidat.
- Retourne un nombre entier brut (non plafonné), ou `"Non Renseigné"` si la description ne
  précise aucune exigence chiffrée.
{% enddocs %}


{% docs int_france_travail_niveau_experience %}
Extraction et classification du **niveau d'expérience** requis.

**Stratégie d'extraction** (priorité décroissante) :
1. Nombre d'années extrait par Gemini (`int_france_travail_ai_niveau_experience`), sans
   plafond : le modèle distingue déjà l'exigence candidat de l'ancienneté d'entreprise.
2. Libellé API (`niveau_experience`) en fallback si Gemini répond "Non Renseigné" ou une
   valeur non numérique.

**Mapping vers 4 classes** :
- `Junior` : débutant accepté ou ≤ 2 ans
- `Confirmé` : 3 à 5 ans
- `Senior` : 6 à 10 ans
- `Expert` : > 10 ans ou "expérience exigée"
{% enddocs %}


{% docs int_france_travail_ai_niveau_formation %}
Classification du **niveau de formation minimum requis** via Gemini Flash (`prod.gemini_model`),
depuis la `description` de l'offre.

- **Incrémental** (`unique_key = id`) : seules les nouvelles offres sont envoyées au LLM.
- **Paramètres** : `temperature = 0.0` (déterministe), `max_output_tokens = 15`.
- Le prompt fournit les équivalences usuelles (BTS/DUT → Bac+2, Licence/BUT/Bachelor → Bac+3,
  Master 1 → Bac+4, Master/école d'ingénieur/grande école/MBA → Bac+5, doctorat/thèse/PhD →
  Doctorat) et demande explicitement d'ignorer les mentions métier homonymes (ex. "Master Data
  Management", "Scrum Master") qu'une regex par mots-clés ne peut pas distinguer de manière
  fiable. Le Doctorat est traité comme un niveau à part, jamais confondu avec Bac+5.
- Retourne l'un des 5 niveaux, ou `"Non Renseigné"`.
{% enddocs %}


{% docs int_france_travail_niveau_formation %}
Extraction et classification du **niveau de formation** requis (Bac+2 à Bac+5, Doctorat).

**Stratégie** (priorité décroissante) :
1. Classification Gemini (`int_france_travail_ai_niveau_formation`).
2. Fallback regex si Gemini répond "Non Renseigné", sur le champ API `formations[].niveauLibelle`
   uniquement (le seul angle mort du modèle, qui ne lit que la `description`) : le Doctorat
   est détecté séparément (mention explicite "doctorat"/"PhD") et prioritaire ; sinon le niveau
   le **plus élevé** parmi Bac+2 à Bac+5 est retenu via `MAX()` sur le rang numérique.

Retourne `"Non Renseigné"` si aucun niveau n'est identifiable par aucune des deux méthodes.
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


{% docs int_france_travail_ai_langues %}
Extraction des **langues exigées du candidat** via Gemini Flash (`prod.gemini_model`), depuis
la `description` de l'offre.

- **Incrémental** (`unique_key = id`) : seules les nouvelles offres sont envoyées au LLM.
- **Paramètres** : `temperature = 0.0` (déterministe), `max_output_tokens = 30` (plusieurs
  langues possibles).
- Le prompt demande explicitement d'ignorer les mentions de langue qui ne décrivent pas une
  compétence attendue du candidat (nationalité de l'entreprise/des clients/du marché, ex.
  "leader français de...") — la principale source de faux positifs de l'ancienne regex,
  notamment sur "français".
- Retourne une liste de langues séparées par une virgule (ex. `"Français, Anglais"`), ou
  `"Non Renseigné"`.
{% enddocs %}


{% docs int_france_travail_offres_langues %}
Extraction des **langues demandées** dans les offres France Travail.

**Deux sources combinées par union dédupliquée** (pas de priorité, une offre peut exiger
plusieurs langues venant des deux sources) :
1. Champ API `langues[]` (structuré, fiable mais peu renseigné : ~5% des offres).
2. Extraction Gemini (`int_france_travail_ai_langues`) depuis la `description`.

Les deux sources sont validées contre la même liste de 21 langues de référence.
{% enddocs %}
