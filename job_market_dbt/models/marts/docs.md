{% docs fact_offres %}
**Table de faits principale** du modèle dimensionnel. **Une ligne = une offre d'emploi publiée sur une plateforme.**

- **Clé** : `id_offre` — surrogate key générée sur `(id, nom_plateforme)`.
- **Incrémentale** (`unique_key = id_offre`) : seules les nouvelles offres sont insérées à chaque run.
- Toutes les dimensions sont reliées via leurs **surrogate keys** (`id_metier`, `id_entreprise`, `id_contrat`, etc.).
- Les **mesures** directement stockées sont : `salaire_min`, `salaire_max`, `statut_salaire`, `nbre_postes`, `nom_plateforme`.
{% enddocs %}


{% docs dim_dates %}
Dimension temporelle avec **décomposition complète** de chaque date de publication. **Une ligne = une date.**

- **Clé** : `id_date` — surrogate key générée sur `date_publication`.
- Colonnes dérivées : `annee`, `mois`, `trimestre`, `semaine`, `jour_semaine` (numérique),
  `nom_mois` (ex : `"January"`), `nom_jour` (ex : `"Monday"`).
{% enddocs %}


{% docs dim_competences %}
Dimension des compétences techniques extraites par Gemini Flash. **Une ligne = une compétence.**

- **Clé** : `id_competence` — surrogate key générée sur `competence`.
- La `categorie` est enrichie par jointure sur le seed `competences` (ex : `"Langages de programmation"`, `"Cloud & Infrastructure"`).
- La jointure est toujours résolvable : `int_offres_competences` valide déjà chaque compétence contre ce même seed.
{% enddocs %}


{% docs bridge_offres_competences %}
Table bridge pour la **relation many-to-many** entre offres et compétences.
Une offre peut exiger plusieurs compétences ; une compétence peut apparaître dans plusieurs offres.

- **Clé composite** : `(id_offre, id_competence)` — les deux colonnes sont des surrogate keys.
- `id_offre` est cohérent avec `fact_offres` (même surrogate key sur `id + nom_plateforme`).
{% enddocs %}


{% docs bridge_offres_langues %}
Table bridge pour la **relation many-to-many** entre offres et langues demandées.
Une offre peut demander plusieurs langues ; une langue peut apparaître dans plusieurs offres.

- **Clé composite** : `(id_offre, id_langue)` — les deux colonnes sont des surrogate keys.
- `id_offre` est cohérent avec `fact_offres` (même surrogate key sur `id + nom_plateforme`).
{% enddocs %}
