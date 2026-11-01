# Projet dbt `job_market_dbt`

Transformation des offres d'emploi brutes (France Travail, Welcome to the Jungle) en modèle en étoile dans BigQuery, avec enrichissement par Gemini et embeddings pour la recommandation.

La documentation du pipeline (couches, enrichissement IA, recommandation, règles d'exploitation) est dans le [README principal](../README.md#le-pipeline).

```bash
dbt build --profiles-dir ../include/dbt   # profil : GCP_PROJECT_ID et BIGQUERY_DATASET dans l'environnement
```

Le détail de chaque modèle est documenté dans les fichiers `docs.md` et `_*.yml` de `models/`.
