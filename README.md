# Job Market Pipeline

Plateforme data end-to-end de centralisation et recommandation d'offres d'emploi.

Construit dans le cadre de la formation **Liora Data Engineer RNCP7** — DataScientest.

## Architecture

Source Systems → Ingestion (Airflow) → Storage (GCS + BigQuery) → Transformation (dbt) → Serving (Power BI ou Streamlit + FastAPI)

Cadre conceptuel : *Fundamentals of Data Engineering* — Joe Reis & Matt Housley (2022)

## État actuel du repo

> La structure est construite de façon itérative, étape par étape.

| Étape | Dossiers | Statut |
|---|---|---|
| 1 — Ingestion APIs | `ingestion/apis/` | ✅ En cours |
| 1 — Scraping | `ingestion/scraping/` | ⏳ À venir |
| 2 — Pipeline ETL | `dags/` `dbt/` | ⏳ À venir |
| 3 — ML | `ml/` | ⏳ À venir |
| 4 — API + Frontend | `api/` `streamlit/` | ⏳ À venir |
| 4 — Docker | `docker/` | ⏳ À venir |

## Sources de données

- API The Muse
- API Adzuna
- API France Travail
- Scraping Welcome to the Jungle

## Setup à suivre pour utiliser le projet

- git clone [https://github.com/hermannagossou/job-market-pipeline.git](https://github.com/hermannagossou/job-market-pipeline.git)
- cd job-market-pipeline
- python -m venv job-market-venv
- source job-market-venv/bin/activate  
- pip install -r requirements.txt

## Équipe

Projet réalisé par une équipe de 3 Data Engineers :

- Hermann AGOSSOU
- Clémence FALLON
- Maxime G.