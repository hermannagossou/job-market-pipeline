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
| 2 — Pipeline ETL | `dags/` `job_market_dbt/` | ✅ En prod (Airflow via Astro) |
| 3 — ML | `ml/` | ⏳ À venir |
| 4 — API + Frontend | `api/` `streamlit-app/` | ✅ En prod (recommandation + observatoire) |
| 4 — Docker | `api/Dockerfile` `streamlit-app/Dockerfile` `compose.app.yml` | ✅ |

## Sources de données

- API The Muse
- API Adzuna
- API France Travail
- Scraping Welcome to the Jungle

## Setup à suivre pour utiliser le projet

### 1. Cloner le repo
git clone https://github.com/hermannagossou/job-market-pipeline.git
cd job-market-pipeline

### 2. Créer et activer l'environnement virtuel
python -m venv job-market-venv
source job-market-venv/bin/activate  # Windows : job-market-venv\Scripts\activate

### 3. Installer les dépendances
pip install -r requirements.txt

### 4. Configurer les variables d'environnement
cp .env.example .env
# Ouvrir .env et renseigner les valeurs (clés API, etc.)

### 5. Configurer les credentials GCP
cp credentials.example.json credentials.json
# Remplacer le contenu de credentials.json par ton fichier de service account GCP

> ⚠️ Ne jamais committer .env ni credentials.json — ces fichiers sont dans le .gitignore.

## Lancer l'application (API FastAPI + Streamlit)

Prérequis : Docker, et les étapes 4 et 5 ci-dessus faites.

- `.env` doit contenir au minimum `GCP_PROJECT_ID` et `BIGQUERY_DATASET` (ex. `prod`).
- `credentials.json` doit être la clé d'un compte de service ayant les rôles
  **BigQuery Data Editor**, **BigQuery Job User**, **Storage Object Creator**
  (bucket `job-market-cv-uploads`) et **Vertex AI User**.
- Le dataset doit contenir les tables du pipeline (`dbt build`) et les objets
  créés une fois par `api/sql/setup_bigquery.sql` (déjà fait pour `prod`).

```bash
docker compose -f compose.app.yml up -d --build
docker compose -f compose.app.yml ps      # api : healthy, streamlit : running
```

- App : http://localhost:8501
- API (documentation interactive) : http://127.0.0.1:8000/docs

Arrêt : `docker compose -f compose.app.yml down`.

> ⚠️ Toujours passer `-f compose.app.yml` : sans lui, Docker Compose chercherait un
> `docker-compose.yml` et fusionnerait `docker-compose.override.yml`, qui appartient
> à la pile Airflow (Astro).

La pile Airflow se lance séparément : `astro dev start`.

## Tests de l'API et de l'app

```bash
pip install -r requirements-dev.txt
pytest tests/test_api_recommandation.py tests/test_streamlit_app.py
```

Aucun test n'appelle BigQuery (accès aux données simulés).

## Équipe

Projet réalisé par une équipe de 3 Data Engineers :

- Hermann AGOSSOU
- Clémence FALLON
- Maxime G.