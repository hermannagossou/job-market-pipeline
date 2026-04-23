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
| 1 — Ingestion APIs | `ingestion/apis/france_travail.py` | ✅ Validé |
| 1 — Scraping | `ingestion/scraping/wttj.py` | ⏳ En cours |
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

## Équipe

Projet réalisé par une équipe de 3 Data Engineers :

- Hermann AGOSSOU
- Clémence FALLON
- Maxime GENET