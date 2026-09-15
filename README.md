# Job Market — API + Dashboard

Couche applicative (API FastAPI + dashboard Streamlit) au-dessus d'un pipeline
de données dbt/BigQuery analysant le marché de l'emploi Data en France
(sources : France Travail et Welcome to the Jungle).

```
Sources → GCS → dbt (BigQuery) → marts → API (FastAPI) → Dashboard (Streamlit)
```

## Structure

```
.
├── api/                    # API FastAPI (couche entre le dashboard et BigQuery)
├── streamlit/              # Dashboard Streamlit (5 espaces + cartes)
├── Dockerfile.api
├── Dockerfile.streamlit
├── docker-compose.yml
├── requirements-api.txt
├── requirements-streamlit.txt
├── .env.example
├── README-api.md           # détail de l'API
└── README-streamlit.md      # détail du dashboard
```

## Lancement rapide avec Docker (recommandé)

### 1. Configuration

```bash
cp .env.example .env
```

Éditez `.env` et renseignez au minimum :

```
BQ_PROJECT_ID=votre-projet-gcp
BQ_DATASET=votre_dataset
```

### 2. Credentials BigQuery

Placez le fichier JSON de votre service account à la racine, nommé
`credentials.json`. Il est monté en lecture seule dans le conteneur API et
n'est jamais copié dans l'image (voir `.dockerignore`).

> Si vous préférez réutiliser vos identifiants locaux (`gcloud auth
> application-default login`) plutôt qu'un service account, voir l'option B
> commentée dans `docker-compose.yml`.

### 3. Démarrage

```bash
docker compose up --build
```

- API : http://localhost:8000 (documentation interactive : http://localhost:8000/docs)
- Dashboard : http://localhost:8501

Streamlit attend que l'API soit « healthy » avant de démarrer (healthcheck
défini dans `docker-compose.yml`), et la joint via le réseau Docker interne
(`http://api:8000`).

Pour arrêter : `Ctrl+C`, puis `docker compose down`.

## Lancement en local sans Docker

Voir `README-api.md` et `README-streamlit.md` pour le détail (venv, variables
d'environnement, `uvicorn` / `streamlit run`).

## Architecture

- **Streamlit ne parle jamais directement à BigQuery** : toutes les données
  transitent par l'API. C'est l'objectif pédagogique du projet et cela isole la
  logique d'accès aux données dans une seule brique testable et réutilisable.
- **Agrégations côté BigQuery** : l'API ne rapatrie jamais des milliers de
  lignes pour les agréger en mémoire — chaque endpoint délègue le `GROUP BY` /
  les calculs à BigQuery et ne renvoie que le résultat agrégé.
- **Cache côté Streamlit** (`st.cache_data`, TTL 5 min) pour éviter de
  resolliciter l'API à chaque interaction, cohérent avec une ingestion
  quotidienne des données.
