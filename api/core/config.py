"""Configuration de l'application, chargée depuis les variables d'environnement.

Aucun secret ni identifiant de projet n'est codé en dur : tout passe par un fichier
`.env` (voir `.env.example`) ou par les variables d'environnement du conteneur.
"""
from functools import lru_cache
from pathlib import Path

from pydantic import ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

# Chemin ABSOLU vers le .env attendu à la racine du projet (job_market_project/.env),
# calculé depuis l'emplacement de ce fichier plutôt que depuis le répertoire courant.
# Sans ça, `env_file=".env"` est résolu relativement au dossier depuis lequel la
# commande est lancée (`uvicorn api.main:app`) : ça marche si vous êtes à la racine
# du projet, mais échoue silencieusement (Settings() sans valeurs) si vous lancez
# la commande depuis un autre dossier, ou depuis un IDE dont le cwd diffère.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_ENV_FILE = _PROJECT_ROOT / ".env"


class Settings(BaseSettings):
    # Projet et dataset BigQuery contenant les modèles marts (fact_offres, dim_*)
    bq_project_id: str
    bq_dataset: str

    # Optionnel : si absent, google-cloud-bigquery utilise les Application Default
    # Credentials (ADC) — pratique en local avec `gcloud auth application-default login`.
    google_application_credentials: str | None = None

    api_title: str = "Job Market API"
    api_version: str = "1.0.0"

    # En développement, autoriser toutes les origines ; à restreindre en production
    # (ex: ["http://localhost:8501"] pour un Streamlit local uniquement).
    cors_allow_origins: list[str] = ["*"]

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        # Le .env est partagé avec le reste du projet (ingestion, Streamlit...) et
        # contient des variables qui ne concernent pas l'API (WTTJ_ALGOLIA_*,
        # GCP_PROJECT_ID, GCS_BUCKET_NAME, STREAMLIT_SERVER_PORT...). "ignore" les
        # tolère au lieu de faire échouer Settings() (comportement par défaut :
        # "forbid", pensé pour un .env dédié à une seule app).
        extra="ignore",
    )


@lru_cache
def get_settings() -> Settings:
    """Instancie les settings une seule fois par process (cache).

    Accepte aussi bien un fichier `.env` à la racine du projet que de vraies
    variables d'environnement déjà positionnées (cas d'un déploiement Docker
    où `.env` n'existe pas sur le système de fichiers de l'image).
    """
    try:
        return Settings()
    except ValidationError as exc:
        raise RuntimeError(
            "Configuration incomplète : BQ_PROJECT_ID et/ou BQ_DATASET manquants.\n"
            f"  - Fichier .env recherché à : {_ENV_FILE} "
            f"({'trouvé' if _ENV_FILE.exists() else 'INTROUVABLE'})\n"
            "  - Sinon, définissez les variables d'environnement directement "
            "(BQ_PROJECT_ID, BQ_DATASET) avant de lancer uvicorn.\n"
            f"Détail : {exc}"
        ) from exc
