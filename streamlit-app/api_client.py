"""Client HTTP vers l'API FastAPI (api/).

Toute communication de l'app avec les données passe par ce module : l'app
n'accède jamais directement à BigQuery, GCS ou Vertex AI, et n'a donc besoin
d'aucun identifiant GCP — seul le conteneur de l'API en détient.

Adresse de l'API lue dans API_BASE_URL : http://api:8000 dans docker compose
(nom du service), http://localhost:8000 par défaut en local.
"""
from __future__ import annotations

import os

import requests
import streamlit as st

API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000").rstrip("/")
TIMEOUT = float(os.getenv("API_TIMEOUT_SECONDS", "10"))
# Analyse de CV (Gemini + 2 embeddings) et recommandations (VECTOR_SEARCH) :
# plusieurs secondes côté BigQuery/Vertex, bien au-delà du délai par défaut.
TIMEOUT_LONG = float(os.getenv("API_TIMEOUT_LONG_SECONDS", "90"))


class ApiError(Exception):
    """L'API est injoignable, trop lente, ou a renvoyé une erreur HTTP."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


def _detail(response: requests.Response) -> str:
    """Message d'erreur lisible depuis la réponse FastAPI : `detail` est une
    chaîne pour nos HTTPException, une liste d'erreurs pour la validation Pydantic."""
    try:
        detail = response.json().get("detail")
    except ValueError:
        return f"erreur {response.status_code}"
    if isinstance(detail, list):
        return " ; ".join(
            f"{'.'.join(str(p) for p in err.get('loc', [])[1:])} : {err.get('msg')}".lstrip(" :")
            for err in detail
        )
    return str(detail or f"erreur {response.status_code}")


def _request(method: str, path: str, timeout: float = TIMEOUT, **kwargs):
    try:
        response = requests.request(method, f"{API_BASE_URL}{path}", timeout=timeout, **kwargs)
    except requests.exceptions.Timeout as exc:
        raise ApiError("L'API met trop de temps à répondre. Réessaie dans quelques instants.") from exc
    except requests.exceptions.ConnectionError as exc:
        raise ApiError(f"Impossible de joindre l'API sur {API_BASE_URL}. Vérifie qu'elle est démarrée.") from exc
    if not response.ok:
        raise ApiError(_detail(response), status_code=response.status_code)
    return response.json()


def _pdf(cv_bytes: bytes, filename: str) -> dict:
    return {"fichier": (filename, cv_bytes, "application/pdf")}


@st.cache_data(ttl="1h", show_spinner=False)
def get_referentiel(nom: str) -> dict[str, str]:
    """{id: libellé}, dans l'ordre renvoyé par l'API (trié par libellé) —
    directement utilisable par les widgets (options=keys, format_func)."""
    return {item["id"]: item["label"] for item in _request("GET", f"/api/referentiels/{nom}")}


def analyser_cv(cv_bytes: bytes, filename: str) -> dict:
    return _request("POST", "/api/cv/analyse", timeout=TIMEOUT_LONG, files=_pdf(cv_bytes, filename))


def find_client_by_email(email: str) -> str | None:
    try:
        return _request("GET", "/api/clients", params={"email": email})["id_client"]
    except ApiError as exc:
        if exc.status_code == 404:
            return None
        raise


def upsert_client(profil: dict) -> dict:
    """Retourne {id_client, est_nouveau}."""
    return _request("PUT", "/api/clients", json=profil)


def upload_cv(id_client: str, cv_bytes: bytes, filename: str) -> str:
    return _request("PUT", f"/api/clients/{id_client}/cv", timeout=TIMEOUT_LONG, files=_pdf(cv_bytes, filename))[
        "cv_storage_path"
    ]


def get_client_profile(id_client: str) -> dict | None:
    try:
        return _request("GET", f"/api/clients/{id_client}")
    except ApiError as exc:
        if exc.status_code == 404:
            return None
        raise


def get_recommendations(id_client: str, top_n: int = 10) -> list[dict]:
    return _request(
        "GET", f"/api/clients/{id_client}/recommandations", timeout=TIMEOUT_LONG, params={"top_n": top_n}
    )
