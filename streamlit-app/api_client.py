"""Client HTTP vers l'API FastAPI (api/), pour les deux parties de l'app :
observatoire (lecture analytique, mise en cache 5 min) et recommandation.

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
# Analyse de CV (Gemini + 2 embeddings), enregistrement du profil (script DML)
# et recommandations (VECTOR_SEARCH) : plusieurs secondes côté BigQuery/Vertex,
# bien au-delà du délai par défaut.
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


def _get(path: str, params: dict | None = None):
    """GET analytique. Retire les filtres None/vides avant l'envoi : un
    `region=` vide serait un filtre explicite sur une chaîne vide pour FastAPI,
    pas "aucun filtre"."""
    clean_params = {k: v for k, v in (params or {}).items() if v not in (None, "")}
    return _request("GET", path, params=clean_params)


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
    """Retourne {id_client, est_nouveau}. Délai long : l'upsert est un script
    BigQuery de 7 instructions (MERGE + 3 × DELETE/INSERT), ~1-2 s chacune."""
    return _request("PUT", "/api/clients", timeout=TIMEOUT_LONG, json=profil)


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


# ---------------------------------------------------------------------------
# Observatoire — lecture analytique. Mise en cache 5 min : données mises à jour
# au plus une fois par jour (ingestion quotidienne), inutile de rappeler l'API
# à chaque changement d'onglet ou de filtre déjà vu.
# ---------------------------------------------------------------------------
@st.cache_data(ttl=300, show_spinner=False)
def get_kpi_overview(filters: dict) -> dict:
    return _get("/api/kpis/overview", filters)


@st.cache_data(ttl=300, show_spinner=False)
def get_regions(filters: dict) -> list[dict]:
    return _get("/api/geo/regions", filters)


@st.cache_data(ttl=300, show_spinner=False)
def get_departements(filters: dict) -> list[dict]:
    return _get("/api/geo/departements", filters)


@st.cache_data(ttl=300, show_spinner=False)
def get_metiers_repartition(filters: dict, limit: int = 20) -> list[dict]:
    return _get("/api/metiers/repartition", {**filters, "limit": limit})


@st.cache_data(ttl=300, show_spinner=False)
def get_metiers_evolution(filters: dict) -> list[dict]:
    return _get("/api/metiers/evolution", filters)


@st.cache_data(ttl=300, show_spinner=False)
def get_secteurs_repartition(filters: dict) -> list[dict]:
    return _get("/api/secteurs/repartition", filters)


@st.cache_data(ttl=300, show_spinner=False)
def get_competences_repartition(filters: dict, group_by: str | None = None, limit: int = 20) -> list[dict]:
    params = {**filters, "limit": limit}
    if group_by:
        params["group_by"] = group_by
    return _get("/api/competences/repartition", params)


@st.cache_data(ttl=300, show_spinner=False)
def get_competences_evolution(filters: dict, competence: str) -> list[dict]:
    return _get("/api/competences/evolution", {**filters, "competence": competence})


@st.cache_data(ttl=300, show_spinner=False)
def get_contrats_repartition(filters: dict) -> list[dict]:
    return _get("/api/contrats/repartition", filters)


@st.cache_data(ttl=300, show_spinner=False)
def get_profil_repartition(filters: dict) -> dict:
    return _get("/api/profil/repartition", filters)


@st.cache_data(ttl=300, show_spinner=False)
def get_offres(filters: dict, page: int = 1, page_size: int = 25) -> dict:
    return _get("/api/offres", {**filters, "page": page, "page_size": page_size})


@st.cache_data(ttl=300, show_spinner=False)
def get_tension_metiers(filters: dict, limit: int = 15) -> list[dict]:
    return _get("/api/insights/tension-metiers", {**filters, "limit": limit})


@st.cache_data(ttl=300, show_spinner=False)
def get_salaires_metiers(filters: dict, limit: int = 15) -> list[dict]:
    return _get("/api/insights/salaires-metiers", {**filters, "limit": limit})


@st.cache_data(ttl=300, show_spinner=False)
def get_comparaison_plateformes(filters: dict, dimension: str = "metier", limit: int = 15) -> list[dict]:
    return _get("/api/insights/comparaison-plateformes", {**filters, "dimension": dimension, "limit": limit})


@st.cache_data(ttl=300, show_spinner=False)
def get_salaires_dimension(filters: dict, dimension: str = "region", limit: int = 30) -> list[dict]:
    return _get("/api/insights/salaires-dimension", {**filters, "dimension": dimension, "limit": limit})


@st.cache_data(ttl=300, show_spinner=False)
def get_salaires_competences(filters: dict, limit: int = 20) -> list[dict]:
    return _get("/api/insights/salaires-competences", {**filters, "limit": limit})


@st.cache_data(ttl=300, show_spinner=False)
def get_top_entreprises_departement(filters: dict, limit_par_dep: int = 1) -> list[dict]:
    return _get("/api/geo/top-entreprises-departement", {**filters, "limit_par_dep": limit_par_dep})


@st.cache_data(ttl=300, show_spinner=False)
def get_evolution_granulaire(filters: dict, granularite: str = "mois") -> list[dict]:
    return _get("/api/metiers/evolution-granulaire", {**filters, "granularite": granularite})
