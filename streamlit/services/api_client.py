"""Client HTTP vers l'API FastAPI.

Toute communication entre Streamlit et les données passe par ce module :
Streamlit n'accède jamais directement à BigQuery ni aux modèles dbt.

Chaque fonction est décorée par `st.cache_data` pour éviter de refaire un
appel HTTP identique à chaque interaction utilisateur (changement d'onglet,
re-render...). Le TTL de 5 minutes est un compromis raisonnable pour des
données mises à jour au maximum une fois par jour (ingestion quotidienne).
"""
from __future__ import annotations

import requests
import streamlit as st

from config import API_BASE_URL, REQUEST_TIMEOUT


class ApiError(Exception):
    """Levée quand l'API ne répond pas, met trop de temps, ou renvoie une erreur HTTP."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.status_code = status_code


def _get(path: str, params: dict | None = None):
    """Appel GET générique. Nettoie les paramètres None/vides avant l'envoi
    (évite de transmettre `region=` vide, que FastAPI interpréterait comme un
    filtre explicite sur une chaîne vide plutôt que "pas de filtre")."""
    clean_params = {k: v for k, v in (params or {}).items() if v not in (None, "")}
    try:
        response = requests.get(f"{API_BASE_URL}{path}", params=clean_params, timeout=REQUEST_TIMEOUT)
    except requests.exceptions.Timeout as exc:
        raise ApiError("L'API met trop de temps à répondre. Réessayez dans quelques instants.") from exc
    except requests.exceptions.ConnectionError as exc:
        raise ApiError(
            f"Impossible de joindre l'API sur {API_BASE_URL}. Vérifiez qu'elle est bien démarrée."
        ) from exc

    if not response.ok:
        raise ApiError(
            f"L'API a renvoyé une erreur ({response.status_code}) pour {path}.",
            status_code=response.status_code,
        )
    return response.json()


def _post(path: str, json_body: dict):
    try:
        response = requests.post(f"{API_BASE_URL}{path}", json=json_body, timeout=REQUEST_TIMEOUT)
    except requests.exceptions.Timeout as exc:
        raise ApiError("L'API met trop de temps à répondre. Réessayez dans quelques instants.") from exc
    except requests.exceptions.ConnectionError as exc:
        raise ApiError(
            f"Impossible de joindre l'API sur {API_BASE_URL}. Vérifiez qu'elle est bien démarrée."
        ) from exc

    if not response.ok:
        raise ApiError(
            f"L'API a renvoyé une erreur ({response.status_code}) pour {path}.",
            status_code=response.status_code,
        )
    return response.json()


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


def post_recommandation_cv(cv_text: str, filters: dict | None = None) -> dict:
    """Appelle le moteur de recommandation CV via l'API.

    NON MIS EN CACHE volontairement : un même texte de CV pourrait légitimement
    retourner des résultats différents une fois de nouvelles offres ingérées.

    NOTE : route pas encore implémentée côté API à la date d'écriture — le
    moteur de recommandation est développé séparément par un collègue et n'est
    pas encore intégré. Cet appel échouera (404) tant que la route
    `/api/recommandation/cv` n'existe pas côté API ; c'est attendu, voir
    pages/4_Recommandation.py pour la gestion de ce cas.
    """
    return _post("/api/recommandation/cv", {"cv_text": cv_text, "filters": filters or {}})
