"""Filtres communs à tous les endpoints du dashboard, injectés via `Depends()`.

Chaque route accepte le même jeu de filtres (région, département, secteur, métier,
type de contrat, niveau d'expérience/formation, période), pour rester cohérent
avec la sidebar de filtres du dashboard Streamlit. Seuls les filtres réellement
fournis par l'appelant génèrent une condition SQL.
"""
from __future__ import annotations

from datetime import date

from fastapi import Query
from google.cloud import bigquery
from pydantic import BaseModel


class FilterParams(BaseModel):
    region: list[str] | None = None
    departement: list[str] | None = None
    secteur: list[str] | None = None
    metier: list[str] | None = None
    type_contrat: list[str] | None = None
    niveau_experience: list[str] | None = None
    niveau_formation: list[str] | None = None
    date_debut: date | None = None
    date_fin: date | None = None


def get_filters(
    region: list[str] | None = Query(None, description="Filtrer par région(s)"),
    departement: list[str] | None = Query(None, description="Filtrer par département(s)"),
    secteur: list[str] | None = Query(None, description="Filtrer par secteur(s) d'activité"),
    metier: list[str] | None = Query(None, description="Filtrer par métier(s)"),
    type_contrat: list[str] | None = Query(None, description="Filtrer par type(s) de contrat"),
    niveau_experience: list[str] | None = Query(None, description="Filtrer par niveau(x) d'expérience"),
    niveau_formation: list[str] | None = Query(None, description="Filtrer par niveau(x) de formation"),
    date_debut: date | None = Query(None, description="Offres publiées à partir de cette date (incluse)"),
    date_fin: date | None = Query(None, description="Offres publiées jusqu'à cette date (incluse)"),
) -> FilterParams:
    return FilterParams(
        region=region,
        departement=departement,
        secteur=secteur,
        metier=metier,
        type_contrat=type_contrat,
        niveau_experience=niveau_experience,
        niveau_formation=niveau_formation,
        date_debut=date_debut,
        date_fin=date_fin,
    )


# Mappe chaque filtre vers (colonne SQL exposée par la vue dénormalisée, type BigQuery).
# Les colonnes référencées ici correspondent aux alias définis dans
# `services/base.py::base_from_clause()`.
_FILTER_COLUMNS: dict[str, tuple[str, str]] = {
    "region": ("loc.region", "STRING"),
    "departement": ("loc.departement", "STRING"),
    "secteur": ("sec.nom", "STRING"),
    "metier": ("met.nom", "STRING"),
    "type_contrat": ("con.contrat", "STRING"),
    "niveau_experience": ("exp.niveau", "STRING"),
    "niveau_formation": ("form.niveau", "STRING"),
}


def build_filter_conditions(
    filters: FilterParams,
) -> tuple[list[str], list[bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter]]:
    """Construit la liste des conditions SQL (non jointes) + les paramètres associés,
    pour ne prendre en compte que les filtres réellement fournis.

    Les appelants peuvent ajouter leurs propres conditions à la liste retournée
    avant de les joindre avec `render_where()` (ex: un `IS NOT NULL` spécifique
    à un endpoint géographique).
    """
    conditions: list[str] = []
    params: list = []

    simple_filters = {
        "region": filters.region,
        "departement": filters.departement,
        "secteur": filters.secteur,
        "metier": filters.metier,
        "type_contrat": filters.type_contrat,
        "niveau_experience": filters.niveau_experience,
        "niveau_formation": filters.niveau_formation,
    }
    for name, values in simple_filters.items():
        if values:
            column, bq_type = _FILTER_COLUMNS[name]
            conditions.append(f"{column} IN UNNEST(@{name})")
            params.append(bigquery.ArrayQueryParameter(name, bq_type, values))

    if filters.date_debut is not None:
        conditions.append("dat.date_publication >= @date_debut")
        params.append(bigquery.ScalarQueryParameter("date_debut", "DATE", filters.date_debut))
    if filters.date_fin is not None:
        conditions.append("dat.date_publication <= @date_fin")
        params.append(bigquery.ScalarQueryParameter("date_fin", "DATE", filters.date_fin))

    return conditions, params


def render_where(conditions: list[str]) -> str:
    """Joint une liste de conditions en clause WHERE, ou chaîne vide si aucune."""
    return f"WHERE {' AND '.join(conditions)}" if conditions else ""
