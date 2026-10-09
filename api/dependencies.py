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
    region: str | None = None
    departement: str | None = None
    secteur: str | None = None
    metier: str | None = None
    type_contrat: str | None = None
    niveau_experience: str | None = None
    niveau_formation: str | None = None
    date_debut: date | None = None
    date_fin: date | None = None


def get_filters(
    region: str | None = Query(None, description="Filtrer par région"),
    departement: str | None = Query(None, description="Filtrer par département"),
    secteur: str | None = Query(None, description="Filtrer par secteur d'activité"),
    metier: str | None = Query(None, description="Filtrer par métier"),
    type_contrat: str | None = Query(None, description="Filtrer par type de contrat"),
    niveau_experience: str | None = Query(None, description="Filtrer par niveau d'expérience"),
    niveau_formation: str | None = Query(None, description="Filtrer par niveau de formation"),
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
) -> tuple[list[str], list[bigquery.ScalarQueryParameter]]:
    """Construit la liste des conditions SQL (non jointes) + les paramètres associés,
    pour ne prendre en compte que les filtres réellement fournis.

    Les appelants peuvent ajouter leurs propres conditions à la liste retournée
    avant de les joindre avec `render_where()` (ex: un `IS NOT NULL` spécifique
    à un endpoint géographique).
    """
    conditions: list[str] = []
    params: list[bigquery.ScalarQueryParameter] = []

    simple_filters = {
        "region": filters.region,
        "departement": filters.departement,
        "secteur": filters.secteur,
        "metier": filters.metier,
        "type_contrat": filters.type_contrat,
        "niveau_experience": filters.niveau_experience,
        "niveau_formation": filters.niveau_formation,
    }
    for name, value in simple_filters.items():
        if value is not None:
            column, bq_type = _FILTER_COLUMNS[name]
            conditions.append(f"{column} = @{name}")
            params.append(bigquery.ScalarQueryParameter(name, bq_type, value))

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
