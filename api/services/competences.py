from typing import Literal

from api.db.bigquery import run_query
from api.dependencies import FilterParams, build_filter_conditions, render_where
from api.schemas.common import EvolutionPoint
from api.schemas.competences import RepartitionCompetence
from api.services.base import base_from_clause, table

GroupBy = Literal["metier", "secteur", "region"]

_GROUP_BY_COLUMN = {
    "metier": "met.nom",
    "secteur": "sec.nom",
    "region": "loc.region",
}


def _bridge_join_clause() -> str:
    return f"""
    JOIN {table('bridge_offres_competences')} boc ON f.id_offre = boc.id_offre
    JOIN {table('dim_competences')} comp ON boc.id_competence = comp.id_competence
    """


def get_repartition_competences(
    filters: FilterParams,
    group_by: GroupBy | None = None,
    limit: int = 20,
) -> list[RepartitionCompetence]:
    """Top compétences demandées, éventuellement ventilées par métier/secteur/région.

    Sans `group_by` : une ligne par compétence (top N global sur les filtres actifs).
    Avec `group_by` : une ligne par (compétence, valeur de la dimension choisie) —
    utile pour "compétences par métier" ou "compétences par secteur" sans créer
    un endpoint dédié pour chacun.
    """
    conditions, params = build_filter_conditions(filters)
    where_sql = render_where(conditions)

    if group_by is None:
        sql = f"""
        SELECT comp.competence AS competence, comp.categorie AS categorie,
               COUNT(DISTINCT f.id_offre) AS nb_offres
        {base_from_clause()}
        {_bridge_join_clause()}
        {where_sql}
        GROUP BY comp.competence, comp.categorie
        ORDER BY nb_offres DESC
        LIMIT {int(limit)}
        """
    else:
        group_column = _GROUP_BY_COLUMN[group_by]
        sql = f"""
        SELECT comp.competence AS competence, comp.categorie AS categorie,
               {group_column} AS categorie_ventilation,
               COUNT(DISTINCT f.id_offre) AS nb_offres
        {base_from_clause()}
        {_bridge_join_clause()}
        {where_sql}
        GROUP BY comp.competence, comp.categorie, {group_column}
        ORDER BY nb_offres DESC
        LIMIT {int(limit)}
        """
    rows = run_query(sql, params)
    return [RepartitionCompetence(**row) for row in rows]


def get_evolution_competence(filters: FilterParams, competence: str) -> list[EvolutionPoint]:
    """Évolution mensuelle de la demande pour une compétence précise."""
    from google.cloud import bigquery

    conditions, params = build_filter_conditions(filters)
    conditions.append("comp.competence = @competence")
    params.append(bigquery.ScalarQueryParameter("competence", "STRING", competence))
    where_sql = render_where(conditions)

    sql = f"""
    SELECT dat.annee AS annee, dat.mois AS mois, COUNT(DISTINCT f.id_offre) AS nb_offres
    {base_from_clause()}
    {_bridge_join_clause()}
    {where_sql}
    GROUP BY dat.annee, dat.mois
    ORDER BY dat.annee, dat.mois
    """
    rows = run_query(sql, params)
    return [EvolutionPoint(**row) for row in rows]
