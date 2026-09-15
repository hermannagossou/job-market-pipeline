from api.db.bigquery import run_query
from api.dependencies import FilterParams, build_filter_conditions, render_where
from api.schemas.common import RepartitionItem
from api.services.base import base_from_clause


def get_repartition_regions(filters: FilterParams) -> list[RepartitionItem]:
    conditions, params = build_filter_conditions(filters)
    conditions.append("loc.region IS NOT NULL")
    where_sql = render_where(conditions)

    sql = f"""
    SELECT loc.region AS label, COUNT(DISTINCT f.id_offre) AS nb_offres
    {base_from_clause()}
    {where_sql}
    GROUP BY loc.region
    ORDER BY nb_offres DESC
    """
    rows = run_query(sql, params)
    return [RepartitionItem(**row) for row in rows]


def get_repartition_departements(filters: FilterParams) -> list[RepartitionItem]:
    """Répartition par département. Si aucune région n'est passée en filtre, la
    requête reste correcte mais retourne potentiellement une centaine de lignes :
    le paramètre `region` est recommandé côté Streamlit pour ce endpoint."""
    conditions, params = build_filter_conditions(filters)
    conditions.append("loc.departement IS NOT NULL")
    where_sql = render_where(conditions)

    sql = f"""
    SELECT loc.departement AS label, COUNT(DISTINCT f.id_offre) AS nb_offres
    {base_from_clause()}
    {where_sql}
    GROUP BY loc.departement
    ORDER BY nb_offres DESC
    """
    rows = run_query(sql, params)
    return [RepartitionItem(**row) for row in rows]


def get_top_entreprises_departement(filters: FilterParams, limit_par_dep: int = 1) -> list[dict]:
    """Pour chaque departement, l'entreprise (ou les N entreprises) qui recrute
    le plus. Renvoie un enregistrement par departement avec l'entreprise en tete
    et son nombre d'offres. Utile pour une carte 'qui recrute ou'.

    Utilise une fenetre (ROW_NUMBER) pour classer les entreprises dans chaque
    departement et ne garder que les premieres, cote BigQuery.
    """
    conditions, params = build_filter_conditions(filters)
    conditions.append("loc.departement IS NOT NULL")
    where_sql = render_where(conditions)

    sql = f"""
    WITH comptes AS (
        SELECT
            loc.departement AS departement,
            ent.nom AS entreprise,
            COUNT(DISTINCT f.id_offre) AS nb_offres,
            ROW_NUMBER() OVER (
                PARTITION BY loc.departement
                ORDER BY COUNT(DISTINCT f.id_offre) DESC, ent.nom
            ) AS rang
        {base_from_clause()}
        {where_sql}
        GROUP BY loc.departement, ent.nom
    )
    SELECT departement AS label, entreprise, nb_offres
    FROM comptes
    WHERE rang <= {int(limit_par_dep)}
    ORDER BY departement, nb_offres DESC
    """
    return run_query(sql, params)
