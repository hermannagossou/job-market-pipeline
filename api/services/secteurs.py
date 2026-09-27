from api.db.bigquery import run_query
from api.dependencies import FilterParams, build_filter_conditions, render_where
from api.schemas.common import RepartitionItem
from api.services.base import base_from_clause


def get_repartition_secteurs(filters: FilterParams) -> list[RepartitionItem]:
    conditions, params = build_filter_conditions(filters)
    where_sql = render_where(conditions)

    sql = f"""
    SELECT sec.nom AS label, COUNT(DISTINCT f.id_offre) AS nb_offres
    {base_from_clause()}
    {where_sql}
    GROUP BY sec.nom
    ORDER BY nb_offres DESC
    """
    rows = run_query(sql, params)
    return [RepartitionItem(**row) for row in rows]
