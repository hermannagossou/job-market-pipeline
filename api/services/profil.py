from api.db.bigquery import run_query
from api.dependencies import FilterParams, build_filter_conditions, render_where
from api.schemas.common import RepartitionItem
from api.schemas.profil import RepartitionProfil
from api.services.base import base_from_clause


def get_repartition_profil(filters: FilterParams) -> RepartitionProfil:
    """Formation + expérience recherchées, en un seul aller-retour BigQuery (deux
    sous-requêtes UNION ALL plutôt que deux appels séparés depuis Streamlit)."""
    conditions, params = build_filter_conditions(filters)
    where_sql = render_where(conditions)

    sql = f"""
    SELECT 'formation' AS dimension, form.niveau AS label,
           COUNT(DISTINCT f.id_offre) AS nb_offres
    {base_from_clause()}
    {where_sql}
    GROUP BY form.niveau

    UNION ALL

    SELECT 'experience' AS dimension, exp.niveau AS label,
           COUNT(DISTINCT f.id_offre) AS nb_offres
    {base_from_clause()}
    {where_sql}
    GROUP BY exp.niveau
    """
    # Les paramètres nommés sont utilisés deux fois dans le SQL (une fois par
    # UNION) mais ne doivent être passés qu'une fois au job BigQuery.
    rows = run_query(sql, params)

    formation = [RepartitionItem(label=r["label"], nb_offres=r["nb_offres"]) for r in rows if r["dimension"] == "formation"]
    experience = [RepartitionItem(label=r["label"], nb_offres=r["nb_offres"]) for r in rows if r["dimension"] == "experience"]
    return RepartitionProfil(niveau_formation=formation, niveau_experience=experience)
