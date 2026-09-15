from api.db.bigquery import run_query
from api.dependencies import FilterParams, build_filter_conditions, render_where
from api.schemas.common import EvolutionPoint, RepartitionItem
from api.services.base import base_from_clause


def get_repartition_metiers(filters: FilterParams, limit: int = 20) -> list[RepartitionItem]:
    conditions, params = build_filter_conditions(filters)
    where_sql = render_where(conditions)

    sql = f"""
    SELECT met.nom AS label, COUNT(DISTINCT f.id_offre) AS nb_offres
    {base_from_clause()}
    {where_sql}
    GROUP BY met.nom
    ORDER BY nb_offres DESC
    LIMIT {int(limit)}
    """
    # `limit` est un int Python casté explicitement, jamais une valeur brute
    # concaténée depuis l'utilisateur : pas de risque d'injection ici.
    rows = run_query(sql, params)
    return [RepartitionItem(**row) for row in rows]


def get_evolution_metiers(filters: FilterParams) -> list[EvolutionPoint]:
    """Évolution mensuelle du volume d'offres. Si `filters.metier` est fourni,
    l'évolution ne porte que sur ce métier (réutilise le filtre commun plutôt
    qu'un paramètre dédié)."""
    conditions, params = build_filter_conditions(filters)
    where_sql = render_where(conditions)

    sql = f"""
    SELECT dat.annee AS annee, dat.mois AS mois, COUNT(DISTINCT f.id_offre) AS nb_offres
    {base_from_clause()}
    {where_sql}
    GROUP BY dat.annee, dat.mois
    ORDER BY dat.annee, dat.mois
    """
    rows = run_query(sql, params)
    return [EvolutionPoint(**row) for row in rows]


def get_evolution_offres(filters: FilterParams, granularite: str = "mois") -> list[dict]:
    """Nombre d'offres publiees dans le temps, avec granularite configurable :
    'jour', 'mois' ou 'annee'. Renvoie une date ISO (premier jour de la periode)
    + le nombre d'offres, pret a tracer.

    On tronque la date de publication au grain demande via DATE_TRUNC cote
    BigQuery, ce qui regroupe proprement les offres par periode.
    """
    granularite = granularite if granularite in ("jour", "mois", "annee") else "mois"
    trunc_part = {"jour": "DAY", "mois": "MONTH", "annee": "YEAR"}[granularite]

    conditions, params = build_filter_conditions(filters)
    where_sql = render_where(conditions)

    sql = f"""
    SELECT
        CAST(DATE_TRUNC(dat.date_publication, {trunc_part}) AS STRING) AS periode,
        COUNT(DISTINCT f.id_offre) AS nb_offres
    {base_from_clause()}
    {where_sql}
    GROUP BY periode
    ORDER BY periode
    """
    return run_query(sql, params)
