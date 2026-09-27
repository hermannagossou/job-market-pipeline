from api.db.bigquery import run_query
from api.dependencies import FilterParams, build_filter_conditions, render_where
from api.schemas.kpis import KpiOverview
from api.services.base import base_from_clause


def get_kpi_overview(filters: FilterParams) -> KpiOverview:
    """KPIs globaux (page 1 du dashboard) : volumes, salaires moyens, couverture temporelle.

    `COUNT(DISTINCT f.id_offre)` plutôt que `COUNT(*)` : chaque ligne de fact_offres
    correspond déjà à une offre unique (grain 1 offre x 1 plateforme), mais le
    DISTINCT protège contre toute régression future du grain sans coût notable.
    """
    conditions, params = build_filter_conditions(filters)
    where_sql = render_where(conditions)

    sql = f"""
    SELECT
        COUNT(DISTINCT f.id_offre) AS nb_offres,
        COUNT(DISTINCT ent.nom) AS nb_entreprises,
        COUNT(DISTINCT met.nom) AS nb_metiers,
        COUNT(DISTINCT loc.region) AS nb_regions,
        AVG(f.salaire_min) AS salaire_min_moyen,
        AVG(f.salaire_max) AS salaire_max_moyen,
        COUNTIF(f.statut_salaire = 'Déclaré') AS nb_offres_salaire_declare,
        COUNTIF(f.statut_salaire = 'Estimé') AS nb_offres_salaire_estime,
        CAST(MIN(dat.date_publication) AS STRING) AS date_min,
        CAST(MAX(dat.date_publication) AS STRING) AS date_max
    {base_from_clause()}
    {where_sql}
    """
    rows = run_query(sql, params)
    return KpiOverview(**rows[0])
