from google.cloud import bigquery

from api.db.bigquery import run_query
from api.dependencies import FilterParams, build_filter_conditions, render_where
from api.schemas.offres import OffreDetail, OffresPage
from api.services.base import base_from_clause

_SELECT_COLUMNS = """
    f.id_offre,
    met.nom AS nom_metier,
    ent.nom AS nom_entreprise,
    con.contrat AS type_contrat,
    form.niveau AS niveau_formation,
    exp.niveau AS niveau_experience,
    loc.ville,
    loc.departement,
    loc.region,
    sec.nom AS nom_secteur,
    CAST(dat.date_publication AS STRING) AS date_publication,
    f.nom_plateforme,
    f.salaire_min,
    f.salaire_max,
    f.statut_salaire,
    f.nbre_postes
"""


def get_offres(filters: FilterParams, page: int = 1, page_size: int = 25) -> OffresPage:
    """Liste paginée d'offres individuelles, pour les tableaux détaillés du dashboard.

    Toujours filtrée et paginée côté BigQuery : jamais de récupération intégrale
    de la table côté API pour découper ensuite en pages en mémoire.
    """
    conditions, params = build_filter_conditions(filters)
    where_sql = render_where(conditions)

    count_sql = f"""
    SELECT COUNT(DISTINCT f.id_offre) AS total
    {base_from_clause()}
    {where_sql}
    """
    total = run_query(count_sql, params)[0]["total"]

    offset = (page - 1) * page_size
    paginated_params = params + [
        bigquery.ScalarQueryParameter("limit_", "INT64", page_size),
        bigquery.ScalarQueryParameter("offset_", "INT64", offset),
    ]
    data_sql = f"""
    SELECT {_SELECT_COLUMNS}
    {base_from_clause()}
    {where_sql}
    ORDER BY dat.date_publication DESC, f.id_offre
    LIMIT @limit_ OFFSET @offset_
    """
    rows = run_query(data_sql, paginated_params)

    return OffresPage(
        total=total,
        page=page,
        page_size=page_size,
        resultats=[OffreDetail(**row) for row in rows],
    )
