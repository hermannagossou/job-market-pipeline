from typing import Literal

from api.db.bigquery import run_query
from api.dependencies import FilterParams, build_filter_conditions, render_where
from api.schemas.insights import (
    ComparaisonPlateforme,
    SalaireCompetence,
    SalaireMetier,
    SalaireParDimension,
    TensionMetier,
)
from api.services.base import base_from_clause, table

DimensionComparaison = Literal["metier", "secteur", "contrat", "region"]

# Dimensions sur lesquelles on peut agréger un salaire moyen/médian.
DimensionSalaire = Literal["region", "departement", "secteur"]

_SALAIRE_DIM_COLUMN = {
    "region": "loc.region",
    "departement": "loc.departement",
    "secteur": "sec.nom",
}

_COMPARAISON_COLUMN = {
    "metier": "met.nom",
    "secteur": "sec.nom",
    "contrat": "con.contrat",
    "region": "loc.region",
}


def get_tension_metiers(filters: FilterParams, limit: int = 15) -> list[TensionMetier]:
    """Nombre d'offres rapporté au nombre d'entreprises distinctes par métier.
    Un ratio élevé = beaucoup d'offres concentrées sur peu d'entreprises (marché
    tendu / concurrentiel)."""
    conditions, params = build_filter_conditions(filters)
    where_sql = render_where(conditions)

    sql = f"""
    SELECT
        met.nom AS metier,
        COUNT(DISTINCT f.id_offre) AS nb_offres,
        COUNT(DISTINCT ent.nom) AS nb_entreprises,
        SAFE_DIVIDE(COUNT(DISTINCT f.id_offre), COUNT(DISTINCT ent.nom)) AS offres_par_entreprise
    {base_from_clause()}
    {where_sql}
    GROUP BY met.nom
    HAVING nb_entreprises > 0
    ORDER BY offres_par_entreprise DESC
    LIMIT {int(limit)}
    """
    rows = run_query(sql, params)
    return [TensionMetier(**row) for row in rows]


def get_salaires_metiers(filters: FilterParams, limit: int = 15) -> list[SalaireMetier]:
    """Fourchette salariale par métier. Filtre volontairement sur les salaires
    strictement positifs pour écarter les zéros/nuls, et expose la part de
    salaires déclarés pour que le consommateur sache à quel point la fourchette
    repose sur du réel plutôt que sur de l'imputation médiane (cf. statut_salaire)."""
    conditions, params = build_filter_conditions(filters)
    conditions.append("f.salaire_min > 0")
    where_sql = render_where(conditions)

    sql = f"""
    SELECT
        met.nom AS metier,
        AVG(f.salaire_min) AS salaire_min_moyen,
        AVG(f.salaire_max) AS salaire_max_moyen,
        APPROX_QUANTILES(f.salaire_min, 2)[OFFSET(1)] AS salaire_median_bas,
        APPROX_QUANTILES(f.salaire_max, 2)[OFFSET(1)] AS salaire_median_haut,
        COUNT(DISTINCT f.id_offre) AS nb_offres,
        COUNTIF(f.statut_salaire = 'Déclaré') AS nb_declares,
        SAFE_DIVIDE(COUNTIF(f.statut_salaire = 'Déclaré'), COUNT(*)) AS part_declares
    {base_from_clause()}
    {where_sql}
    GROUP BY met.nom
    ORDER BY salaire_max_moyen DESC
    LIMIT {int(limit)}
    """
    rows = run_query(sql, params)
    return [SalaireMetier(**row) for row in rows]


def get_comparaison_plateformes(
    filters: FilterParams,
    dimension: DimensionComparaison = "metier",
    limit: int = 15,
) -> list[ComparaisonPlateforme]:
    """Répartition d'une dimension ventilée par plateforme source, en colonnes
    (une ligne par valeur, deux colonnes France Travail / WTTJ)."""
    conditions, params = build_filter_conditions(filters)
    where_sql = render_where(conditions)
    column = _COMPARAISON_COLUMN[dimension]

    sql = f"""
    SELECT
        {column} AS label,
        COUNTIF(f.nom_plateforme = 'France Travail') AS france_travail,
        COUNTIF(f.nom_plateforme = 'Welcome To The Jungle') AS wttj
    {base_from_clause()}
    {where_sql}
    GROUP BY {column}
    ORDER BY (COUNTIF(f.nom_plateforme = 'France Travail')
              + COUNTIF(f.nom_plateforme = 'Welcome To The Jungle')) DESC
    LIMIT {int(limit)}
    """
    rows = run_query(sql, params)
    return [ComparaisonPlateforme(**row) for row in rows]


def get_salaires_par_dimension(
    filters: FilterParams,
    dimension: DimensionSalaire = "region",
    limit: int = 30,
) -> list[SalaireParDimension]:
    """Salaire moyen ET median par region / departement / secteur.

    Ne prend en compte que les salaires strictement positifs (ecarte zeros/nuls).
    APPROX_QUANTILES(..., 2)[OFFSET(1)] donne la mediane. On expose la part de
    salaires declares pour signaler la fiabilite (le reste etant impute cote dbt).
    """
    conditions, params = build_filter_conditions(filters)
    conditions.append("f.salaire_min > 0")
    where_sql = render_where(conditions)
    column = _SALAIRE_DIM_COLUMN[dimension]

    sql = f"""
    SELECT
        {column} AS label,
        AVG((f.salaire_min + f.salaire_max) / 2) AS salaire_moyen,
        APPROX_QUANTILES((f.salaire_min + f.salaire_max) / 2, 2)[OFFSET(1)] AS salaire_median,
        COUNT(DISTINCT f.id_offre) AS nb_offres,
        COUNTIF(f.statut_salaire = 'Déclaré') AS nb_declares,
        SAFE_DIVIDE(COUNTIF(f.statut_salaire = 'Déclaré'), COUNT(*)) AS part_declares
    {base_from_clause()}
    {where_sql}
    GROUP BY {column}
    HAVING label IS NOT NULL
    ORDER BY salaire_moyen DESC
    LIMIT {int(limit)}
    """
    rows = run_query(sql, params)
    return [SalaireParDimension(**row) for row in rows]


def get_salaires_competences(filters: FilterParams, limit: int = 20) -> list[SalaireCompetence]:
    """Salaire moyen/median des offres demandant chaque competence.

    Joint fact_offres au bridge competences. Utile cote candidat : quelles
    competences sont associees aux meilleures remunerations.
    """
    conditions, params = build_filter_conditions(filters)
    conditions.append("f.salaire_min > 0")
    where_sql = render_where(conditions)

    sql = f"""
    SELECT
        comp.competence AS competence,
        comp.categorie AS categorie,
        AVG((f.salaire_min + f.salaire_max) / 2) AS salaire_moyen,
        APPROX_QUANTILES((f.salaire_min + f.salaire_max) / 2, 2)[OFFSET(1)] AS salaire_median,
        COUNT(DISTINCT f.id_offre) AS nb_offres
    {base_from_clause()}
    JOIN {table('bridge_offres_competences')} boc ON f.id_offre = boc.id_offre
    JOIN {table('dim_competences')} comp ON boc.id_competence = comp.id_competence
    {where_sql}
    GROUP BY comp.competence, comp.categorie
    HAVING nb_offres >= 3
    ORDER BY salaire_moyen DESC
    LIMIT {int(limit)}
    """
    rows = run_query(sql, params)
    return [SalaireCompetence(**row) for row in rows]
