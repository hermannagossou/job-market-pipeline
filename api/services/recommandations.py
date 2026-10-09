"""Recommandations d'offres pour un client, calculées en direct.

DOIT rester alignée sur models/marts/bridge_offres_clients.sql (version
persistée par dbt) : mêmes filtres durs, même formule, même normalisation.

Filtres durs (6) : formation, expérience, contrat, localisation, salaire
(chevauchement des fourchettes : offre_min <= client_max ET offre_max >= client_min),
métier (correspondance exacte). + exclusion des offres sans aucune compétence
renseignée (leurs scores tomberaient à 0 par construction).

score_final = 0.625 × score_exact + 0.375 × score_embedding
  - score_exact : part des compétences de l'offre que le client possède
  - score_embedding : proximité sémantique compétences client / offre, normalisée
    min-max sur les seules offres éligibles

Dédoublonnage final par contenu (métier + entreprise + salaire) : un recruteur
republie parfois la même annonce sous un nouvel id quelques jours plus tard.
"""
from google.cloud import bigquery

from api.db.bigquery import run_query
from api.schemas.recommandations import OffreRecommandee
from api.services.base import table


def get_recommendations(id_client: str, top_n: int = 10) -> list[OffreRecommandee]:
    sql = f"""
    WITH
    client_profile AS (
        SELECT
            dc.id_client,
            df.rang_formation AS client_rang_formation,
            de.rang_experience AS client_rang_experience,
            dc.id_contrat AS client_id_contrat,
            dc.salaire_min AS client_salaire_min,
            dc.salaire_max AS client_salaire_max
        FROM {table('dim_clients')} dc
        JOIN {table('dim_formations')} df ON df.id_formation = dc.id_formation
        JOIN {table('dim_experiences')} de ON de.id_experience = dc.id_experience
        WHERE dc.id_client = @id_client
    ),
    offres_eligibles AS (
        SELECT
            fo.id_offre, fo.id_metier, fo.id_entreprise, fo.id_localisation,
            fo.salaire_min AS offre_salaire_min, fo.salaire_max AS offre_salaire_max,
            fo.lien_offre
        FROM {table('fact_offres')} fo
        JOIN {table('dim_formations')} df ON df.id_formation = fo.id_formation
        JOIN {table('dim_experiences')} de ON de.id_experience = fo.id_experience
        CROSS JOIN client_profile cp
        WHERE cp.client_rang_formation >= df.rang_formation
          AND cp.client_rang_experience >= de.rang_experience
          AND fo.id_contrat = cp.client_id_contrat
          AND fo.id_localisation IN (
              SELECT id_localisation FROM {table('bridge_clients_localisations')}
              WHERE id_client = @id_client
          )
          AND fo.salaire_min <= cp.client_salaire_max
          AND fo.salaire_max >= cp.client_salaire_min
          AND fo.id_metier IN (
              SELECT id_metier FROM {table('bridge_clients_metiers')}
              WHERE id_client = @id_client
          )
          AND fo.id_offre IN (
              SELECT DISTINCT id_offre FROM {table('bridge_offres_competences')}
          )
    ),
    competences_client AS (
        SELECT id_competence FROM {table('bridge_clients_competences')}
        WHERE id_client = @id_client
    ),
    score_exact_calc AS (
        SELECT
            boc.id_offre,
            COUNT(DISTINCT CASE WHEN cc.id_competence IS NOT NULL THEN boc.id_competence END)
                / NULLIF(COUNT(DISTINCT boc.id_competence), 0) AS score_exact
        FROM {table('bridge_offres_competences')} boc
        LEFT JOIN competences_client cc ON cc.id_competence = boc.id_competence
        WHERE boc.id_offre IN (SELECT id_offre FROM offres_eligibles)
        GROUP BY boc.id_offre
    ),
    texte_competences_client AS (
        SELECT STRING_AGG(dc.competence, ', ') AS content
        FROM competences_client cc
        JOIN {table('dim_competences')} dc ON dc.id_competence = cc.id_competence
    ),
    -- Même top_k que bridge_offres_clients.sql, et normalisation min-max sur les
    -- seules offres éligibles (jointure ci-dessous), pas sur tout le catalogue.
    score_embedding_raw AS (
        SELECT base.id_offre, distance
        FROM VECTOR_SEARCH(
            TABLE {table('offres_embeddings')},
            'ml_generate_embedding_result',
            (
                SELECT ml_generate_embedding_result
                FROM ML.GENERATE_EMBEDDING(
                    MODEL {table('embedding_model')},
                    (SELECT content FROM texte_competences_client)
                )
            ),
            top_k => 5000
        )
        JOIN offres_eligibles oe ON oe.id_offre = base.id_offre
    ),
    score_embedding_calc AS (
        SELECT
            id_offre,
            SAFE_DIVIDE(
                MAX(distance) OVER () - distance,
                NULLIF(MAX(distance) OVER () - MIN(distance) OVER (), 0)
            ) AS score_embedding
        FROM score_embedding_raw
    )
    SELECT
        oe.id_offre,
        dm.nom AS metier,
        dent.nom AS entreprise,
        dl.ville AS ville,
        oe.offre_salaire_min,
        oe.offre_salaire_max,
        oe.lien_offre,
        COALESCE(sec.score_exact, 0) AS score_exact,
        COALESCE(sem.score_embedding, 0) AS score_embedding,
        ROUND(
            0.625 * COALESCE(sec.score_exact, 0)
            + 0.375 * COALESCE(sem.score_embedding, 0)
        , 3) AS score_final
    FROM offres_eligibles oe
    LEFT JOIN score_exact_calc sec ON sec.id_offre = oe.id_offre
    LEFT JOIN score_embedding_calc sem ON sem.id_offre = oe.id_offre
    LEFT JOIN {table('dim_metiers')} dm ON dm.id_metier = oe.id_metier
    LEFT JOIN {table('dim_entreprises')} dent ON dent.id_entreprise = oe.id_entreprise
    LEFT JOIN {table('dim_localisations')} dl ON dl.id_localisation = oe.id_localisation
    -- Salaires en FLOAT64 : BigQuery refuse de partitionner dessus, d'où le CAST
    -- (sans perte, les salaires sont toujours entiers).
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY oe.id_metier, oe.id_entreprise,
            CAST(oe.offre_salaire_min AS INT64), CAST(oe.offre_salaire_max AS INT64)
        ORDER BY score_final DESC
    ) = 1
    ORDER BY score_final DESC
    LIMIT {int(top_n)}
    """
    rows = run_query(sql, [bigquery.ScalarQueryParameter("id_client", "STRING", id_client)])
    return [OffreRecommandee(**row) for row in rows]
