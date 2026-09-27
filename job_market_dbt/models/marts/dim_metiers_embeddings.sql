-- ============================================================
-- models/marts/dim_metiers_embeddings.sql
-- ============================================================
{{ config(
    materialized='incremental',
    unique_key='id_metier',
    static_analysis='off'
) }}

SELECT *
FROM ML.GENERATE_EMBEDDING(
    MODEL `{{ this.database }}.{{ this.schema }}.embedding_model`,
    (
        SELECT id_metier, nom AS content
        FROM {{ ref('dim_metiers') }}
        {% if is_incremental() %}
        WHERE id_metier NOT IN (SELECT id_metier FROM {{ this }})
        {% endif %}
    )
)
WHERE LENGTH(ml_generate_embedding_status) = 0