-- ============================================================
-- models/marts/dim_competences_embeddings.sql
-- ============================================================
{{ config(
    materialized='incremental',
    unique_key='id_competence',
    static_analysis='off',
    post_hook="{{ create_vector_index_if_not_exists('competences_index', 'ml_generate_embedding_result') }}"
) }}

SELECT *
FROM ML.GENERATE_EMBEDDING(
    MODEL `{{ this.database }}.{{ this.schema }}.embedding_model`,
    (
        SELECT id_competence, competence AS content
        FROM {{ ref('dim_competences') }}
        {% if is_incremental() %}
        WHERE id_competence NOT IN (SELECT id_competence FROM {{ this }})
        {% endif %}
    )
)
WHERE LENGTH(ml_generate_embedding_status) = 0