{{ config(
    materialized='incremental',
    unique_key='id_offre',
    static_analysis='off',
    post_hook="{{ create_vector_index_if_not_exists('offres_index', 'ml_generate_embedding_result') }}"
) }}

with texte_competences_offres as (
    select
        boc.id_offre,
        string_agg(dc.competence, ', ') as texte_competences
    from {{ ref('bridge_offres_competences') }} boc
    join {{ ref('dim_competences') }} dc on dc.id_competence = boc.id_competence
    group by boc.id_offre
)

select *
from ML.GENERATE_EMBEDDING(
    MODEL `{{ this.database }}.{{ this.schema }}.embedding_model`,
    (
        select id_offre, texte_competences as content
        from texte_competences_offres
        {% if is_incremental() %}
        where id_offre not in (select id_offre from {{ this }})
        {% endif %}
    )
)
where length(ml_generate_embedding_status) = 0