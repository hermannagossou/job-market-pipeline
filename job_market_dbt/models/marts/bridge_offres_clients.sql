-- ============================================================
-- models/marts/bridge_offres_clients.sql
-- ============================================================
-- Persiste les scores de matching (client x offre) pour tous les couples
-- éligibles. Gère le double flux : nouveaux clients (ou profils mis à jour)
-- x toutes les offres, ET clients existants x nouvelles offres.
--
-- Le pre_hook supprime d'abord les anciennes lignes des clients dont le
-- profil a changé depuis le dernier calcul — sans ça, un client qui change
-- de contrat/formation garderait des offres devenues inéligibles.
--
-- dim_clients et les 3 bridges côté client sont créées manuellement dans
-- BigQuery (alimentées par l'app Streamlit, pas par dbt) -> déclarées comme
-- sources (voir _sources_streamlit.yml), jamais via ref().
-- ============================================================
{{ config(
    materialized='incremental',
    unique_key=['id_client', 'id_offre'],
    static_analysis='off',
    pre_hook="
        {% if is_incremental() %}
        DELETE FROM {{ this }}
        WHERE id_client IN (
            SELECT dc.id_client
            FROM {{ source('app_streamlit', 'dim_clients') }} dc
            JOIN (
                SELECT id_client, MAX(date_calcul_matching) AS derniere_maj
                FROM {{ this }}
                GROUP BY id_client
            ) last_match ON last_match.id_client = dc.id_client
            WHERE dc.date_soumission > last_match.derniere_maj
        )
        {% endif %}
    "
) }}

with clients_a_traiter as (
    -- Clients jamais scorés, OU dont le profil a été mis à jour depuis le
    -- dernier calcul (date_soumission plus récente que le dernier
    -- date_calcul_matching connu pour ce client).
    select dc.id_client
    from {{ source('app_streamlit', 'dim_clients') }} dc
    {% if is_incremental() %}
    left join (
        select id_client, max(date_calcul_matching) as derniere_maj
        from {{ this }}
        group by id_client
    ) last_match on last_match.id_client = dc.id_client
    where last_match.id_client is null
       or dc.date_soumission > last_match.derniere_maj
    {% endif %}
),

offres_a_traiter as (
    select id_offre from {{ ref('fact_offres') }}
    {% if is_incremental() %}
    where id_offre not in (select distinct id_offre from {{ this }})
    {% endif %}
),

client_profile as (
    select
        dc.id_client,
        df.rang_formation as client_rang_formation,
        de.rang_experience as client_rang_experience,
        dc.id_contrat as client_id_contrat,
        dc.salaire_min as client_salaire_min,
        dc.salaire_max as client_salaire_max
    from {{ source('app_streamlit', 'dim_clients') }} dc
    join {{ ref('dim_formations') }} df on df.id_formation = dc.id_formation
    join {{ ref('dim_experiences') }} de on de.id_experience = dc.id_experience
),

offre_info as (
    select
        fo.id_offre, fo.id_metier, fo.id_localisation, fo.id_contrat as offre_id_contrat,
        fo.salaire_min as offre_salaire_min, fo.salaire_max as offre_salaire_max,
        df.rang_formation as offre_rang_formation,
        de.rang_experience as offre_rang_experience
    from {{ ref('fact_offres') }} fo
    join {{ ref('dim_formations') }} df on df.id_formation = fo.id_formation
    join {{ ref('dim_experiences') }} de on de.id_experience = fo.id_experience
),

-- FILTRE DUR + double flux (nouveaux/mis-à-jour x tout, existants x nouvelles offres)
paires_eligibles as (
    select cp.id_client, oi.id_offre
    from client_profile cp
    join offre_info oi
        on cp.client_rang_formation >= oi.offre_rang_formation
       and cp.client_rang_experience >= oi.offre_rang_experience
       and cp.client_id_contrat = oi.offre_id_contrat
    where cp.id_client in (select id_client from clients_a_traiter)

    union distinct

    select cp.id_client, oi.id_offre
    from client_profile cp
    join offre_info oi
        on cp.client_rang_formation >= oi.offre_rang_formation
       and cp.client_rang_experience >= oi.offre_rang_experience
       and cp.client_id_contrat = oi.offre_id_contrat
    where oi.id_offre in (select id_offre from offres_a_traiter)
      and cp.id_client not in (select id_client from clients_a_traiter)
),

-- SCORE EXACT
score_exact_calc as (
    select
        pe.id_client, pe.id_offre,
        count(distinct case when cc.id_competence is not null then boc.id_competence end)
            / nullif(count(distinct boc.id_competence), 0) as score_exact
    from paires_eligibles pe
    join {{ ref('bridge_offres_competences') }} boc on boc.id_offre = pe.id_offre
    left join {{ source('app_streamlit', 'bridge_clients_competences') }} cc
        on cc.id_client = pe.id_client and cc.id_competence = boc.id_competence
    group by pe.id_client, pe.id_offre
),

-- SCORE EMBEDDING
clients_texte_competences as (
    select cc.id_client, string_agg(dc.competence, ', ') as content
    from {{ source('app_streamlit', 'bridge_clients_competences') }} cc
    join {{ ref('dim_competences') }} dc on dc.id_competence = cc.id_competence
    where cc.id_client in (select id_client from paires_eligibles)
    group by cc.id_client
),

score_embedding_raw as (
    select query.id_client, base.id_offre, distance
    from VECTOR_SEARCH(
        table {{ ref('offres_embeddings') }}, 'ml_generate_embedding_result',
        (
            select ml_generate_embedding_result, id_client
            from ML.GENERATE_EMBEDDING(
                MODEL `{{ this.database }}.{{ this.schema }}.embedding_model`,
                (select id_client, content from clients_texte_competences)
            )
        ),
        top_k => 5000
    )
    join paires_eligibles pe on pe.id_client = query.id_client and pe.id_offre = base.id_offre
),

score_embedding_calc as (
    select
        id_client, id_offre,
        safe_divide(
            max(distance) over (partition by id_client) - distance,
            nullif(max(distance) over (partition by id_client) - min(distance) over (partition by id_client), 0)
        ) as score_embedding
    from score_embedding_raw
),

-- SCORE PRÉFÉRENCES
localisations_client as (
    select id_client, array_agg(id_localisation) as localisations
    from {{ source('app_streamlit', 'bridge_clients_localisations') }}
    group by id_client
),

metiers_client as (
    select id_client, array_agg(id_metier) as metiers
    from {{ source('app_streamlit', 'bridge_clients_metiers') }}
    group by id_client
),

score_preferences_calc as (
    select
        pe.id_client, pe.id_offre,
        coalesce(
            greatest(0,
                least(cp.client_salaire_max, oi.offre_salaire_max)
                - greatest(cp.client_salaire_min, oi.offre_salaire_min)
            ) / nullif(cp.client_salaire_max - cp.client_salaire_min, 0)
        , 0) as score_salaire,
        case when oi.id_localisation in unnest(coalesce(lc.localisations, [])) then 1.0 else 0.0 end as score_localisation,
        case when oi.id_metier in unnest(coalesce(mc.metiers, [])) then 1.0 else 0.3 end as score_metier
    from paires_eligibles pe
    join client_profile cp on cp.id_client = pe.id_client
    join offre_info oi on oi.id_offre = pe.id_offre
    left join localisations_client lc on lc.id_client = pe.id_client
    left join metiers_client mc on mc.id_client = pe.id_client
)

-- ASSEMBLAGE FINAL
select
    pe.id_client,
    pe.id_offre,
    coalesce(sec.score_exact, 0) as score_exact,
    coalesce(sem.score_embedding, 0) as score_embedding,
    round((spc.score_salaire + spc.score_localisation + spc.score_metier) / 3, 3) as score_preferences,
    round(
        0.5 * coalesce(sec.score_exact, 0)
        + 0.3 * coalesce(sem.score_embedding, 0)
        + 0.2 * ((spc.score_salaire + spc.score_localisation + spc.score_metier) / 3)
    , 3) as score_final,
    current_date() as date_calcul_matching
from paires_eligibles pe
left join score_exact_calc sec on sec.id_client = pe.id_client and sec.id_offre = pe.id_offre
left join score_embedding_calc sem on sem.id_client = pe.id_client and sem.id_offre = pe.id_offre
join score_preferences_calc spc on spc.id_client = pe.id_client and spc.id_offre = pe.id_offre