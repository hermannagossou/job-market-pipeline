-- ============================================================
-- models/marts/bridge_offres_clients.sql
-- ============================================================
-- Persiste les scores de matching (client x offre) pour tous les couples
-- éligibles. Gère le double flux : nouveaux clients (ou profils mis à jour)
-- x toutes les offres, ET clients existants x nouvelles offres.
--
-- IMPORTANT : cette version est synchronisée avec get_recommendations()
-- dans shared.py (calcul en direct côté Streamlit) — les deux DOIVENT
-- rester alignées, sinon un client verrait des scores différents selon
-- qu'il consulte "Mes recommandations" (direct) ou un futur dashboard basé
-- sur cette table (persistant).
--
-- Filtre dur sur 6 critères : formation, expérience, contrat (exigences
-- objectives de l'offre) + localisation (sans ambiguïté possible), salaire
-- (chevauchement — l'offre est retenue dès que sa fourchette recoupe celle
-- du client, même partiellement ; ancienne version en inclusion stricte,
-- excluait à tort des offres avec un vrai recoupement, cf. diagnostic
-- "Camille Dubois"), métier (passé en filtre dur le 23 août, option "C" —
-- sans repli automatique si 0 résultat). Exclut aussi les offres sans
-- aucune compétence renseignée (score_exact/score_embedding y seraient à 0
-- par construction).
--
-- Métier en filtre dur — historique complet de la décision : filtre dur
-- (trop restrictif, écarté) -> score plat 1.0/0.3 (trop permissif, écarté)
-- -> score gradué par embedding (retenu un temps) -> filtre dur à nouveau,
-- définitivement cette fois. Déclencheur : sur le profil "Hermann"
-- (Analytics Engineer/Data Analyst), la seule offre à Toulouse était un
-- Data Scientist à 35% — jugé plus déroutant que rigoureux une fois montré
-- concrètement. Assumé : peut désormais renvoyer 0 résultat si aucune offre
-- du métier exact n'existe dans la zone/le budget du client.
--
-- score_final = 0.625 × score_exact + 0.375 × score_embedding
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
    -- Filtre en amont les offres sans aucune compétence renseignée : sans
    -- ce filtre, score_exact et score_embedding tomberaient tous les deux à
    -- 0 pour ces offres, faussant le classement.
    select
        fo.id_offre, fo.id_metier, fo.id_localisation, fo.id_contrat as offre_id_contrat,
        fo.salaire_min as offre_salaire_min, fo.salaire_max as offre_salaire_max,
        df.rang_formation as offre_rang_formation,
        de.rang_experience as offre_rang_experience
    from {{ ref('fact_offres') }} fo
    join {{ ref('dim_formations') }} df on df.id_formation = fo.id_formation
    join {{ ref('dim_experiences') }} de on de.id_experience = fo.id_experience
    where fo.id_offre in (select distinct id_offre from {{ ref('bridge_offres_competences') }})
),

-- FILTRE DUR (5 critères, métier exclu volontairement) + double flux
paires_eligibles as (
    select cp.id_client, oi.id_offre, oi.id_metier
    from client_profile cp
    join offre_info oi
        on cp.client_rang_formation >= oi.offre_rang_formation
       and cp.client_rang_experience >= oi.offre_rang_experience
       and cp.client_id_contrat = oi.offre_id_contrat
       and oi.offre_salaire_min <= cp.client_salaire_max
       and oi.offre_salaire_max >= cp.client_salaire_min
    where oi.id_localisation in (select id_localisation from {{ source('app_streamlit', 'bridge_clients_localisations') }} where id_client = cp.id_client)
      and oi.id_metier in (select id_metier from {{ source('app_streamlit', 'bridge_clients_metiers') }} where id_client = cp.id_client)
      and cp.id_client in (select id_client from clients_a_traiter)

    union distinct

    select cp.id_client, oi.id_offre, oi.id_metier
    from client_profile cp
    join offre_info oi
        on cp.client_rang_formation >= oi.offre_rang_formation
       and cp.client_rang_experience >= oi.offre_rang_experience
       and cp.client_id_contrat = oi.offre_id_contrat
       and oi.offre_salaire_min <= cp.client_salaire_max
       and oi.offre_salaire_max >= cp.client_salaire_min
    where oi.id_localisation in (select id_localisation from {{ source('app_streamlit', 'bridge_clients_localisations') }} where id_client = cp.id_client)
      and oi.id_metier in (select id_metier from {{ source('app_streamlit', 'bridge_clients_metiers') }} where id_client = cp.id_client)
      and oi.id_offre in (select id_offre from offres_a_traiter)
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

-- SCORE EMBEDDING (compétences)
clients_texte_competences as (
    select cc.id_client, string_agg(dc.competence, ', ') as content
    from {{ source('app_streamlit', 'bridge_clients_competences') }} cc
    join {{ ref('dim_competences') }} dc on dc.id_competence = cc.id_competence
    where cc.id_client in (select distinct id_client from paires_eligibles)
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
)

-- ASSEMBLAGE FINAL
select
    pe.id_client,
    pe.id_offre,
    coalesce(sec.score_exact, 0) as score_exact,
    coalesce(sem.score_embedding, 0) as score_embedding,
    round(
        0.625 * coalesce(sec.score_exact, 0)
        + 0.375 * coalesce(sem.score_embedding, 0)
    , 3) as score_final,
    current_date() as date_calcul_matching
from paires_eligibles pe
left join score_exact_calc sec on sec.id_client = pe.id_client and sec.id_offre = pe.id_offre
left join score_embedding_calc sem on sem.id_client = pe.id_client and sem.id_offre = pe.id_offre