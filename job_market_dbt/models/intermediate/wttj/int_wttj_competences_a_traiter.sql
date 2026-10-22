-- File d'attente de l'extraction des compétences WTTJ par Gemini.
-- Source  : int_wttj_offres (offres validées) + int_wttj_merge_ville (texte)
-- Sortie  : une ligne par offre validée, avec le texte envoyé au modèle
--           (description + profil + missions) et a_traiter.
--
-- Calculé dans une table séparée et non dans int_wttj_reponses_competences : un
-- `id not in {{ this }}` placé dans la requête qui appelle Gemini est appliqué APRÈS
-- l'appel (voir macros/ids_deja_traites.sql).

{{
    config(
        materialized='table',
        static_analysis='off'
    )
}}

with offres_validees as (
    select id
    from {{ ref('int_wttj_offres') }}
),

base as (
    select
        id,
        nom_plateforme,
        concat(
            coalesce(description, ''), ' ',
            coalesce(profile, ''), ' ',
            coalesce(
                (
                    select string_agg(json_value(mission), ' ')
                    from unnest(json_extract_array(missions_cles, '$')) as mission
                ),
                ''
            )
        ) as description
    from {{ ref('int_wttj_merge_ville') }}
)

select
    o.id,
    b.nom_plateforme,
    b.description,
    -- Déjà traitée = réponse de Gemini enregistrée (int_wttj_reponses_competences ne
    -- garde que les réponses non NULL : un appel en erreur est retenté).
    o.id not in {{ ids_deja_traites('int_wttj_reponses_competences') }} as a_traiter
from offres_validees as o
left join base as b on o.id = b.id
