-- File d'attente des enrichissements Gemini au niveau des offres France Travail retenues
-- (après filtrage métier/département), pour les langues et les compétences.
-- Source  : int_france_travail_offres + stg_france_travail_offres (description)
-- Sortie  : une ligne par offre retenue, avec la description et un drapeau par modèle IA.
--
-- Même principe que int_france_travail_ai_a_traiter (voir macros/ids_deja_traites.sql).
-- Compétences : une offre est traitée dès qu'elle a une ligne dans
-- int_france_travail_offres_competences, y compris la ligne 'Non Renseigné' écrite
-- quand Gemini a répondu sans compétence reconnue.

{{
    config(
        materialized='table',
        static_analysis='off'
    )
}}

with offres as (
    select id from {{ ref('int_france_travail_offres') }}
)

select
    o.id,
    stg.nom_plateforme,
    stg.description,
    o.id not in {{ ids_deja_traites('int_france_travail_ai_langues', 'langues_ia is not null') }}
        as a_traiter_langues,
    o.id not in {{ ids_deja_traites('int_france_travail_offres_competences') }}
        as a_traiter_competences
from offres as o
left join {{ ref('stg_france_travail_offres') }} as stg on o.id = stg.id
