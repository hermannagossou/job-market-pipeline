-- Extraction des compétences techniques des offres WTTJ depuis les réponses Gemini.
-- Source  : int_wttj_reponses_competences (une réponse brute par offre) + seed competences
-- Sortie  : une ligne par couple (offre, compétence) ; toutes les compétences sont dans le seed.
--
-- N'appelle plus Gemini : l'appel est fait (et mémorisé) par
-- int_wttj_reponses_competences. Ce modèle se contente de découper et valider,
-- et peut donc être recalculé entièrement à chaque run.

{{
    config(
        materialized='table'
    )
}}

with reponses_llm as (
    select id, competences_csv, nom_plateforme
    from {{ ref('int_wttj_reponses_competences') }}
),

-- Split CSV -> une ligne par compétence, nettoyage des espaces.
competences_splittees as (
    select distinct
        id,
        trim(competence) as competence_llm,
        nom_plateforme
    from reponses_llm
    left join unnest(split(competences_csv, ',')) as competence
    where trim(competence) != 'Non Renseigné'
        and trim(competence) != ''
),

-- Validation : inner join sur le seed — filtre dur contre les hallucinations résiduelles.
competences_validees as (
    select
        cs.id,
        c.skill_name as competence,
        cs.nom_plateforme
    from competences_splittees as cs
    inner join {{ ref('competences') }} as c
        on lower(cs.competence_llm) = lower(c.skill_name)
)

select id, competence, nom_plateforme
from competences_validees