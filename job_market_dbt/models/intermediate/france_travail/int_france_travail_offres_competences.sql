-- Extraction des compétences techniques via Gemini Flash + liste fixe (seed competences).
-- Source  : int_france_travail_offres (offres validées) + stg_france_travail_offres
-- Sortie  : une ligne par couple (offre, compétence) ; toutes les compétences sont dans le seed.
--
-- Optimisations coût vs version sans liste fixe :
--   1. Description tronquée à 2000 chars — les skills apparaissent quasi-systématiquement
--      dans les premiers tiers de l'offre (techno stack, prérequis), pas en fin de texte.
--   2. Liste injectée depuis {{ ref('competences') }} via STRING_AGG : le LLM choisit dans
--      un vocabulaire contraint — plus de normalisation canonique à maintenir manuellement.
--   3. JOIN de validation post-LLM (inner join sur le seed) : filtre dur qui garantit
--      l'absence d'hallucinations sans post-traitement applicatif.
--   4. temperature à 0.1 (vs 0.2) + max_output_tokens à 200 (vs 256) :
--      la tâche de matching est déterministe, on n'a pas besoin de variabilité.
--   5. Incrémental sur (id, competence) : seules les nouvelles offres sont envoyées au LLM.

{{
    config(
        materialized='incremental',
        static_analysis='off',
        unique_key=['id', 'competence']
    )
}}

with offres_validees as (
    select id
    from {{ ref('int_france_travail_offres') }}
),

stg_offres as (
    select id, nom_plateforme, description
    from {{ ref('stg_france_travail_offres') }}
),

offres as (
    select
        o.id,
        s.nom_plateforme,
        s.description
    from offres_validees as o
    left join stg_offres as s on o.id = s.id

    {% if is_incremental() %}
    where o.id not in (select distinct id from {{ this }})
    {% endif %}
),

-- Une seule ligne : liste complète des compétences depuis le seed, ordre alphabétique.
-- La cross join ci-dessous copie cette liste dans chaque prompt sans rescanner la table.
competences_seed as (
    select string_agg(skill_name, ', ' order by skill_name) as liste
    from {{ ref('competences') }}
),

reponses_llm as (
    select distinct
        id,
        result as competences_csv,
        nom_plateforme
    from ai.generate_text(
        model `dbt_hermann.gemini_flash`,
        (
            select
                concat(
                    """
                    Tu es un expert en recrutement data. Extrait les compétences techniques mentionnées dans l'offre ci-dessous.

                    Règle 1 : réponds UNIQUEMENT avec les compétences séparées par des virgules.
                    Règle 2 : chaque compétence doit être choisie EXACTEMENT dans cette liste (respecte la casse) :
                    """,
                    cs.liste,
                    """

                    Règle 3 : si aucune compétence de la liste n'est présente, réponds uniquement : Non Renseigné
                    Règle 4 : pas de texte avant ni après. Pas de ponctuation finale.

                    Offre :
                    """,
                    substr(o.description, 1, 2000)
                ) as prompt,
                o.id,
                o.nom_plateforme
            from offres as o
            cross join competences_seed as cs
        ),
        struct(
            0.1 as temperature,
            200 as max_output_tokens
        )
    )
),

-- Split CSV → une ligne par compétence, nettoyage des espaces.
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
-- Le skill_name du seed fait autorité ; la casse LLM peut varier d'une lettre, le lower() corrige.
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
