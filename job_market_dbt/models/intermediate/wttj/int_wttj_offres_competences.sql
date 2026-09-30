-- Extraction des compétences techniques via Gemini Flash + liste fixe (seed competences).
-- Source  : int_wttj_offres (offres validées) + int_wttj_merge_ville
-- Sortie  : une ligne par couple (offre, compétence) ; toutes les compétences sont dans le seed.
--
-- Miroir strict de int_france_travail_offres_competences : même prompt, mêmes
-- paramètres, même validation post-LLM par inner join sur le seed. Seule différence :
-- le texte envoyé concatène `description` et `profile`, WTTJ répartissant le contenu
-- utile entre les deux (le profil recherché porte l'essentiel des compétences).

{{
    config(
        materialized='incremental',
        static_analysis='off',
        unique_key=['id', 'competence']
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
),

offres as (
    select
        o.id,
        b.nom_plateforme,
        b.description
    from offres_validees as o
    left join base as b on o.id = b.id

    {% if is_incremental() %}
    where o.id not in (select distinct id from {{ this }})
    {% endif %}
),

-- Une seule ligne : liste complète des compétences depuis le seed, ordre alphabétique.
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