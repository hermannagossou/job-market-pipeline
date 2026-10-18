-- Réponses brutes de Gemini pour l'extraction des compétences WTTJ.
-- Source  : int_wttj_offres (offres validées) + int_wttj_merge_ville + seed competences
-- Sortie  : UNE LIGNE PAR OFFRE TRAITÉE, avec la réponse brute (liste CSV).
--
-- Séparé de l'extraction (int_wttj_offres_competences) pour que le filtre
-- incrémental porte sur les offres déjà ENVOYÉES à Gemini, et non sur les offres
-- ayant au moins une compétence retenue : une offre sans compétence reconnue
-- (réponse « Non Renseigné », ou compétences absentes du seed) n'avait aucune ligne
-- et repartait chez Gemini à chaque run (55 offres sur 397).
--
-- Miroir strict de int_france_travail_offres_competences : même prompt, mêmes
-- paramètres. Seule différence : le texte envoyé concatène `description` et
-- `profile`, WTTJ répartissant le contenu utile entre les deux (le profil
-- recherché porte l'essentiel des compétences).

{{
    config(
        materialized='incremental',
        static_analysis='off',
        unique_key='id'
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
        model `prod.gemini_model`,
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
)

-- Appels en erreur (quota, délai) : result NULL, non enregistrés pour être
-- retentés au run suivant.
select id, nom_plateforme, competences_csv
from reponses_llm
where competences_csv is not null
