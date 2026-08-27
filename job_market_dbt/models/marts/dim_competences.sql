-- Dimension compétences : une ligne par compétence technique extraite par Gemini.
-- Clé : id_competence (surrogate key sur competence)
-- La catégorie est enrichie depuis le seed competences (les valeurs matchent toujours
-- car int_offres_competences valide déjà chaque skill contre ce même seed).

with source as (
    select distinct competence
    from {{ ref('int_offres_competences') }}
),

competences as (
    select skill_name, category
    from {{ ref('competences') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['competence']) }} as id_competence,
    s.competence,
    c.category as categorie
from source as s
left join competences as c
    on s.competence = c.skill_name