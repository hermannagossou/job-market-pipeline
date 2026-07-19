-- Table bridge offres ↔ compétences (relation many-to-many).
-- Une offre peut exiger plusieurs compétences ; une compétence peut apparaître dans plusieurs offres.
-- Clé composite : id_offre + id_competence

with source as (
    select * from {{ ref('int_offres_competences') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['id', 'nom_plateforme']) }} as id_offre,
    {{ dbt_utils.generate_surrogate_key(['competence']) }} as id_competence
from source
