-- Dimension expériences : une ligne par niveau d'expérience (Junior, Confirmé, Senior, Expert).
-- Clé : id_experience (surrogate key sur niveau_experience)

with source as (
    select * from {{ ref('int_offres') }}
)

select distinct
    {{ dbt_utils.generate_surrogate_key(['niveau_experience']) }} as id_experience,
    niveau_experience as niveau
from source