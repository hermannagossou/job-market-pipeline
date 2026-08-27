-- Dimension secteurs : une ligne par secteur d'activité identifié par Gemini.
-- Clé : id_secteur (surrogate key sur nom_secteur)

with source as (
    select * from {{ ref('int_offres') }}
)

select distinct
    {{ dbt_utils.generate_surrogate_key(['nom_secteur']) }} as id_secteur,
    nom_secteur as nom
from source