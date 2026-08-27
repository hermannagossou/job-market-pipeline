-- Dimension métiers : une ligne par intitulé de poste data normalisé (ex. "Data Engineer").
-- Clé : id_metier (surrogate key sur nom_metier)

with source as (
    select * from {{ ref('int_offres') }}
)

select distinct
    {{ dbt_utils.generate_surrogate_key(['nom_metier']) }} as id_metier,
    nom_metier as nom
from source