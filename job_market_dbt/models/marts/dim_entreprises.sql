-- Dimension entreprises : une ligne par nom d'entreprise ayant publié au moins une offre.
-- Clé : id_entreprise (surrogate key sur nom_entreprise)

with source as (
    select * from {{ ref('int_offres') }}
)

select distinct
    {{ dbt_utils.generate_surrogate_key(['nom_entreprise']) }} as id_entreprise,
    nom_entreprise as nom
from source