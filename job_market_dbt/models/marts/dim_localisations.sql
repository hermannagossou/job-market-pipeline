-- Dimension localisations : une ligne par combinaison ville / département / région.
-- Clé : id_localisation (surrogate key sur ville + departement + region)

with source as (
    select * from {{ ref('int_offres') }}
)

select distinct
    {{ dbt_utils.generate_surrogate_key(['ville', 'departement', 'region']) }} as id_localisation,
    ville,
    departement,
    region
from source