-- Dimension formations : une ligne par niveau de formation requis (Bac+2 à Bac+5).
-- Clé : id_formation (surrogate key sur niveau_formation)

with source as (
    select * from {{ ref('int_offres') }}
)

select distinct
    {{ dbt_utils.generate_surrogate_key(['niveau_formation']) }} as id_formation,
    niveau_formation as niveau
from source