-- Dimension langues : une ligne par langue demandée dans les offres (Anglais, Français…).
-- Clé : id_langue (surrogate key sur langue)

with source as (
    select * from {{ ref('int_offres_langues') }}
)

select distinct
    {{ dbt_utils.generate_surrogate_key(['langue']) }} as id_langue,
    langue
from source