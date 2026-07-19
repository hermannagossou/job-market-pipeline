-- Table bridge offres ↔ langues (relation many-to-many).
-- Une offre peut demander plusieurs langues ; une langue peut apparaître dans plusieurs offres.
-- Clé composite : id_offre + id_langue

with source as (
    select * from {{ ref('int_offres_langues') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['id', 'nom_plateforme']) }} as id_offre,
    {{ dbt_utils.generate_surrogate_key(['langue']) }} as id_langue
from source
