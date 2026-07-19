-- Dimension contrats : une ligne par type de contrat (CDI, CDD, Alternance, Stage…).
-- Clé : id_contrat (surrogate key sur type_contrat)

with source as (
    select * from {{ ref('int_offres') }}
)

select distinct
    {{ dbt_utils.generate_surrogate_key(['type_contrat']) }} as id_contrat,
    type_contrat as contrat
from source