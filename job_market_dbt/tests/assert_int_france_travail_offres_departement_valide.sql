select
    id,
    departement
from {{ ref('int_france_travail_offres') }}
where departement not in (
    select distinct nom_departement from {{ ref('stg_ville_dept_reg') }}
)
