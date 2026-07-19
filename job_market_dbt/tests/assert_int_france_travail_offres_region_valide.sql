select
    id,
    region
from {{ ref('int_france_travail_offres') }}
where region not in (
    select distinct nom_region from {{ ref('stg_ville_dept_reg') }}
)
