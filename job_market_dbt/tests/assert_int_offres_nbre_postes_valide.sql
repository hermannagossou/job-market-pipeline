select
    id,
    nbre_postes
from {{ ref('int_offres') }}
where nbre_postes < 1