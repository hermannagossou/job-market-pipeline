select
    id,
    nbre_postes
from {{ ref('int_france_travail_offres') }}
where nbre_postes < 1