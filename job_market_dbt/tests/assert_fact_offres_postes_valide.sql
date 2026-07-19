select
    id_offre,
    nbre_postes
from {{ ref('fact_offres') }}
where nbre_postes < 1