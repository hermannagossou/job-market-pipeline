select
    id_offre,
    salaire_min,
    salaire_max
from {{ ref('fact_offres') }}
where salaire_min <= 0.0
    or salaire_max <= 0.0
    or salaire_min > salaire_max