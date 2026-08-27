select
    id,
    salaire_min,
    salaire_max
from {{ ref('int_france_travail_offres') }}
where salaire_min <= 0.0
    or salaire_max <= 0.0
    or salaire_min > salaire_max