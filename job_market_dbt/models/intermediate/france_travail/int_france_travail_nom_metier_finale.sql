with all_source_nom_metier as (
    select * from {{ ref('int_france_travail_nom_metier') }}
),

all_source_nom_metier_ai as (
    select * from {{ ref('int_france_travail_ai_nom_metier') }}
)

select
    nm.id,
    coalesce(nm.nom_metier, nma.nom_metier) as nom_metier
from all_source_nom_metier as nm
left join all_source_nom_metier_ai as nma
    on nm.id = nma.id