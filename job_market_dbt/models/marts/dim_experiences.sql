-- ============================================================
-- dim_experiences.sql
-- ============================================================
-- Dimension expériences : une ligne par niveau d'expérience (Junior, Confirmé, Senior, Expert).
-- Clé : id_experience (surrogate key sur niveau_experience)
-- rang_experience : niveau numérique (1 à 4) pour le filtre dur du matching —
-- voir seeds/mapping_rang_experience.csv.

with source as (
    select * from {{ ref('int_offres') }}
),

distinct_niveaux as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['niveau_experience']) }} as id_experience,
        niveau_experience as niveau
    from source
)

select
    d.id_experience,
    d.niveau,
    r.rang_experience
from distinct_niveaux d
left join {{ ref('mapping_rang_experience') }} as r
    on d.niveau = r.niveau