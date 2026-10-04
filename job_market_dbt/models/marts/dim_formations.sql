-- ============================================================
-- dim_formations.sql
-- ============================================================
-- Dimension formations : une ligne par niveau de formation requis (Bac+2 à Bac+5).
-- Clé : id_formation (surrogate key sur niveau_formation)
-- rang_formation : niveau numérique (0 à 5) pour permettre les comparaisons dans le
-- filtre dur du matching (client.rang_formation >= offre.rang_formation) — voir
-- seeds/mapping_rang_formation.csv pour le mapping texte -> rang.

with source as (
    select * from {{ ref('int_offres_niveau_experience_titre') }}
),

distinct_niveaux as (
    select distinct
        {{ dbt_utils.generate_surrogate_key(['niveau_formation']) }} as id_formation,
        niveau_formation as niveau
    from source
)

select
    d.id_formation,
    d.niveau,
    r.rang_formation
from distinct_niveaux d
left join {{ ref('mapping_rang_formation') }} as r
    on d.niveau = r.niveau