-- Point d'union multi-sources pour les langues.
-- Actuellement : France Travail uniquement. WTTJ sera ajouté via UNION ALL ici.
-- Source  : int_france_travail_offres_langues (et futurs équivalents par plateforme)
-- Sortie  : tous les couples (offre, langue) toutes plateformes confondues.

with source_france_travail as (
    select * from {{ ref('int_france_travail_offres_langues') }}

),

int_offres_combinees as (
    select * from source_france_travail
)

select id, langue, nom_plateforme from int_offres_combinees