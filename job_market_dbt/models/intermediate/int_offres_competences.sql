-- Point d'union multi-sources pour les compétences.
-- Actuellement : France Travail uniquement. WTTJ sera ajouté via UNION ALL ici.
-- Source  : int_france_travail_offres_competences (et futurs équivalents par plateforme)
-- Sortie  : tous les couples (offre, compétence) toutes plateformes confondues.

with source_france_travail as (
    select * from {{ ref('int_france_travail_offres_competences') }}

),

int_offres_combinees as (
    select * from source_france_travail
)

select id, competence, nom_plateforme from int_offres_combinees