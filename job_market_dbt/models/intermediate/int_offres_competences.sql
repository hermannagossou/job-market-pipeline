-- Point d'union multi-sources pour les compétences.
-- Source  : int_france_travail_offres_competences + int_wttj_offres_competences
-- Sortie  : tous les couples (offre, compétence) toutes plateformes confondues.

with source_france_travail as (
    select * from {{ ref('int_france_travail_offres_competences') }}

),

source_wttj as (
    select * from {{ ref('int_wttj_offres_competences') }}

),

int_offres_combinees as (
    select * from source_france_travail
    union all
    select * from source_wttj
)

select id, competence, nom_plateforme from int_offres_combinees
