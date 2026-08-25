-- Point d'union multi-sources pour les langues.
-- Source  : int_france_travail_offres_langues + int_wttj_offres_langues
-- Sortie  : tous les couples (offre, langue) toutes plateformes confondues.

with source_france_travail as (
    select * from {{ ref('int_france_travail_offres_langues') }}

),

source_wttj as (
    select * from {{ ref('int_wttj_offres_langues') }}

),

int_offres_combinees as (
    select * from source_france_travail
    union all
    select * from source_wttj
)

select id, langue, nom_plateforme from int_offres_combinees
