-- Point d'union multi-sources pour les compétences.
-- Source  : int_france_travail_offres_competences + int_wttj_offres_competences
-- Sortie  : tous les couples (offre, compétence) toutes plateformes confondues.

with source_france_travail as (
    select * from {{ ref('int_france_travail_offres_competences') }}

),

-- int_wttj_offres_competences est incrémental (appels Gemini) : il garde les offres
-- écartées depuis par int_wttj_offres (ex. offres hors de France). On ne retient que
-- les offres encore présentes, sinon bridge_offres_competences pointerait vers des
-- offres absentes de fact_offres.
source_wttj as (
    select * from {{ ref('int_wttj_offres_competences') }}
    where id in (select id from {{ ref('int_wttj_offres') }})

),

int_offres_combinees as (
    select * from source_france_travail
    union all
    select * from source_wttj
)

select id, competence, nom_plateforme from int_offres_combinees