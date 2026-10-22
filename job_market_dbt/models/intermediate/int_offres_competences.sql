-- Point d'union multi-sources pour les compétences.
-- Source  : int_france_travail_offres_competences + int_wttj_offres_competences
-- Sortie  : tous les couples (offre, compétence) toutes plateformes confondues.
-- Seules les offres conservées par int_offres_doublons sont gardées, juste après
-- l'union : les tables de liaison et les dimensions restent ainsi alignées sur
-- fact_offres (ni doublon retiré, ni offre écartée en amont, comme les offres
-- étrangères encore présentes dans les réponses Gemini mémorisées).

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
),

offres_conservees as (
    select id, nom_plateforme from {{ ref('int_offres_doublons') }}
)

select c.id, c.competence, c.nom_plateforme
from int_offres_combinees as c
inner join offres_conservees using (id, nom_plateforme)
-- 'Non Renseigné' : marqueur « Gemini a répondu, aucune compétence reconnue »
-- (voir int_france_travail_offres_competences), pas une compétence.
where c.competence != 'Non Renseigné'
