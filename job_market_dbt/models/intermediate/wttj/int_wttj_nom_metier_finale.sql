-- Fusion du métier : intitulé prioritaire, taxonomie pivot WTTJ en fallback.
-- Source  : int_wttj_nom_metier
-- Sortie  : une ligne par offre, nom_metier final (NULL si aucun des deux ne matche).
--
-- Equivalent de int_france_travail_nom_metier_finale, à ceci près qu'aucun appel LLM
-- n'est nécessaire : WTTJ fournit déjà une taxonomie métier structurée
-- (new_profession.pivot_name), là où France Travail doit passer par Gemini.

with source as (
    select * from {{ ref('int_wttj_nom_metier') }}
)

select
    id,
    coalesce(nom_metier, nom_metier_pivot) as nom_metier
from source
