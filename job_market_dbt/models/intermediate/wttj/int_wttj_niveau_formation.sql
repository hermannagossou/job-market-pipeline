-- Classification du niveau de formation WTTJ vers le vocabulaire commun.
-- Source  : int_wttj_merge_ville
-- Sortie  : une ligne par offre, niveau_formation parmi Bac+2 / Bac+3 / Bac+4 /
--           Bac+5 / Non Renseigné (vocabulaire partagé avec France Travail).
--
-- WTTJ fournit un enum structuré (education_level), aucun parsing nécessaire.
--
-- LIMITES DE MAPPING (le vocabulaire commun s'arrête à Bac+2 en bas et Bac+5 en haut) :
--   - 'phd'        (doctorat)      -> Bac+5, borne haute du vocabulaire commun
--   - 'bac'        (niveau Bac)    -> Non Renseigné, sous la borne basse
--   - 'cap'        (CAP)           -> Non Renseigné, sous la borne basse
--   - 'no_diploma' (sans diplôme)  -> Non Renseigné, sous la borne basse
-- Les trois derniers cas sont une PERTE D'INFORMATION réelle : ces offres ont un
-- niveau renseigné mais deviennent indistinguables des offres sans information.

with base as (
    select id, niveau_formation
    from {{ ref('int_wttj_merge_ville') }}
)

select
    id,
    case
        when niveau_formation = 'bac_2' then 'Bac+2'
        when niveau_formation = 'bac_3' then 'Bac+3'
        when niveau_formation = 'bac_4' then 'Bac+4'
        when niveau_formation = 'bac_5' then 'Bac+5'
        when niveau_formation = 'phd' then 'Bac+5'
        else 'Non Renseigné'
    end as niveau_formation
from base
