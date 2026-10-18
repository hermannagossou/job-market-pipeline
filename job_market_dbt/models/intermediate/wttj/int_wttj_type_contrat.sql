-- Classification du type de contrat WTTJ vers le vocabulaire commun.
-- Source  : int_wttj_merge_ville
-- Sortie  : une ligne par offre, type_contrat parmi le vocabulaire partagé avec
--           France Travail : CDI / CDD / Intérim / Stage / Alternance / Freelance
--           / Non Renseigné.
--
-- Contrairement à France Travail (texte libre nécessitant une passe regex sur la
-- description), WTTJ fournit un enum propre : un simple mapping suffit.
--
-- LIMITES DE MAPPING (valeurs WTTJ sans équivalent dans le vocabulaire commun) :
--   - 'vie'              (Volontariat International) -> Non Renseigné
--   - 'part_time'        (temps partiel)             -> Non Renseigné
--   - 'graduate_program' (programme jeune diplômé)   -> Non Renseigné
--   - 'other'                                        -> Non Renseigné
-- Ces offres existent bien mais sont indistinguables des vraies valeurs manquantes
-- une fois dans int_offres. A rediscuter si le vocabulaire commun doit s'élargir.
--
-- 'temporary' est mappé vers CDD (et non Intérim) : WTTJ ne distingue pas les deux,
-- CDD étant le cas de loin le plus fréquent sur ce type d'offres.

with base as (
    select id, type_contrat
    from {{ ref('int_wttj_merge_ville') }}
)

select
    id,
    case
        when type_contrat = 'full_time' then 'CDI'
        when type_contrat = 'temporary' then 'CDD'
        when type_contrat = 'internship' then 'Stage'
        when type_contrat = 'apprenticeship' then 'Alternance'
        when type_contrat = 'freelance' then 'Freelance'
        else 'Non Renseigné'
    end as type_contrat
from base
