-- Classification du niveau d'expérience WTTJ vers le vocabulaire commun.
-- Source  : int_wttj_merge_ville
-- Sortie  : une ligne par offre, niveau_experience parmi Junior / Confirmé /
--           Senior / Expert / Non Renseigné.
--
-- Seuils STRICTEMENT identiques à int_france_travail_niveau_experience pour que les
-- deux plateformes soient comparables : <=2 Junior, 3-5 Confirmé, 6-10 Senior,
-- >10 Expert.
--
-- WTTJ expose experience_level_minimum comme un nombre d'années (valeurs observées
-- en test : 0 à 15). Contrairement à France Travail, aucun parsing de texte libre
-- n'est nécessaire.
-- NB : 0 an est mappé vers Junior (et non "Non Renseigné"), cohérent avec le
-- traitement France Travail où "Débutant accepté" tombe aussi dans Junior.

with base as (
    select id, niveau_experience
    from {{ ref('int_wttj_merge_ville') }}
)

select
    id,
    case
        when niveau_experience is null then 'Non Renseigné'
        when niveau_experience <= 2 then 'Junior'
        when niveau_experience between 3 and 5 then 'Confirmé'
        when niveau_experience between 6 and 10 then 'Senior'
        when niveau_experience > 10 then 'Expert'
        else 'Non Renseigné'
    end as niveau_experience
from base
