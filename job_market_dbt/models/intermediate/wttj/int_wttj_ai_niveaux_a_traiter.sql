-- Offres WTTJ à envoyer à Gemini pour compléter les niveaux (int_wttj_ai_niveaux).
-- Source  : int_wttj_offres (périmètre data, niveaux structurés) + int_wttj_merge_ville
-- Sortie  : une ligne par offre dont la formation OU l'expérience vaut
--           'Non Renseigné' et qui a une description.
--
-- Matérialisé en TABLE exprès : quand int_wttj_ai_niveaux lisait directement la
-- jointure sur les vues amont, BigQuery évaluait l'appel à Gemini avant les
-- filtres, sur tout le brut WTTJ (≈ 33 000 lignes lues, ≈ 5 000 appels facturés
-- par run même sans offre à enrichir). En ne lisant que cette petite table,
-- l'étape Gemini ne voit plus que les offres réellement concernées.

{{
    config(
        materialized='table'
    )
}}

select
    o.id,
    o.nom_plateforme,
    o.niveau_formation,
    o.niveau_experience,
    d.description
from {{ ref('int_wttj_offres') }} as o
inner join {{ ref('int_wttj_merge_ville') }} as d
    on o.id = d.id
where d.description is not null
    and (o.niveau_formation = 'Non Renseigné' or o.niveau_experience = 'Non Renseigné')
