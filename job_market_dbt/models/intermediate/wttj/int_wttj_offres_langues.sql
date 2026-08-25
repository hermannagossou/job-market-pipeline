-- Extraction des langues demandées dans les offres WTTJ.
-- Source  : int_wttj_offres (offres filtrées) + int_wttj_merge_ville
-- Sortie  : une ligne par couple (offre, langue).
--
-- Beaucoup plus simple que son équivalent France Travail : WTTJ expose un champ
-- `language` structuré (code ISO type 'fr', 'en'), là où France Travail doit
-- combiner un tableau API et une extraction regex sur la description.
--
-- Le code ISO est converti en libellé français capitalisé pour produire exactement
-- le même vocabulaire que int_france_travail_offres_langues (qui applique initcap
-- sur des libellés déjà en français : "Anglais", "Français"...).

with offres_validees as (
    select id
    from {{ ref('int_wttj_offres') }}
),

base as (
    select id, niveau_langue, nom_plateforme
    from {{ ref('int_wttj_merge_ville') }}
),

offres as (
    select
        o.id,
        b.niveau_langue,
        b.nom_plateforme
    from offres_validees as o
    left join base as b on o.id = b.id
)

select
    id,
    initcap(
        case lower(niveau_langue)
            when 'fr' then 'français'
            when 'en' then 'anglais'
            when 'es' then 'espagnol'
            when 'de' then 'allemand'
            when 'it' then 'italien'
            when 'pt' then 'portugais'
            when 'nl' then 'néerlandais'
            when 'ru' then 'russe'
            when 'zh' then 'chinois'
            when 'ja' then 'japonais'
            when 'ar' then 'arabe'
            else 'Non Renseigné'
        end
    ) as langue,
    nom_plateforme
from offres
