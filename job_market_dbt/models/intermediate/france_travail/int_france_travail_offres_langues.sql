-- Extraction des langues demandées dans les offres France Travail.
-- Source  : int_france_travail_offres (offres filtrées) + stg_france_travail_offres (champ API
--           langues[]) + int_france_travail_ai_langues (extraction Gemini depuis la description)
-- Sortie  : une ligne par couple (offre, langue).
--
-- Deux sources combinées par union dédupliquée (pas de priorité : une offre peut légitimement
-- exiger plusieurs langues, provenant potentiellement des deux sources à la fois) :
--   1. Champ API langues[] : structuré donc fiable, mais très peu renseigné (~5% des offres).
--   2. Extraction Gemini depuis la description : distingue une langue exigée du candidat d'une
--      mention de nationalité de l'entreprise/des clients/du marché (source principale des
--      faux positifs de l'ancienne regex, notamment sur "français").
-- Les deux sources sont validées contre la même liste de langues de référence (protection
-- contre une valeur API inattendue ou une variante non normalisée renvoyée par le modèle).

{% set langues_canonique = {
    'anglais': 'Anglais',
    'fran[çc]ais': 'Français',
    'espagnol': 'Espagnol',
    'allemand': 'Allemand',
    'italien': 'Italien',
    'portugais': 'Portugais',
    'chinois': 'Chinois',
    'mandarin': 'Mandarin',
    'japonais': 'Japonais',
    'arabe': 'Arabe',
    'n[ée]erlandais': 'Néerlandais',
    'hollandais': 'Hollandais',
    'russe': 'Russe',
    'cor[ée]en': 'Coréen',
    'turc': 'Turc',
    'polonais': 'Polonais',
    'su[ée]dois': 'Suédois',
    'danois': 'Danois',
    'norv[ée]gien': 'Norvégien',
    'finnois': 'Finnois',
    'hindi': 'Hindi'
} %}

with int_france_travail_offres as (
    select id from {{ ref('int_france_travail_offres') }}
),

stg_france_travail_offres as (
    select id, langues, nom_plateforme
    from {{ ref('stg_france_travail_offres') }}
),

ai_langues as (
    select id, langues_ia
    from {{ ref('int_france_travail_ai_langues') }}
),

base as (
    select
        ft.id,
        stg.langues,
        stg.nom_plateforme,
        ai.langues_ia
    from int_france_travail_offres as ft
    left join stg_france_travail_offres as stg on ft.id = stg.id
    left join ai_langues as ai on ft.id = ai.id
),

-- Langues issues du champ API structuré, validées contre la liste de référence.
langues_api_brutes as (
    select
        b.id,
        case
        {% for pattern, nom in langues_canonique.items() %}
            when regexp_contains(lower(trim(json_value(l, '$.libelle'))), r'^{{ pattern }}s?$')
                then '{{ nom }}'
        {% endfor %}
            else null
        end as langue
    from base as b
    left join unnest(json_query_array(b.langues)) as l
),

langues_api as (
    select distinct id, langue
    from langues_api_brutes
    where langue is not null
),

-- Langues issues de l'extraction Gemini, validées contre la même liste de référence.
langues_ia_brutes as (
    select
        b.id,
        case
        {% for pattern, nom in langues_canonique.items() %}
            when regexp_contains(lower(trim(mot)), r'^{{ pattern }}s?$') then '{{ nom }}'
        {% endfor %}
            else null
        end as langue
    from base as b, unnest(split(b.langues_ia, ',')) as mot
),

langues_ia as (
    select distinct id, langue
    from langues_ia_brutes
    where langue is not null
),

-- Union dédupliquée des deux sources.
langues_combinees as (
    select id, langue from langues_api
    union distinct
    select id, langue from langues_ia
)

select
    ft.id,
    coalesce(lc.langue, 'Non Renseigné') as langue,
    stg.nom_plateforme
from int_france_travail_offres as ft
left join stg_france_travail_offres as stg on ft.id = stg.id
left join langues_combinees as lc on ft.id = lc.id
