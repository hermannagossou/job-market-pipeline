-- Extraction et classification du niveau de formation requis (Bac+2 à Bac+5, Doctorat).
-- Source  : int_france_travail_offres_data (champ formations[])
--           + int_france_travail_ai_niveau_formation (classification par Gemini)
-- Sortie  : une ligne par offre avec le niveau de formation le plus élevé trouvé,
--           ou "Non Renseigné" si aucun niveau identifiable.
--
-- Stratégie : priorité à la classification Gemini (comprend les équivalences de diplômes et
-- ignore les mentions métier sans rapport, ex. "Master Data Management"). Fallback sur
-- l'extraction regex du champ API formations[].niveauLibelle si Gemini répond "Non Renseigné".
-- Pas de regex sur `description` en fallback : Gemini lit déjà ce même champ avec une
-- compréhension supérieure (équivalences, filtrage des faux amis) donc un regex dessus
-- n'apporterait aucun signal que Gemini n'aurait pas déjà capté. Le champ formations[]
-- reste, lui, un angle mort du modèle (le prompt ne lui transmet que la description).

{%
    set niveaux_formation = {
        'bac+2': ['bac\\s*\\+\\s*2'],
        'bac+3': ['bac\\s*\\+\\s*3', '\\blicence\\s*3', 'licence', 'bachelor'],
        'bac+4': ['bac\\s*\\+\\s*4', 'master\\s*1', '\\bm\\s*1\\b'],
        'bac+5': ['bac\\s*\\+\\s*5', 'master\\s*2', '\\bm\\s*2\\b', 'master']
    }
%}

with base as (
    select id, formations, description
    from {{ ref('int_france_travail_merge_ville') }}
),

ai_extraction as (
    select id, niveau_formation_ia
    from {{ ref('int_france_travail_ai_niveau_formation') }}
),

-- Unnest formations[] puis extraction de TOUS les niveaux mentionnés dans le libellé API
-- (regexp_extract_all, pas juste le premier : un libellé comme "Bac+3, Bac+4 ou équivalents"
-- contient deux niveaux). Chaque token brut est ensuite normalisé vers "bac+X" via le dict
-- ci-dessus, pour que les formes sans chiffre ("master", "licence"...) comptent aussi dans le
-- calcul du rang — le vocabulaire est volontairement large : le champ API n'utilise que
-- "Bac+X ..." dans les données observées, mais rien ne garantit que ce sera toujours le cas.
niveaux_bruts as (
    select
        ft.id,
        case
        {% for niveau, liste_regex in niveaux_formation.items() %}
            when regexp_contains(token, r'{{ liste_regex | join("|") }}') then "{{ niveau }}"
        {% endfor %}
            else null
        end as niveau_formation_api
    from base as ft
    left join unnest(json_query_array(ft.formations)) as f
    left join unnest(
        regexp_extract_all(
            lower(json_value(f, '$.niveauLibelle')),
            r'bac\s*\+\s*\d|master\s*\d?|\bm\s*\d\b|\blicence\s*\d*|\bbachelor\b'
        )
    ) as token
),

-- On prend le niveau numérique le plus élevé (ex. Bac+5 > Bac+3) pour chaque offre.
rang_max as (
    select
        id,
        max(safe_cast(substr(niveau_formation_api, 5) as int64)) as rang
    from niveaux_bruts
    group by id
),

-- Détection du Doctorat séparément du système de rang numérique ci-dessus : c'est un niveau
-- à part (Bac+8), pas une variante de Bac+5. Recherche directe dans formations[] (API), seul
-- angle mort de Gemini pour ce modèle.
doctorat_signal as (
    select
        id,
        regexp_contains(
            lower(to_json_string(formations)),
            r'doctorat|doctoral|docteur|\bphd\b|\bdoctorate\b|thèse'
        ) as a_doctorat
    from base
),

-- Fallback regex : le Doctorat est prioritaire s'il est détecté ; les niveaux résiduels
-- au-delà de Bac+5 sans mention explicite de doctorat sont plafonnés à Bac+5 (rang >= 5).
niveau_formation_regex as (
    select
        rang_max.id,
        case
            when doctorat_signal.a_doctorat then 'Doctorat'
            when rang_max.rang = 2 then 'Bac+2'
            when rang_max.rang = 3 then 'Bac+3'
            when rang_max.rang = 4 then 'Bac+4'
            when rang_max.rang >= 5 then 'Bac+5'
            else 'Non Renseigné'
        end as niveau_formation
    from rang_max
    left join doctorat_signal on rang_max.id = doctorat_signal.id
)

select
    base.id,
    case
        when ai_extraction.niveau_formation_ia in ('Bac+2', 'Bac+3', 'Bac+4', 'Bac+5', 'Doctorat')
            then ai_extraction.niveau_formation_ia
        else coalesce(niveau_formation_regex.niveau_formation, 'Non Renseigné')
    end as niveau_formation
from base
left join ai_extraction on base.id = ai_extraction.id
left join niveau_formation_regex on base.id = niveau_formation_regex.id
