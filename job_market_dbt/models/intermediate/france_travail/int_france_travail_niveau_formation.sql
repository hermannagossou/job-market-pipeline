-- Extraction et classification du niveau de formation requis (Bac+2 à Bac+5).
-- Source  : int_france_travail_offres_data (champs formations[] et description)
-- Sortie  : une ligne par offre avec le niveau de formation le plus élevé trouvé,
--           ou "Non Renseigné" si aucun niveau identifiable.
--
-- Stratégie : extraction regex depuis le champ API formations[] (niveauLibelle)
-- ET depuis la description (fallback). On prend le niveau max trouvé via QUALIFY sur rang.

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

-- Unnest formations[] et extraction regex depuis le libellé API et la description.
-- Le cross-join des deux unnest est voulu : on veut toutes les combinaisons pour récupérer
-- le niveau le plus élevé dans la passe suivante.
niveaux_bruts as (
    select
        ft.id,
        regexp_extract(
            lower(json_value(f, '$.niveauLibelle')),
            r'bac\s*\+\s*\d|master\s*\d?|\bm\s*\d\b|\blicence\s*\d*|\bbachelor\b'
        ) as niveau_formation_api,
        case
        {% for niveau, liste_regex in niveaux_formation.items() %}
            when regexp_contains(niveau_formation_description, r'{{ liste_regex | join("|") }}') then "{{ niveau }}"
        {% endfor %}
            else null
        end as niveau_formation_description
    from base as ft
    left join unnest(json_query_array(ft.formations)) as f
    left join unnest(
        regexp_extract_all(
            lower(ft.description),
            r'bac\s*\+\s*\d|master\s*\d?|\bm\s*\d\b|\blicence\s*\d*|\bbachelor\b'
        )
    ) as niveau_formation_description
),

-- On prend le niveau numérique le plus élevé (ex. Bac+5 > Bac+3) pour chaque offre.
rang_max as (
    select
        id,
        max(
            safe_cast(
                substr(coalesce(niveau_formation_api, niveau_formation_description), 5)
                as int64
            )
        ) as rang
    from niveaux_bruts
    group by id
)

select
    id,
    case
        when rang = 2 then 'Bac+2'
        when rang = 3 then 'Bac+3'
        when rang = 4 then 'Bac+4'
        when rang = 5 then 'Bac+5'
        else 'Non Renseigné'
    end as niveau_formation
from rang_max
