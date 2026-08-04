-- Classification du type de contrat.
-- Source  : int_france_travail_merge_ville (champ type_contrat)
--           + int_france_travail_ai_type_contrat (classification par Gemini)
-- Sortie  : une ligne par offre avec le type de contrat normalisé parmi la liste définie,
--           ou "Non Renseigné" si non identifiable.
--
-- Stratégie : priorité à la classification Gemini (distingue le contrat de CE poste des
-- mentions non pertinentes : expérience passée acquise "en stage/alternance", sens non
-- contractuel d'un mot comme "alternance" - rotation d'équipe -, encadrement de personnes
-- employées sous un autre type de contrat...). Fallback sur la normalisation du champ API
-- (ex. "CDD - 12 Mois" -> "CDD", "Profession libérale" -> "Freelance") si Gemini répond
-- "Non Renseigné". Le champ API ne peut de toute façon jamais exprimer "Alternance" ou
-- "Stage" nativement (toujours replié en "CDD - X Mois" dans les données observées), donc
-- l'IA est indispensable pour ces deux catégories.

{%
    set types_contrat = {
        'Alternance': ['alternance', 'contrat d\'apprentissage', 'contrat de professionnalisation', 'professionnalisation', 'préparez un diplôme'],
        'Stage': ['stage'],
        'CDD': ['cdd'],
        'Freelance': ['freelance', 'profession libérale', 'profession commerciale'],
        'Intérim': ['intérim', 'interim'],
        'CDI': ['cdi', 'cdic']
    }
%}

with base as (
    select id, type_contrat
    from {{ ref('int_france_travail_merge_ville') }}
),

ai_extraction as (
    select id, type_contrat_ia
    from {{ ref('int_france_travail_ai_type_contrat') }}
),

-- Normalisation du champ API brut (utilisée uniquement en fallback).
type_contrat_api_normalise as (
    select
        id,
        case
        {% for type_contrat, liste_mots in types_contrat.items() %}
            when regexp_contains(lower(type_contrat), r"{{ liste_mots | join("|") }}")
            then "{{ type_contrat }}"
        {% endfor %}
            else 'Non Renseigné'
        end as type_contrat
    from base
)

select
    base.id,
    case
        when ai_extraction.type_contrat_ia in ('CDI', 'CDD', 'Stage', 'Alternance', 'Freelance', 'Intérim')
            then ai_extraction.type_contrat_ia
        else coalesce(type_contrat_api_normalise.type_contrat, 'Non Renseigné')
    end as type_contrat
from base
left join ai_extraction on base.id = ai_extraction.id
left join type_contrat_api_normalise on base.id = type_contrat_api_normalise.id
