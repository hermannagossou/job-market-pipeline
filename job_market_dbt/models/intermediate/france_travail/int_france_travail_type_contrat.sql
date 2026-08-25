-- Classification du type de contrat en deux passes.
-- Source  : int_france_travail_offres_data
-- Sortie  : une ligne par offre avec le type de contrat normalisé parmi la liste définie,
--           ou "Non Renseigné" si non identifiable.
--
-- Passe 1 : détection dans la description (prioritaire, plus précise que le champ API).
-- Passe 2 : normalisation du résultat combiné (description en priorité, sinon champ API).

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
    select id, type_contrat, description
    from {{ ref('int_france_travail_merge_ville') }}
),

-- Passe 1 : le type issu de la description écrase le champ API si un mot-clé est trouvé.
type_contrat_combine as (
    select
        id,
        case
        {% for type_contrat, liste_mots in types_contrat.items() %}
            when regexp_contains(
                lower(description),
                r"\b({{ liste_mots | join("|") }})\b"
            ) then "{{ type_contrat }}"
        {% endfor %}
            else type_contrat
        end as type_contrat_brut
    from base
)

select
    id,
    case
    {% for type_contrat, liste_mots in types_contrat.items() %}
        when regexp_contains(
            lower(type_contrat_brut),
            r"{{ liste_mots | join("|") }}"
        )
        then "{{ type_contrat }}"
    {% endfor %}
        else 'Non Renseigné'
    end as type_contrat
from type_contrat_combine
