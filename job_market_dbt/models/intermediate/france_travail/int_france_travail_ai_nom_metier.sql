-- Classification du métier via Gemini Flash (BigQuery AI).
-- Source  : int_france_travail_offres_data (offres pré-filtrées)
-- Sortie  : une ligne par offre avec le métier normalisé parmi la liste définie,
--           ou "Non Renseigné" si l'offre ne correspond pas à un métier data.
-- Paramètres : temperature=0.2 (légère variabilité), max_output_tokens=40 (réponse courte attendue).

{{
    config(
        materialized='incremental',
        unique_key='id',
        static_analysis='off'
    )
}}

{%-
    set liste_noms_metier = [
        'data engineer',
        'data analyst',
        'data scientist',
        'machine learning engineer',
        'analytics engineer',
        'data architect',
        'business intelligence analyst',
        'data product manager',
        'chief data officer',
        'data steward',
        'data manager',
        'dataops engineer',
        'mlops engineer'
    ]
-%}

with source as (
    select * from {{ ref('int_france_travail_nom_metier') }}
    where nom_metier is null

    {% if is_incremental() %}
    and id not in (select id from {{ this }})
    {% endif %}
)

select
    id,
    initcap(trim(result)) as nom_metier
from ai.generate_text(
    model `prod.gemini_model`,
    (
        select
            concat(
                """
                Tu es un expert RH spécialisé dans les métiers de la Data.
                Classifie cette offre d'emploi dans UNE des catégories suivantes :

                {% for metier in liste_noms_metier %}
                - {{ metier | title }}
                {% endfor %}
                - Non Renseigné (si l'offre n'est PAS un métier de la data)

                Règles :
                1. Réponds avec SEULEMENT le nom de la catégorie, rien d'autre.
                2. Pas de guillemets, pas de ponctuation, pas de phrase.
                3. Si l'offre ne concerne pas la data, réponds exactement : Non Renseigné

                Description (extrait) :
                """, substr(description, 1, 1500)
            ) as prompt,
            id
        from source
    ),
    struct(
        0.2 as temperature,
        40 as max_output_tokens
    )
)