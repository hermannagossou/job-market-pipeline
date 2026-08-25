-- Classification du secteur d'activité via Gemini Flash parmi une liste de 15 secteurs définis.
-- Source  : int_france_travail_offres_data
-- Sortie  : une ligne par offre avec le secteur normalisé (ex. "Banque & Finance", "Conseil & ESN").
-- Paramètres : temperature=0.0 (déterministe), max_output_tokens=50 (label court attendu).

{{
    config(
        materialized='incremental',
        unique_key='id',
        static_analysis='off'
    )
}}

with source as (
    select * from {{ ref('int_france_travail_merge_ville') }}

    {% if is_incremental() %}
    where id not in (select id from {{ this }})
    {% endif %}
)

select
    id,
    initcap(trim(result)) as nom_secteur
from ai.generate_text(
    model `prod.gemini_model`,
    (
        select
            concat(
                """
                Tu es un expert en ressources humaines et en analyse du marché de l'emploi français.
                À partir de la description d'offre d'emploi ci-dessous, identifie le secteur d'activité
                de l'entreprise qui recrute.
                Réponds UNIQUEMENT avec le nom du secteur parmi cette liste stricte :

                1. Banque & Finance
                2. Assurance
                3. Télécommunications
                4. Retail & E-commerce
                5. Industrie & Manufacturing
                6. Santé & Pharmaceutique
                7. Conseil & ESN
                8. Média & Divertissement
                9. Logistique & Transport
                10. Énergie & Environnement
                11. Immobilier & Construction
                12. Éducation & Recherche
                13. Administration Publique
                14. Startup & Tech
                15. Autre

                Si le secteur ne correspond à aucun élément de la liste, réponds Autre.
                Ne fournis aucune explication, aucun texte supplémentaire.
                Réponds avec exactement un élément de la liste, sans le numéro.

                Description : """,
                substr(description, 1, 1500)
            ) as prompt,
            id
        from source
    ),
    struct(
        0.0 as temperature,
        50 as max_output_tokens
    )
)