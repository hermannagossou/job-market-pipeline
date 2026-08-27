-- Table de faits principale : une ligne par offre d'emploi publiée sur une plateforme.
-- Granularité : 1 offre × 1 plateforme (France Travail, WTTJ…)
-- Clé : id_offre (surrogate key sur id + nom_plateforme)
-- Toutes les dimensions sont reliées via leurs surrogate keys respectives.

{{
    config(
        materialized='incremental',
        unique_key='id_offre'
    )
}}

with int_offres as (
    select * from {{ ref('int_offres') }}

    {% if is_incremental() %}
    where {{ dbt_utils.generate_surrogate_key(['id', 'nom_plateforme']) }}
        not in (select id_offre from {{ this }})
    {% endif %}
),

fact as (
    select
        {{ dbt_utils.generate_surrogate_key(['id', 'nom_plateforme']) }} as id_offre,
        {{ dbt_utils.generate_surrogate_key(['nom_metier']) }} as id_metier,
        {{ dbt_utils.generate_surrogate_key(['nom_entreprise']) }} as id_entreprise,
        {{ dbt_utils.generate_surrogate_key(['type_contrat']) }} as id_contrat,
        {{ dbt_utils.generate_surrogate_key(['niveau_formation']) }} as id_formation,
        {{ dbt_utils.generate_surrogate_key(['niveau_experience']) }} as id_experience,
        {{ dbt_utils.generate_surrogate_key(['ville', 'departement', 'region']) }} as id_localisation,
        {{ dbt_utils.generate_surrogate_key(['nom_secteur']) }} as id_secteur,
        {{ dbt_utils.generate_surrogate_key(['date_publication']) }} as id_date,
        nom_plateforme,
        salaire_min,
        salaire_max,
        statut_salaire,
        nbre_postes
    from int_offres
)

select * from fact