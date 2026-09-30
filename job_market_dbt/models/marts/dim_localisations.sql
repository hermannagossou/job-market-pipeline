-- Dimension localisations : une ligne par combinaison ville / département / région.
-- Clé : id_localisation (surrogate key sur ville + departement + region)
-- La ville est normalisée AVANT le calcul de la clé : "Lyon 5e Arrondissement"
-- et "Lyon" doivent produire le même id_localisation, sinon un client qui
-- choisit "Lyon" ne matche jamais les offres taguées par arrondissement.

with source as (
    select
        *,
        {{ normalize_ville('ville') }} as ville_normalisee
    from {{ ref('int_offres') }}
)

select distinct
    {{ dbt_utils.generate_surrogate_key(['ville_normalisee', 'departement', 'region']) }} as id_localisation,
    ville_normalisee as ville,
    departement,
    region
from source