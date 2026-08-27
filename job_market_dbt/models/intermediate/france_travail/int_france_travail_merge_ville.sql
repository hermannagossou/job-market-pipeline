-- Déduplication des offres France Travail et enrichissement avec la référence géographique.
-- Source  : stg_france_travail_offres + stg_ville_dept_reg
-- Sortie  : une ligne par offre (doublons éliminés par QUALIFY), avec nom_ville, nom_departement
--           et nom_region issus du seed INSEE, jointure sur code_commune.

with stg_france_travail_offres as (
    select * from {{ ref('stg_france_travail_offres') }}

),

-- Dédoublonnage : en cas d'id dupliqué, on conserve la version la plus récente.
int_france_travail_dedup as (
    select *
    from stg_france_travail_offres
    qualify row_number() over(partition by id order by date_publication desc) = 1
),

stg_ville_dept_reg as (
    select * from {{ ref('stg_ville_dept_reg') }}
)

select
    ft.id,
    ft.intitule,
    ft.nom_entreprise,
    ft.type_contrat,
    ft.formations,
    ft.niveau_experience,
    ft.code_commune as code_commune,
    coalesce(ft.code_departement, df.code_departement) as code_departement,
    ft.ville,
    df.nom_ville,
    ft.salaire_min,
    ft.salaire_max,
    ft.salaire_periodicite,
    ft.nom_secteur,
    ft.date_publication,
    ft.description,
    ft.nom_plateforme,
    ft.nbre_postes
from int_france_travail_dedup as ft
left join stg_ville_dept_reg as df
    on ft.code_commune = df.code_commune