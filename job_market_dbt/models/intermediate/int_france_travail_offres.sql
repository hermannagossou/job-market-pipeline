with france_travail as (
    select * from {{ ref('stg_france_travail_offres') }}
),

geo as (
    select * from {{ ref('stg_ville_dept_reg') }}
),

france_travail_intermediate as (
    select
        ft.id as id_offre,
        ft.libelle_rome as nom_metier,
        ft.intitule as intitule,
        ft.nom_entreprise,
        case
            when ft.type_contrat like 'CDI%' then 'CDI'
            when ft.type_contrat like 'CDD%' then 'contrat temporaire'
            when ft.type_contrat like 'Intérim%' then 'contrat temporaire'
            when ft.type_contrat like 'Stage%' then 'stage'
            when ft.type_contrat like 'Apprentissage%' then 'alternance'
            when ft.type_contrat like 'Professionnalisation%' then 'alternance'
            when ft.type_contrat like '%Mois' then 'contrat temporaire'
            when ft.type_contrat like 'VIE%' then 'VIE'
            when ft.type_contrat is null then 'non renseigné'
            else 'autre'
        end as type_contrat,
        cast(null as string) as niveau_formation, -- pas de champ source exploité pour l'instant (voir formations JSON)
        ft.niveau_experience,
        geo.nom_ville as ville,
        geo.nom_departement as departement,
        geo.nom_region as region,
        ft.salaire_min,
        ft.salaire_max,
        case
            when lower(ft.salaire_periodicite) = 'annuel' then 'annuel'
            when lower(ft.salaire_periodicite) = 'mensuel' then 'mensuel'
            when lower(ft.salaire_periodicite) = 'horaire' then 'horaire'
            else 'non renseigné'
        end as statut_salaire,
        ft.nom_secteur,
        ft.date_publication,
        ft.nom_plateforme,
        ft.nbre_postes
    from france_travail as ft
    left join geo
        on ft.code_commune = geo.code_commune
)

select * from france_travail_intermediate