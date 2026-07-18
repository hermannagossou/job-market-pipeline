with wttj as (
    select * from {{ ref('int_wttj_offres') }}
),

france_travail as (
    select * from {{ ref('int_france_travail_offres') }}
),

unified_raw as (
    select * from wttj
    union all
    select * from france_travail
),

dim_experience as (
    select * from {{ ref('dim_niveau_experience') }}
),

unified as (
    select
        u.id_offre,
        u.nom_metier,
        u.intitule,
        u.nom_entreprise,
        u.type_contrat,
        u.niveau_formation,
        u.niveau_experience,
        u.ville,
        u.departement,
        u.region,
        u.salaire_min,
        u.salaire_max,
        u.statut_salaire,
        u.nom_secteur,
        u.date_publication,
        u.nom_plateforme,
        u.nbre_postes,
        dim.categorie as niveau_experience_categorie
    from unified_raw as u
    left join dim_experience as dim
        on least(u.niveau_experience, 15) = dim.annees
    qualify row_number() over (
        partition by u.id_offre, u.nom_plateforme
        order by u.date_publication desc
    ) = 1
)

select * from unified