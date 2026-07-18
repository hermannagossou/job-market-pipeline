
with wttj_intermediate as (

    select
        id as id_offre,
        metier as nom_metier,
        intitule as intitule,
        nom_entreprise,

        case 
            when type_contrat = 'intership' then 'stage'
            when type_contrat = 'full_time' then 'CDI'
            when type_contrat = 'freelance' then 'freelance'
            when type_contrat = 'apprenticeship' then 'alternance'
            when type_contrat = 'temporary' then 'contrat temporaire'
            when type_contrat = 'other' then 'autre'
            when type_contrat = 'vie' then 'VIE'
            when type_contrat = 'graduate_program' then 'programme jeune diplomes'
            when type_contrat = 'part_time' then 'temps partiel'
            when type_contrat is null then 'non renseigné'
            else 'non renseigné'
        end as type_contrat,

        case 
            when niveau_formation = 'bac_5' then 'BAC +5'
            when niveau_formation = 'phd' then 'PHD'
            when niveau_formation = 'bac_4' then 'BAC +4'
            when niveau_formation = 'bac_3' then 'BAC +3'
            when niveau_formation = 'bac_2' then 'BAC +2'
            when niveau_formation = 'bac' then 'BAC'
            when niveau_formation = 'no_diploma' then 'sans diplome'
            when niveau_formation = 'cap' then 'CAP'
            when niveau_formation is null then 'non renseigné'
        end as niveau_formation,

        cast(niveau_experience as string) as niveau_experience,

        trim(json_value(offices, '$[0].city')) as ville,
        trim(json_value(offices, '$[0].district')) as departement,
        trim(json_value(offices, '$[0].local_state')) as region,

        salaire_min,
        salaire_max,

        case
            when lower(salaire_periodicite) = 'yearly' then 'annuel'
            when lower(salaire_periodicite) = 'monthly' then 'mensuel'
            when lower(salaire_periodicite) = 'daily' then 'journal'
            else 'non renseigné'
        end as statut_salaire,

        trim(json_value(sectors, '$[0].name')) as nom_secteur,

        date_publication,
        nom_plateforme,
        nbre_postes

    from {{ ref('stg_wttj_offres') }}

)

select *
from wttj_intermediate
