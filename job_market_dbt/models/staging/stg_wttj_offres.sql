-- Normalisation des offres brutes Welcome to the Jungle.
-- Source  : table raw_wttj_offres 
-- Sortie  : colonnes renommées, types castés, salaire extrait par regex depuis le libellé texte,
--           code commune normalisé sur 5 caractères (zéro-padding pour les DOM-TOM).



with source as (
    select * from {{ source('raw_offres', 'raw_wttj_offres') }}
),

colonnes_utiles as (
    select
        trim(json_value(data, '$.objectID')) as id,
        trim(json_value(data, '$.name')) as intitule,
        trim(json_value(data, '$.new_profession.pivot_reference')) as new_profession_pivot_reference,
        trim(json_value(data, '$.new_profession.pivot_name')) as metier,
        trim(json_value(data, '$.contract_type')) as type_contrat,
        trim(json_value(data, '$.education_level')) as niveau_formation,
        safe_cast(json_value(data, '$.experience_level_minimum') as int64) as niveau_experience,
        trim(json_value(data, '$.language')) as niveau_langue,
        trim(
            regexp_replace(
                json_value(data, '$.profile'),
                r'<[^>]*>',
                ''
            )
        ) as profile,
        trim(json_value(data, '$.organization.name')) as nom_entreprise,
        json_query(data, '$.offices') as offices,
        safe_cast(
            trim(json_value(data, '$.salary_minimum')) as float64
        ) as salaire_min,
        safe_cast(
            trim(json_value(data, '$.salary_maximum')) as float64
        ) as salaire_max,
        trim(json_value(data, '$.salary_period')) as salaire_periodicite,
        trim(json_value(data, '$.salary_currency')) as salaire_devise,
        json_query(data, '$.sectors') as sectors,
        extract(
                date from safe_cast(
                    json_value(data, '$.published_at') as timestamp
                )
        ) as date_publication,
        trim(json_value(data, '$.summary')) as description,
        -- Pas d'URL dans la donnée Algolia : reconstruite depuis les slugs, au
        -- format des pages offres du site.
        concat(
            'https://www.welcometothejungle.com/fr/companies/',
            json_value(data, '$.organization.slug'),
            '/jobs/',
            json_value(data, '$.slug')
        ) as lien_offre,
        json_query(data, '$.key_missions') as missions_cles,
        'Welcome to the Jungle' as nom_plateforme,
        1 as nbre_postes
    from source
),

-- Dédoublonnage, même logique et même raison que côté France Travail (voir
-- stg_france_travail_offres.sql) -- aucun filtre équivalent n'existait ici
-- non plus.
dedoublonnees as (
    select *
    from colonnes_utiles
    qualify row_number() over (partition by id order by date_publication desc) = 1
)

select * from dedoublonnees