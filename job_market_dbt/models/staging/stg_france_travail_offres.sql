-- Normalisation des offres brutes France Travail.
-- Source  : table raw_france_travail_offres (champs natifs de l'API Pôle Emploi / France Travail)
-- Sortie  : colonnes renommées, types castés, salaire extrait par regex depuis le libellé texte,
--           code commune normalisé sur 5 caractères (zéro-padding pour les DOM-TOM).

with source as (
    select * from {{ source('raw_offres', 'raw_france_travail_offres') }}
),

colonnes_utiles as (
    select
        trim(json_value(data, '$.id')) as id,
        trim(json_value(data, '$.intitule')) as intitule,
        trim(json_value(data, '$.romeCode')) as code_rome,
        trim(json_value(data, '$.romeLibelle')) as libelle_rome,
        trim(json_value(data, '$.typeContratLibelle')) as type_contrat,
        json_query(data, '$.formations') as formations,
        trim(json_value(data, '$.experienceLibelle')) as niveau_experience,
        json_query(data, '$.langues') as langues,
        trim(json_value(data, '$.entreprise.nom')) as nom_entreprise,
        case
            when length(json_value(data, '$.lieuTravail.commune')) = 4
            then trim(
                concat(
                    "0",
                    json_value(data, '$.lieuTravail.commune')
                )
            )
            when length(json_value(data, '$.lieuTravail.commune')) = 5
            then trim(json_value(data, '$.lieuTravail.commune'))
            else null
        end as code_commune,
        regexp_extract(json_value(data, '$.lieuTravail.libelle'), r'[a-zA-ZÎéè]+\-?\s?[a-zA-Z0-9]*\-?\s?[a-zA-Z]*') as ville,
        regexp_extract(json_value(data, '$.lieuTravail.libelle'), r'^[0-9]{2,3}') as code_departement,
        safe_cast(
            regexp_extract(
                json_value(data, '$.salaire.libelle'), 
                r'de (\d+\.?\d*)'
            ) as float64
        ) as salaire_min,
        safe_cast(
            regexp_extract(
                json_value(data, '$.salaire.libelle'), 
                r'à (\d+\.?\d*)'
            ) as float64
        ) as salaire_max,
        regexp_extract(
            json_value(data, '$.salaire.libelle'), 
            r'Annuel|Mensuel|Horaire'
        ) as salaire_periodicite,
        trim(json_value(data, '$.secteurActiviteLibelle')) as nom_secteur,
        extract(
            date from cast(
                json_value(data, '$.dateCreation') as timestamp
            )
        ) as date_publication,        
        trim(json_value(data, '$.description')) as description,
        'France Travail' as nom_plateforme,
        cast(
            trim(json_value(data, '$.nombrePostes')) as int64
        ) as nbre_postes
    from source
)

select * from colonnes_utiles