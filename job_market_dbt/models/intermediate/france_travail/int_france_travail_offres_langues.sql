-- Extraction des langues demandées dans les offres France Travail.
-- Source  : int_france_travail_offres (offres filtrées) + stg_france_travail_offres
-- Sortie  : une ligne par couple (offre, langue) ; la langue API est prioritaire sur la description.
--
-- Deux sources combinées :
--   1. Champ API langues[] (tableau structuré) → libelle de la langue.
--   2. Regex sur description : capte les mentions implicites ("anglais courant", "maîtrise du français").

{% set langues = [
    "anglais",
    "fran[çc]ais",
    "espagnol",
    "allemand",
    "italien",
    "portugais",
    "chinois",
    "mandarin",
    "japonais",
    "arabe",
    "n[ée]erlandais",
    "hollandais",
    "russe",
    "cor[ée]en",
    "turc",
    "polonais",
    "su[ée]dois",
    "danois",
    "norv[ée]gien",
    "finnois",
    "hindi"
] %}
{% set langues_pattern = langues | join("|") %}

{% set prefixes = [
    "niveau d'",
    "niveau de",
    "ma[îi]trise de l'",
    "ma[îi]trise de",
    "ma[îi]trise du",
    "bonne ma[îi]trise de",
    "connaissance de",
    "pratique de",
    "parler"
] %}
{% set prefixes_pattern = prefixes | join("|") %}

{% set suffixes = [
    "professionnel(?:le)?s?",
    "requis(?:e)?",
    "indispensables?",
    "opérationnel(?:le)?s?",
    "courant(?:e)?s?",
    "obligatoires?",
    "techniques?",
    "fluent(?:e)?s?",
    "avancé(?:e)?s?",
    "bilingues?",
    "natif",
    "native"
] %}
{% set suffixes_pattern = suffixes | join("|") %}

with int_france_travail_offres as (
    select 
        id
    from {{ ref('int_france_travail_offres') }}
),

stg_france_travail_offres as (
    select
        id,
        langues,
        nom_plateforme,
        description
    from {{ ref('stg_france_travail_offres') }}
),

-- Unnest du tableau langues[] API et extraction regex depuis la description en parallèle.
int_france_travail_stg_offres as (
    select distinct
        ft.id,
        json_value(langue, '$.libelle') as langue_api,
        langue_description,
        stg.nom_plateforme,
        stg.description
    from int_france_travail_offres as ft
    left join stg_france_travail_offres as stg
        on ft.id = stg.id
    left join unnest(json_query_array(stg.langues)) as langue
    left join unnest(
        regexp_extract_all(
            lower(stg.description),
            r"(?:{{ prefixes_pattern }})?\s*({{ langues_pattern }})\s+(?:{{ suffixes_pattern }})?"
        )
    ) as langue_description
),

-- Priorité à la langue API ; fallback sur la description ; "Non Renseigné" si aucune trouvée.
int_france_travail_langue_finale as (
    select
        id,
        initcap(
            coalesce(langue_api, langue_description, 'Non Renseigné')
        ) as langue,
        nom_plateforme
    from int_france_travail_stg_offres
)

select id, langue, nom_plateforme from int_france_travail_langue_finale