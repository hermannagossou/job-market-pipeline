-- Extraction du métier data par regex sur l'intitulé de l'offre.
-- Source  : int_wttj_merge_ville
-- Sortie  : une ligne par offre, nom_metier parmi les 13 métiers data ou NULL.
--
-- Miroir de int_france_travail_nom_metier : même liste, même méthode, pour que les
-- deux plateformes produisent exactement le même vocabulaire métier.
-- WTTJ expose aussi `metier` (taxonomie interne pivot) : utilisé en fallback dans
-- int_wttj_nom_metier_finale, l'intitulé restant prioritaire car plus spécifique.

{%
    set liste_noms_metier = [
        'data engineer',
        'data analyst',
        'data scientist',
        'machine learning engineer',
        'analytics engineer',
        'data architect',
        'business intelligence analyst',
        'data product manager',
        'chief data officer',
        'data steward',
        'data manager',
        'dataops engineer',
        'mlops engineer'
    ]
%}

with source as (
    select * from {{ ref('int_wttj_merge_ville') }}
),

extraction_nom_metier as (
    select
        id,
        initcap(
            regexp_extract(
                lower(intitule),
                r'{% for nom_metier in liste_noms_metier %}{{ nom_metier }}{% if not loop.last %}|{% endif %}{% endfor %}'
            )
        ) as nom_metier,
        initcap(
            regexp_extract(
                lower(metier),
                r'{% for nom_metier in liste_noms_metier %}{{ nom_metier }}{% if not loop.last %}|{% endif %}{% endfor %}'
            )
        ) as nom_metier_pivot,
        description
    from source
)

select * from extraction_nom_metier
