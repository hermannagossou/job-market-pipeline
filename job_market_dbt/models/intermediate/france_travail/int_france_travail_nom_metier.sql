{%-
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
-%}

with source as (
    select * from {{ ref('int_france_travail_merge_ville') }}
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
        description
    from source
)

select * from extraction_nom_metier