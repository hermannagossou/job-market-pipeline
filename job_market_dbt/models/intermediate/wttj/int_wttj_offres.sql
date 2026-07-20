-- Assemblage final des offres WTTJ : jointure de tous les modèles de transformation.
-- Source  : int_wttj_merge_ville + modèles de transformation spécialisés
-- Sortie  : une ligne par offre data confirmée (métier identifié ET département renseigné).
--
-- Miroir de int_france_travail_offres : mêmes colonnes, même ordre, mêmes filtres,
-- pour permettre le UNION ALL dans int_offres.
--
-- Deux filtres s'appliquent en sortie :
--   - regexp_contains sur nom_metier -> ne garde que les offres data de la liste.
--   - departement is not null        -> élimine les offres sans localisation exploitable
--                                       (offres "full remote worldwide" notamment).

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

with base as (
    select * from {{ ref('int_wttj_merge_ville') }}
),

nom_metier as (
    select id, nom_metier
    from {{ ref('int_wttj_nom_metier_finale') }}
),

type_contrat as (
    select id, type_contrat
    from {{ ref('int_wttj_type_contrat') }}
),

niveau_formation as (
    select id, niveau_formation
    from {{ ref('int_wttj_niveau_formation') }}
),

niveau_experience as (
    select id, niveau_experience
    from {{ ref('int_wttj_niveau_experience') }}
),

localisation as (
    select id, ville, departement, region
    from {{ ref('int_wttj_localisation') }}
),

salaire as (
    select id, salaire_min, salaire_max, statut_salaire
    from {{ ref('int_wttj_salaire') }}
),

-- WTTJ fournit le secteur dans le tableau sectors : premier élément retenu.
-- Pas d'appel LLM nécessaire ici, contrairement à int_france_travail_ai_nom_secteur.
secteur as (
    select
        id,
        coalesce(trim(json_value(sectors, '$[0].name')), 'Non Renseigné') as nom_secteur
    from base
)

select
    b.id,
    nm.nom_metier,
    b.nom_entreprise,
    tc.type_contrat,
    nf.niveau_formation,
    ne.niveau_experience,
    coalesce(l.ville, 'Non Renseigné') as ville,
    l.departement,
    l.region,
    s.salaire_min,
    s.salaire_max,
    s.statut_salaire,
    sec.nom_secteur,
    b.date_publication,
    b.nom_plateforme,
    b.nbre_postes
from base as b
left join nom_metier as nm on b.id = nm.id
left join type_contrat as tc on b.id = tc.id
left join niveau_formation as nf on b.id = nf.id
left join niveau_experience as ne on b.id = ne.id
left join localisation as l on b.id = l.id
left join salaire as s on b.id = s.id
left join secteur as sec on b.id = sec.id
where regexp_contains(
    lower(nm.nom_metier),
    r"{{ liste_noms_metier | join('|') }}"
)
    and l.departement is not null
