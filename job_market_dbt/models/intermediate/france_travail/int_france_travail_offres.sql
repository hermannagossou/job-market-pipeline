-- Assemblage final des offres France Travail : jointure de tous les modèles de transformation.
-- Source  : int_france_travail_offres_data + 7 modèles de transformation spécialisés
-- Sortie  : une ligne par offre data confirmée (métier validé par Gemini ET département renseigné).
--
-- Deux filtres s'appliquent en sortie :
--   - regexp_contains sur nom_metier → élimine les offres classifiées "Non Renseigné" par Gemini.
--   - departement is not null          → élimine les offres sans localisation exploitable.

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
    select * from {{ ref('int_france_travail_merge_ville') }}
),

nom_metier as (
    select id, nom_metier
    from {{ ref('int_france_travail_nom_metier_finale') }}
),

-- Fusion nom_entreprise : champ API en priorité, Gemini en fallback pour les offres anonymes.
entreprise_api as (
    select id, nom_entreprise from base
),

entreprise_ai as (
    select id, nom_entreprise
    from {{ ref('int_france_travail_ai_nom_entreprise') }}
),

entreprise as (
    select
        a.id,
        coalesce(a.nom_entreprise, ai.nom_entreprise) as nom_entreprise
    from entreprise_api as a
    left join entreprise_ai as ai on a.id = ai.id
),

type_contrat as (
    select id, type_contrat
    from {{ ref('int_france_travail_type_contrat') }}
),

niveau_formation as (
    select id, niveau_formation
    from {{ ref('int_france_travail_niveau_formation') }}
),

niveau_experience as (
    select id, niveau_experience
    from {{ ref('int_france_travail_niveau_experience') }}
),

localisation as (
    select id, ville, departement, region
    from {{ ref('int_france_travail_localisation') }}
),

salaire as (
    select id, salaire_min, salaire_max, statut_salaire
    from {{ ref('int_france_travail_salaire') }}
),

secteur as (
    select id, nom_secteur
    from {{ ref('int_france_travail_ai_nom_secteur') }}
)

select
    b.id,
    nm.nom_metier,
    e.nom_entreprise,
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
left join entreprise as e on b.id = e.id
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
