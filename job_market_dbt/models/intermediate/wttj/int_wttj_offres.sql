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

-- WTTJ fournit le secteur dans le tableau sectors : premier élément retenu, puis
-- ramené à la liste fermée des secteurs de int_offres (test accepted_values).
-- La taxonomie WTTJ étant finie, une correspondance explicite suffit : pas d'appel
-- LLM, contrairement à int_france_travail_ai_nom_secteur. Secteur absent ou non
-- reconnu -> 'Autre', comme côté France Travail.
secteur_wttj as (
    select
        id,
        lower(trim(json_value(sectors, '$[0].name'))) as secteur
    from base
),

secteur as (
    select
        id,
        case
            when secteur is null then 'Autre'
            when regexp_contains(secteur, r'assurance') and not regexp_contains(secteur, r'fintech') then 'Assurance'
            when regexp_contains(secteur, r'banque|finance|fintech') then 'Banque & Finance'
            when regexp_contains(secteur, r'télécom') then 'Télécommunications'
            when regexp_contains(secteur, r'santé|pharma|biotech|medtech|médical') then 'Santé & Pharmaceutique'
            when regexp_contains(secteur, r'education|éducation|edtech|formation|recherche') then 'Éducation & Recherche'
            when regexp_contains(secteur, r'administration publique|secteur public') then 'Administration Publique'
            when regexp_contains(secteur, r'environnement|énergie|energie|cleantech|développement durable') then 'Énergie & Environnement'
            when regexp_contains(secteur, r'immobilier|bâtiment|travaux publics|construction') then 'Immobilier & Construction'
            when regexp_contains(secteur, r'logistique|supply chain|transport|mobilité') then 'Logistique & Transport'
            when regexp_contains(secteur, r'e-commerce|distribution|luxe|mode|grande consommation|retail') then 'Retail & E-Commerce'
            when regexp_contains(secteur, r'média|marketing|publicité|musique|design|jeux vidéo|divertissement') then 'Média & Divertissement'
            when regexp_contains(secteur, r'aéronautique|spatial|métallurgie|industrie|automobile|électronique|electronique') then 'Industrie & Manufacturing'
            when regexp_contains(secteur, r'conseil|organisation|management|stratégie|transformation|accompagnement|ingénierie|bureau d.études|recrutement|ressources humaines') then 'Conseil & Esn'
            when regexp_contains(secteur, r'it / digital|intelligence artificielle|logiciel|application mobile|saas|cloud|big data|cybersécurité|économie collaborative|incubateur') then 'Startup & Tech'
            else 'Autre'
        end as nom_secteur
    from secteur_wttj
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
    b.nbre_postes,
    b.lien_offre
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
