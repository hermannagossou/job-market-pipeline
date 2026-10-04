-- Résolution de la localisation (ville / département / région) des offres WTTJ.
-- Source  : int_wttj_merge_ville + stg_ville_dept_reg
-- Sortie  : une ligne par offre avec ville, departement et region.
--
-- WTTJ fournit directement les trois niveaux en texte libre (city / district /
-- local_state) — mais ce texte ne suit pas les règles de capitalisation
-- françaises ("Bouches-du-Rhône" chez France Travail devient "Bouches-Du-Rhône"
-- avec un simple initcap(), "de/du/et" ne devant pourtant pas prendre de
-- majuscule). Observé en test : cet écart de casse empêchait le regroupement
-- d'un même département/région entre les deux plateformes dans int_offres.
--
-- Stratégie : on résout le texte WTTJ vers le libellé CANONIQUE du référentiel
-- INSEE (même source que France Travail) par comparaison insensible à la casse
-- (et aux variantes d'apostrophe). Département et région sont résolus
-- INDÉPENDAMMENT l'un de l'autre (pas région = région du département matché) :
-- observé en test qu'une offre peut avoir un texte de région qui matche l'INSEE
-- alors que son texte de département ne matche pas (ou inversement), et faire
-- dépendre l'un de l'autre perdait des correspondances valides.
--
-- LIMITE CONNUE : ne résout que les écarts de CASSE/ACCENTS/APOSTROPHE, pas les
-- libellés entièrement différents (ex. "Maritime Alps" vs "Alpes-Maritimes", un nom
-- en anglais chez WTTJ). Ces cas retombent sur le repli initcap() non résolu.

with base as (
    select id, ville, departement, region
    from {{ ref('int_wttj_merge_ville') }}
),

geo_departements as (
    select distinct
        code_departement,
        nom_departement
    from {{ ref('stg_ville_dept_reg') }}
),

geo_regions as (
    select distinct nom_region
    from {{ ref('stg_ville_dept_reg') }}
),

geo_communes as (
    select
        code_departement,
        nom_ville
    from {{ ref('stg_ville_dept_reg') }}
),

-- Résolutions département et région indépendantes, normalisées (casse + apostrophe).
dept_resolu as (
    select
        b.id,
        b.ville,
        gd.code_departement,
        coalesce(gd.nom_departement, initcap(b.departement)) as departement
    from base as b
    left join geo_departements as gd
        on lower(trim(regexp_replace(b.departement, r"[\x{2018}\x{2019}]", "'")))
            = lower(trim(regexp_replace(gd.nom_departement, r"[\x{2018}\x{2019}]", "'")))
    qualify row_number() over (partition by b.id order by gd.code_departement) = 1
),

region_resolue as (
    select
        b.id,
        coalesce(gr.nom_region, initcap(b.region)) as region
    from base as b
    left join geo_regions as gr
        on lower(trim(regexp_replace(b.region, r"[\x{2018}\x{2019}]", "'")))
            = lower(trim(regexp_replace(gr.nom_region, r"[\x{2018}\x{2019}]", "'")))
),

-- Résolution de la ville, restreinte au département déjà trouvé pour éviter les
-- collisions entre communes homonymes de départements différents.
ville_resolue as (
    select
        dr.id,
        coalesce(gc.nom_ville, initcap(dr.ville)) as ville,
        dr.departement
    from dept_resolu as dr
    left join geo_communes as gc
        on dr.code_departement = gc.code_departement
        and lower(trim(dr.ville)) = lower(trim(gc.nom_ville))
    qualify row_number() over (partition by dr.id order by gc.nom_ville) = 1
)

select
    v.id,
    v.ville,
    v.departement,
    r.region
from ville_resolue as v
left join region_resolue as r
    on v.id = r.id