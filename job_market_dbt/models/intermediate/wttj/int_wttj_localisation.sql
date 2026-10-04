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
-- INSEE (même source que France Travail), en comparant des clés insensibles à la
-- casse, aux accents et aux apostrophes (macro cle_geo) : sans les accents,
-- "Ile-de-France" restait distinct de "Île-de-France" et créait une région de plus.
--
--   1. Département : par son libellé ; à défaut, par la ville quand son nom est
--      unique en France (libellés en anglais : "Maritime Alps" pour Valbonne).
--   2. Région : celle du département résolu. Le texte de région WTTJ n'est pas
--      utilisé : il est en anglais ("Loire Region") ou dans la langue locale.
--   3. Ville : libellé INSEE, cherché dans le département résolu (évite les
--      communes homonymes) ; repli initcap() sinon.
--
-- Département non résolu -> NULL : l'offre est écartée par int_wttj_offres
-- (filtre departement is not null). C'est le cas des offres hors de France
-- (Casablanca, Barcelone...), sans correspondance dans le référentiel INSEE.

with base as (
    select id, ville, departement
    from {{ ref('int_wttj_merge_ville') }}
),

geo_departements as (
    select distinct
        code_departement,
        nom_departement,
        nom_region
    from {{ ref('stg_ville_dept_reg') }}
),

geo_communes as (
    select
        code_departement,
        nom_ville,
        {{ cle_geo('nom_ville') }} as cle_ville
    from {{ ref('stg_ville_dept_reg') }}
),

villes_uniques as (
    select *
    from geo_communes
    qualify count(*) over (partition by cle_ville) = 1
),

dept_par_libelle as (
    select
        b.id,
        gd.code_departement
    from base as b
    inner join geo_departements as gd
        on {{ cle_geo('b.departement') }} = {{ cle_geo('gd.nom_departement') }}
    qualify row_number() over (partition by b.id order by gd.code_departement) = 1
),

dept_par_ville as (
    select
        b.id,
        vu.code_departement
    from base as b
    inner join villes_uniques as vu
        on {{ cle_geo('b.ville') }} = vu.cle_ville
),

dept_resolu as (
    select
        b.id,
        b.ville,
        gd.code_departement,
        gd.nom_departement as departement,
        gd.nom_region as region
    from base as b
    left join dept_par_libelle as dl on b.id = dl.id
    left join dept_par_ville as dv on b.id = dv.id
    left join geo_departements as gd
        on coalesce(dl.code_departement, dv.code_departement) = gd.code_departement
)

select
    dr.id,
    coalesce(gc.nom_ville, initcap(dr.ville)) as ville,
    dr.departement,
    dr.region
from dept_resolu as dr
left join geo_communes as gc
    on dr.code_departement = gc.code_departement
    and {{ cle_geo('dr.ville') }} = gc.cle_ville
qualify row_number() over (partition by dr.id order by gc.nom_ville) = 1
