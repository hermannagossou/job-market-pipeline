-- Résolution de la localisation (ville / département / région) pour chaque offre.
-- Source  : int_france_travail_offres_data + stg_ville_dept_reg
-- Sortie  : une ligne par offre avec ville, departement et region résolus.
--
-- Deux chemins de résolution :
--   1. code_commune présent → jointure directe sur le seed INSEE (précis).
--   2. code_commune absent  → jointure floue sur nom_departement (préfixe ou égalité stricte).
-- Le COALESCE final fusionne les deux pour obtenir la valeur la plus complète.

with base as (
    select id, code_commune, code_departement, ville, nom_ville
    from {{ ref('int_france_travail_merge_ville') }}
),

source_region_departement as (
    select distinct
        code_departement,
        nom_departement,
        nom_region
    from {{ ref('stg_ville_dept_reg') }}
),

-- Chemin 1 : offres avec code_commune reconnu → libellé officiel INSEE, tel quel.
-- Le nom extrait du libellé API n'est jamais plus fiable : il est tronqué par la
-- regex de staging aux accents/apostrophes ("Villeneuve-d", "Besan") ou en
-- majuscules sans tirets ("LEVALLOIS PERRET"). L'ancienne règle "nom le plus long"
-- retenait ce dernier à longueur égale, d'où des doublons de villes
-- ("Levallois Perret" / "Levallois-Perret") ; initcap déformait aussi les noms
-- officiels ("Aix-en-Provence" -> "Aix-En-Provence").
ville_dept_region_choisie as (
    select
        b.id,
        b.code_commune,
        b.code_departement,
        b.nom_ville as ville_choisie,
        srd.nom_departement,
        srd.nom_region
    from base as b
    left join source_region_departement as srd
        on b.code_departement = srd.code_departement
),

-- Chemin 2 : fallback pour les offres sans code_commune, résolution via nom du département.
-- La jointure par préfixe (starts_with) gère les abréviations dans les libellés API.
sans_code_commune as (
    select
        vc.id,
        vc.ville_choisie,
        srd.nom_departement,
        srd.nom_region
    from ville_dept_region_choisie as vc
    left join source_region_departement as srd
        on lower(vc.nom_departement) = lower(srd.nom_departement)
            or (
                length(vc.nom_departement) >= 2
                and starts_with(lower(srd.nom_departement), lower(vc.nom_departement))
            )
    where vc.code_commune is null
)

select
    vc.id,
    coalesce(cast(vc.ville_choisie as string), cast(sc.ville_choisie as string)) as ville,
    coalesce(cast(sc.nom_departement as string), cast(vc.nom_departement as string)) as departement,
    coalesce(cast(vc.nom_region as string), cast(sc.nom_region as string)) as region
from ville_dept_region_choisie as vc
left join sans_code_commune as sc
    on vc.id = sc.id
