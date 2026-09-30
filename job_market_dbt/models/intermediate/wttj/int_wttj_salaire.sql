-- Normalisation et imputation des salaires annuels bruts (offres WTTJ).
-- Source  : int_wttj_merge_ville + modèles métier, contrat, expérience, localisation
-- Sortie  : une ligne par offre avec salaire_min, salaire_max (annuels) et statut_salaire.
--
-- Miroir de int_france_travail_salaire : même stratégie en deux étapes et surtout
-- MEME sémantique de statut_salaire (Déclaré / Estimé / Incomplet), pour que la
-- colonne ait le même sens des deux côtés dans int_offres.
--
-- Étape 1 — Normalisation : conversion Horaire/Journalier/Mensuel -> Annuel.
--   WTTJ a une périodicité de plus que France Travail : 'daily' (freelance / TJM),
--   converti sur 218 jours travaillés par an.
-- Étape 2 — Imputation : médiane (PERCENTILE_CONT 0.5) en cascade sur des partitions
--   de plus en plus larges, identique à France Travail.

{{
    config(
        materialized='table'
    )
}}

with base as (
    select id, salaire_min, salaire_max, salaire_periodicite
    from {{ ref('int_wttj_merge_ville') }}
),

nom_metier as (
    select id, nom_metier
    from {{ ref('int_wttj_nom_metier_finale') }}
),

type_contrat as (
    select id, type_contrat
    from {{ ref('int_wttj_type_contrat') }}
),

niveau_experience as (
    select id, niveau_experience
    from {{ ref('int_wttj_niveau_experience') }}
),

localisation as (
    select id, departement, region
    from {{ ref('int_wttj_localisation') }}
),

-- Étape 1 : conversion en base annuelle. Seuil 10 000 € pour distinguer mensuel/annuel
-- mal typé, même règle que côté France Travail.
--
-- Correctif additionnel découvert en test réel (id 0b4a20f6...) : des offres
-- taguées 'yearly' contiennent en fait des valeurs en K€ non multipliées (ex.
-- "48-55" pour un salaire de 48000-55000€) ou des gratifications mensuelles de
-- stage/alternance mal taguées 'yearly' (ex. "800-1200" pour 800-1200€/MOIS).
-- Les deux populations ne se chevauchent pas dans l'échantillon observé (15-110
-- pour le K€, 800-1500 pour le mensuel), d'où les seuils <=300 / <5000 ci-dessous.
-- Seuils choisis sur un échantillon limité : à revalider si des cas hors de ces
-- bornes apparaissent (ex. un vrai gros salaire K€ >300 ou une petite gratification
-- <800€/mois qui se ferait happer par la mauvaise règle).
salaire_normalise as (
    select
        id,
        case
            when salaire_periodicite = 'hourly' then salaire_min * 35 * 52
            when salaire_periodicite = 'daily' then salaire_min * 218
            when salaire_periodicite = 'monthly' and salaire_min <= 10000.0 then salaire_min * 12
            when salaire_periodicite = 'monthly' and salaire_min > 10000.0 then salaire_min
            when salaire_periodicite = 'yearly' and salaire_min > 0.0 and salaire_min <= 300.0 then salaire_min * 1000
            when salaire_periodicite = 'yearly' and salaire_min > 300.0 and salaire_min < 5000.0 then salaire_min * 12
            when salaire_min = 0.0 then null
            else salaire_min
        end as salaire_min,
        case
            when salaire_periodicite = 'hourly' then salaire_max * 35 * 52
            when salaire_periodicite = 'daily' then salaire_max * 218
            when salaire_periodicite = 'monthly' and salaire_max <= 10000.0 then salaire_max * 12
            when salaire_periodicite = 'monthly' and salaire_max > 10000.0 then salaire_max
            when salaire_periodicite = 'yearly' and salaire_max > 0.0 and salaire_max <= 300.0 then salaire_max * 1000
            when salaire_periodicite = 'yearly' and salaire_max > 300.0 and salaire_max < 5000.0 then salaire_max * 12
            when salaire_max = 0.0 then null
            else salaire_max
        end as salaire_max
    from base
),

-- Étape 2 : imputation par médiane en cascade (partition de plus en plus large).
salaire_impute as (
    select
        sn.id,
        coalesce(
            sn.salaire_min,
            percentile_cont(sn.salaire_min, 0.5 ignore nulls) over (partition by nm.nom_metier, ne.niveau_experience, l.departement, tc.type_contrat),
            percentile_cont(sn.salaire_min, 0.5 ignore nulls) over (partition by nm.nom_metier, ne.niveau_experience, l.region, tc.type_contrat),
            percentile_cont(sn.salaire_min, 0.5 ignore nulls) over (partition by nm.nom_metier, ne.niveau_experience, tc.type_contrat),
            percentile_cont(sn.salaire_min, 0.5 ignore nulls) over (partition by nm.nom_metier, tc.type_contrat),
            percentile_cont(sn.salaire_min, 0.5 ignore nulls) over (partition by tc.type_contrat),
            percentile_cont(sn.salaire_min, 0.5 ignore nulls) over ()
        ) as salaire_min_impute,
        coalesce(
            sn.salaire_max,
            percentile_cont(sn.salaire_max, 0.5 ignore nulls) over (partition by nm.nom_metier, ne.niveau_experience, l.departement, tc.type_contrat),
            percentile_cont(sn.salaire_max, 0.5 ignore nulls) over (partition by nm.nom_metier, ne.niveau_experience, l.region, tc.type_contrat),
            percentile_cont(sn.salaire_max, 0.5 ignore nulls) over (partition by nm.nom_metier, ne.niveau_experience, tc.type_contrat),
            percentile_cont(sn.salaire_max, 0.5 ignore nulls) over (partition by nm.nom_metier, tc.type_contrat),
            percentile_cont(sn.salaire_max, 0.5 ignore nulls) over (partition by tc.type_contrat),
            percentile_cont(sn.salaire_max, 0.5 ignore nulls) over ()
        ) as salaire_max_impute
    from salaire_normalise as sn
    left join nom_metier as nm on sn.id = nm.id
    left join type_contrat as tc on sn.id = tc.id
    left join niveau_experience as ne on sn.id = ne.id
    left join localisation as l on sn.id = l.id
)

select
    si.id,
    si.salaire_min_impute as salaire_min,
    case
        when si.salaire_max_impute < si.salaire_min_impute then si.salaire_min_impute
        else si.salaire_max_impute
    end as salaire_max,
    case
        when sn.salaire_min is not null and sn.salaire_max is not null then 'Déclaré'
        when sn.salaire_min is null and sn.salaire_max is null then 'Estimé'
        else 'Incomplet'
    end as statut_salaire
from salaire_impute as si
left join salaire_normalise as sn on si.id = sn.id