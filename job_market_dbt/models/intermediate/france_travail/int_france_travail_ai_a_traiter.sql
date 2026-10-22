-- File d'attente des enrichissements Gemini au niveau de toutes les offres France Travail.
-- Source  : int_france_travail_merge_ville (+ int_france_travail_nom_metier pour le métier)
-- Sortie  : une ligne par offre, avec le texte envoyé au modèle et un drapeau par modèle IA
--           (a_traiter_<x> = true si l'offre doit lui être envoyée à ce run).
--
-- Une offre est à traiter tant que le modèle IA n'a pas enregistré de réponse non NULL
-- pour elle : un appel en erreur (quota, délai) est donc retenté au run suivant. Une
-- réponse reçue mais hors vocabulaire n'est pas retentée (temperature 0 : même réponse)
-- et est corrigée en aval (ex. secteur -> 'Autre' dans int_france_travail_offres).
-- Pourquoi une table séparée : voir macros/ids_deja_traites.sql.
--
-- --full-refresh d'un modèle IA : sélectionner aussi cette table
-- (ex. dbt build -s +int_france_travail_ai_nom_secteur --full-refresh), sinon le modèle
-- serait recréé avec les seules offres de la file d'attente.

{{
    config(
        materialized='table',
        static_analysis='off'
    )
}}

with offres as (
    select id, intitule, description, nom_entreprise
    from {{ ref('int_france_travail_merge_ville') }}
),

metier_regex as (
    select id, nom_metier
    from {{ ref('int_france_travail_nom_metier') }}
)

select
    o.id,
    o.intitule,
    o.description,
    o.id not in {{ ids_deja_traites('int_france_travail_ai_nom_secteur', 'nom_secteur is not null') }}
        as a_traiter_secteur,
    o.id not in {{ ids_deja_traites('int_france_travail_ai_niveau_formation', 'niveau_formation_ia is not null') }}
        as a_traiter_formation,
    o.id not in {{ ids_deja_traites('int_france_travail_ai_niveau_experience', 'niveau_experience_ia is not null') }}
        as a_traiter_experience,
    o.id not in {{ ids_deja_traites('int_france_travail_ai_type_contrat', 'type_contrat_ia is not null') }}
        as a_traiter_contrat,
    -- Entreprise : seulement les offres anonymes.
    o.nom_entreprise is null
        and o.id not in {{ ids_deja_traites('int_france_travail_ai_nom_entreprise', 'nom_entreprise is not null') }}
        as a_traiter_entreprise,
    -- Métier : seulement quand la regex sur l'intitulé n'a rien trouvé.
    m.nom_metier is null
        and o.id not in {{ ids_deja_traites('int_france_travail_ai_nom_metier', 'nom_metier is not null') }}
        as a_traiter_metier
from offres as o
left join metier_regex as m on o.id = m.id
