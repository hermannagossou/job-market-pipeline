-- Point d'union multi-sources pour les offres.
-- Actuellement : France Travail uniquement. WTTJ sera ajouté via UNION ALL ici.
-- Source  : int_france_travail_offres (et futurs équivalents par plateforme)
-- Sortie  : toutes les offres toutes plateformes confondues, schéma unifié.

with source_france_travail as (
    select * from {{ ref('int_france_travail_offres') }}

),

int_offres_combinees as (
    select * from source_france_travail
)

select
    id,
    nom_metier,
    nom_entreprise,
    type_contrat,
    niveau_formation,
    niveau_experience,
    ville,
    departement,
    region,
    salaire_min,
    salaire_max,
    statut_salaire,
    nom_secteur,
    date_publication,
    nom_plateforme,
    nbre_postes
from int_offres_combinees
