-- Unification des offres d'emploi des deux plateformes (WTTJ + France Travail)
-- sur un schéma commun. Les deux modèles source sont alignés en amont sur les
-- mêmes noms de colonnes, la même casse et les mêmes tranches niveau_experience.
--
-- ATTENTION points de vigilance connus, non résolus par cette unification :
--   - GRAIN : côté WTTJ, une offre multi-sites produit 1 ligne PAR VILLE (une offre
--     à 17 bureaux = 17 lignes) ; côté France Travail c'est 1 ligne = 1 offre.
--     Donc `count(*)` surestime le volume et biaise la comparaison entre
--     plateformes. Pour compter des offres, utiliser count(distinct id).
--   - SALAIRES non comparables en l'état : salaire_min/max cohabitent en annuel,
--     mensuel et horaire selon les offres (cf. statut_salaire). Toute moyenne
--     calculée directement sur ces colonnes mélange des ordres de grandeur.
--   - niveau_formation est NULL sur 100% des offres France Travail (mapping du
--     champ `formations` non fait) : tout filtre sur ce champ ne porte donc que
--     sur WTTJ.
--   - statut_salaire = périodicité de versement des deux côtés (Annuel/Mensuel/
--     Horaire/Journalier), PAS une info estimé/déclaré.
--   - Taxonomies métier non interchangeables : code_rome (France Travail, ROME
--     officiel) et new_profession_pivot_reference (WTTJ, taxonomie interne) sont
--     gardés en colonnes distinctes, chacune NULL sur l'autre plateforme.

with wttj as (

    select
        cast(id as string) as id,
        nom_metier,
        cast(null as string) as code_rome,
        new_profession_pivot_reference,
        intitule,
        nom_entreprise,
        type_contrat,
        niveau_formation,
        niveau_experience,
        niveau_langue,
        ville,
        departement,
        region,
        salaire_min,
        salaire_max,
        statut_salaire,
        nom_secteur,
        description,
        profile,
        missions_cles,
        date_publication,
        nom_plateforme,
        nbre_postes

    from `job-market-de-492514`.`dbt_maxime`.`int_wttj_offres`

),

france_travail as (

    select
        cast(id as string) as id,
        nom_metier,
        code_rome,
        cast(null as string) as new_profession_pivot_reference,
        intitule,
        nom_entreprise,
        type_contrat,
        niveau_formation,
        niveau_experience,
        cast(null as string) as niveau_langue,
        ville,
        departement,
        region,
        salaire_min,
        salaire_max,
        statut_salaire,
        nom_secteur,
        description,
        cast(null as string) as profile,
        cast(null as json) as missions_cles,
        date_publication,
        nom_plateforme,
        nbre_postes

    from `job-market-de-492514`.`dbt_maxime`.`int_france_travail`

),

offres_unifiees as (

    select * from wttj
    union all
    select * from france_travail

)

select *
from offres_unifiees