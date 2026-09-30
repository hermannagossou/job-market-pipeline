-- Version enrichie de int_offres : niveau_formation / niveau_experience complétés
-- par extraction LLM (int_offres_enrichissement_ia) quand le champ structuré
-- valait 'Non Renseigné'. Toutes les autres colonnes sont inchangées.
--
-- Ne remplace JAMAIS une valeur déjà connue, même si le LLM en propose une
-- différente : la donnée structurée d'origine est toujours prioritaire, l'IA ne
-- comble que les trous.
--
-- int_offres lui-même n'est pas modifié (contrat verrouillé sur 16 colonnes, miroir
-- exact du schéma France Travail pour l'UNION ALL) : ce modèle est une couche
-- séparée, à utiliser à la place de int_offres partout où l'enrichissement compte
-- (ex. filtres par niveau de formation/expérience dans un dashboard).

with source as (

    select * from {{ ref('int_offres') }}

),

enrichissement as (

    select id, niveau_formation_ia, niveau_experience_ia
    from {{ ref('int_offres_enrichissement_ia') }}

)

select
    s.id,
    s.nom_metier,
    s.nom_entreprise,
    s.type_contrat,
    coalesce(e.niveau_formation_ia, s.niveau_formation) as niveau_formation,
    coalesce(e.niveau_experience_ia, s.niveau_experience) as niveau_experience,
    s.ville,
    s.departement,
    s.region,
    s.salaire_min,
    s.salaire_max,
    s.statut_salaire,
    s.nom_secteur,
    s.date_publication,
    s.nom_plateforme,
    s.nbre_postes
from source as s
left join enrichissement as e
    on s.id = e.id
