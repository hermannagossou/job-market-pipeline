-- Dernier repli pour niveau_experience : mots-clés dans l'intitulé du poste
-- (Junior/Confirmé/Senior), quand ni le champ structuré ni l'extraction LLM sur le
-- texte libre n'ont rien donné.
-- Source  : int_offres_enrichi (déjà enrichi par int_offres_enrichissement_ia)
--           + intitule brut (int_wttj_merge_ville / int_france_travail_merge_ville)
-- Sortie  : une ligne par offre, avec niveau_experience (repris tel quel si déjà
--           connu, ou déduit du titre en dernier recours) + niveau_experience_source
--           pour tracer l'origine de la valeur.
--
-- ATTENTION, signal DÉLIBÉRÉMENT moins fiable que les deux précédents : on a
-- justement rejeté ce type de signal lors de l'extraction LLM sur description
-- (fuite "Senior" depuis le titre, cf. int_offres_enrichissement_ia) parce qu'un
-- titre "Senior X" ne garantit pas un vrai niveau d'expérience élevé — c'est
-- parfois un effet d'inflation de titre (title inflation), sans lien avec les
-- années réellement exigées. On l'utilise ici SCIEMMENT comme dernier repli,
-- explicitement tracé via niveau_experience_source pour que la couche
-- consommatrice (dashboard, reco CV) puisse distinguer un niveau structuré fiable
-- d'une simple déduction depuis le titre.
--
-- 'Lead'/'Principal' volontairement exclus du mapping : ce sont des titres de
-- management/leadership, pas une mesure de séniorité technique — les inclure
-- recréerait le même biais que celui déjà corrigé.
--
-- Priorité de résolution : structure/ia_texte (déjà dans int_offres_enrichi) >
-- titre_intitule (ce modèle) > Non Renseigné.

with descriptions_intitule as (

    select id, intitule from {{ ref('int_wttj_merge_ville') }}
    union all
    select id, intitule from {{ ref('int_france_travail_merge_ville') }}

),

base as (

    select
        o.id,
        o.nom_metier,
        o.nom_entreprise,
        o.type_contrat,
        o.niveau_formation,
        o.niveau_experience,
        o.ville,
        o.departement,
        o.region,
        o.salaire_min,
        o.salaire_max,
        o.statut_salaire,
        o.nom_secteur,
        o.date_publication,
        o.nom_plateforme,
        o.nbre_postes,
        di.intitule
    from {{ ref('int_offres_enrichi') }} as o
    left join descriptions_intitule as di
        on o.id = di.id

),

-- Mots-clés recherchés dans l'intitulé, uniquement si niveau_experience est encore
-- inconnu à ce stade. Mots entourés de limites de mot (\b) pour éviter un match
-- partiel dans un autre mot.
deduction_titre as (

    select
        *,
        case
            when niveau_experience != 'Non Renseigné' then null
            when regexp_contains(lower(intitule), r'\bjunior\b|\bdébutant\b') then 'Junior'
            when regexp_contains(lower(intitule), r'\bconfirmé\b') then 'Confirmé'
            when regexp_contains(lower(intitule), r'\bsenior\b') then 'Senior'
            else null
        end as niveau_experience_depuis_titre
    from base

)

select
    id,
    nom_metier,
    nom_entreprise,
    type_contrat,
    niveau_formation,
    coalesce(niveau_experience_depuis_titre, niveau_experience) as niveau_experience,
    case
        when niveau_experience != 'Non Renseigné' then 'structure_ou_ia_texte'
        when niveau_experience_depuis_titre is not null then 'titre_intitule'
        else null
    end as niveau_experience_source,
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
from deduction_titre
