-- Détection des quasi-doublons intra-plateforme (même offre republiée sous un id
-- différent), pour une gestion pérenne au fil des runs quotidiens.
-- Source  : int_offres
-- Sortie  : une ligne par offre, enrichie d'un id_groupe_doublon, d'un compteur
--           d'annonces similaires et d'un rang dans le groupe.
--
-- Ne dédupliquer JAMAIS les lignes où nom_entreprise = 'Non Renseigné' (ou NULL) :
-- observé en test que deux offres réellement DIFFÉRENTES d'employeurs anonymes
-- distincts peuvent coïncider par hasard sur ville/métier/salaire (surtout sur des
-- salaires imputés par médiane, cf. limite ci-dessous). Chaque ligne à entreprise
-- inconnue reste donc son propre groupe, jamais fusionnée avec une autre.
--
-- Recalculé sur TOUT l'historique à chaque run (pas de logique incrémentale) : une
-- offre republiée à J+15 (cas observé : Sopra Steria/Nantes, 06/06 puis 19/06) ne
-- serait pas rapprochée de l'originale si seul le lot du jour était comparé.
--
-- CE MODÈLE NE SUPPRIME AUCUNE LIGNE. Le choix de compter les offres en volume brut
-- (count(*)) ou dédupliqué (count(distinct id_groupe_doublon), ou filtrer sur
-- est_premiere_annonce_du_groupe) est laissé à la couche marts/dashboard : les deux
-- lectures ("nombre d'annonces publiées" vs "nombre de postes distincts estimés")
-- sont légitimes selon la question posée, et ce modèle ne tranche pas à leur place.
--
-- LIMITE CONNUE : salaire_min/salaire_max peuvent être des valeurs IMPUTÉES (médiane
-- en cascade, cf. int_wttj_salaire / int_france_travail_salaire), pas déclarées.
-- Deux offres différentes tombées dans la même partition d'imputation reçoivent
-- exactement le même salaire par construction, ce qui peut sur-grouper des offres
-- distinctes comme si elles étaient identiques. Le risque est partiellement mitigé
-- par le fait qu'on requiert AUSSI la même entreprise pour grouper, mais reste réel
-- pour une même entreprise ayant plusieurs postes distincts non-déclarés dans la
-- même ville/métier/contrat/expérience.

with source as (
    select * from {{ ref('int_offres') }}
),

fingerprint as (
    select
        *,
        case
            when nom_entreprise is null or nom_entreprise = 'Non Renseigné' then id
            else to_hex(md5(concat(
                lower(trim(nom_entreprise)), '|',
                lower(trim(coalesce(ville, ''))), '|',
                lower(trim(coalesce(nom_metier, ''))), '|',
                lower(trim(coalesce(type_contrat, ''))), '|',
                lower(trim(coalesce(niveau_experience, ''))), '|',
                cast(coalesce(salaire_min, -1) as string), '|',
                cast(coalesce(salaire_max, -1) as string)
            )))
        end as id_groupe_doublon
    from source
),

groupes as (
    select
        *,
        count(*) over (partition by id_groupe_doublon) as nb_annonces_similaires,
        row_number() over (
            partition by id_groupe_doublon
            order by date_publication asc, id asc
        ) as rang_dans_groupe
    from fingerprint
)

select
    id,
    id_groupe_doublon,
    nb_annonces_similaires,
    rang_dans_groupe,
    rang_dans_groupe = 1 as est_premiere_annonce_du_groupe,
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
from groupes