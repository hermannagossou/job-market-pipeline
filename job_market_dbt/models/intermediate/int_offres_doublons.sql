-- Dédoublonnage des offres (même annonce republiée sous un autre id), dernière
-- étape avant les marts.
-- Source  : int_offres + intitulé / description bruts (int_*_merge_ville)
-- Sortie  : les 17 colonnes de int_offres + nb_annonces_similaires, UNE LIGNE PAR
--           GROUPE DE DOUBLONS (la première annonce publiée est conservée).
--
-- Fait après l'union : c'est le seul endroit où les annonces des deux plateformes
-- peuvent être rapprochées.
--
-- Règle : même entreprise, même intitulé, même ville, même contrat ET description
-- identique. On préfère laisser passer une republication retouchée plutôt que
-- faire disparaître une offre réelle. La première empreinte (ville / métier /
-- contrat / expérience / salaire) regroupait des offres différentes : 231 offres
-- retirées sur 1 118, dont 223 avec une description différente, surtout à cause
-- des salaires imputés par médiane, identiques par construction (ex. 30 missions
-- Collective.work distinctes à 44 000-56 500 € estimés). Une règle floue
-- (similarité des descriptions) a été écartée : sur les cas « même entreprise,
-- même titre, même ville, texte différent », les scores des republications et des
-- offres distinctes se chevauchent (Jaccard 0,4-0,7). À revoir avec plus de volume.
--
-- Entreprise inconnue ('Non Renseigné' ou NULL) : jamais regroupée, deux
-- employeurs anonymes distincts pouvant coïncider par hasard.
--
-- Recalculé sur TOUT l'historique à chaque run (pas d'incrémental) : une offre
-- republiée à J+15 ne serait pas rapprochée de l'originale si seul le lot du jour
-- était comparé.

with source as (
    select * from {{ ref('int_offres') }}
),

textes as (
    select id, nom_plateforme, intitule, description from {{ ref('int_france_travail_merge_ville') }}
    union all
    select id, nom_plateforme, intitule, description from {{ ref('int_wttj_merge_ville') }}
),

empreintes as (
    select
        s.*,
        case
            when s.nom_entreprise is null or s.nom_entreprise = 'Non Renseigné'
                then concat(s.nom_plateforme, '|', s.id)
            else to_hex(md5(concat(
                {{ normalize_text('s.nom_entreprise') }}, '|',
                {{ normalize_text("coalesce(t.intitule, '')") }}, '|',
                {{ normalize_text("coalesce(s.ville, '')") }}, '|',
                coalesce(s.type_contrat, ''), '|',
                to_hex(md5(coalesce(t.description, '')))
            )))
        end as id_groupe_doublon
    from source as s
    -- Jointure sur id ET plateforme : les identifiants des deux sources ne sont
    -- pas garantis disjoints.
    left join textes as t
        on s.id = t.id and s.nom_plateforme = t.nom_plateforme
),

groupes as (
    select
        *,
        count(*) over (partition by id_groupe_doublon) as nb_annonces_similaires,
        row_number() over (
            partition by id_groupe_doublon
            order by date_publication asc, id asc
        ) as rang_dans_groupe
    from empreintes
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
    nbre_postes,
    lien_offre,
    nb_annonces_similaires
from groupes
where rang_dans_groupe = 1
