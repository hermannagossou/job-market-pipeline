-- Offres WTTJ avec niveaux de formation et d'expérience complétés.
-- Source  : int_wttj_offres + int_wttj_ai_niveaux (Gemini) + int_wttj_merge_ville (intitulé)
-- Sortie  : même schéma que int_wttj_offres (17 colonnes, même ordre), pour le
--           UNION ALL de int_offres.
--
-- Priorité de résolution, sans jamais écraser une valeur déjà connue :
--   formation  : structuré > Gemini
--   expérience : structuré > Gemini > mots-clés de l'intitulé
--
-- Le repli sur l'intitulé est DÉLIBÉRÉMENT moins fiable : un titre « Senior X » ne
-- garantit pas un vrai niveau d'expérience élevé (inflation de titre), c'est
-- pourquoi l'extraction Gemini rejette ce signal. Il ne sert qu'en dernier recours.
-- 'Lead'/'Principal' volontairement exclus : titres de management, pas une mesure
-- de séniorité technique.
--
-- Limites de mot écrites à la main : dans BigQuery, \b ne reconnaît que les
-- lettres ASCII, donc \bconfirmé\b ne trouvait jamais de fin de mot après le « é »
-- (8 offres « Confirmé(e) » restaient sans niveau).

{% set debut = "(?:^|[^a-zà-ÿ])" %}
{% set fin = "(?:$|[^a-zà-ÿ])" %}

with offres as (
    select * from {{ ref('int_wttj_offres') }}
),

niveaux_ia as (
    select id, niveau_formation_ia, niveau_experience_ia
    from {{ ref('int_wttj_ai_niveaux') }}
),

intitules as (
    select id, lower(intitule) as intitule
    from {{ ref('int_wttj_merge_ville') }}
),

niveaux_completes as (
    select
        o.id,
        case
            when o.niveau_formation != 'Non Renseigné' then o.niveau_formation
            else coalesce(ia.niveau_formation_ia, 'Non Renseigné')
        end as niveau_formation,
        case
            when o.niveau_experience != 'Non Renseigné' then o.niveau_experience
            when ia.niveau_experience_ia is not null then ia.niveau_experience_ia
            when regexp_contains(i.intitule, r'{{ debut }}(?:junior|débutante?){{ fin }}') then 'Junior'
            when regexp_contains(i.intitule, r'{{ debut }}(?:confirmée?|confirmed){{ fin }}') then 'Confirmé'
            when regexp_contains(i.intitule, r'{{ debut }}s[eé]nior{{ fin }}') then 'Senior'
            else 'Non Renseigné'
        end as niveau_experience
    from offres as o
    left join niveaux_ia as ia on o.id = ia.id
    left join intitules as i on o.id = i.id
)

select
    o.id,
    o.nom_metier,
    o.nom_entreprise,
    o.type_contrat,
    n.niveau_formation,
    n.niveau_experience,
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
    o.lien_offre
from offres as o
inner join niveaux_completes as n on o.id = n.id
