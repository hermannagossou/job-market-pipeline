-- Enrichissement ciblé par LLM de niveau_formation / niveau_experience quand les
-- champs structurés n'ont rien donné (valeur 'Non Renseigné').
-- Source  : int_offres (unifié, les deux plateformes)
-- Sortie  : une ligne par offre traitée, niveau_formation_ia / niveau_experience_ia
--           (NULL si le LLM n'a rien trouvé d'exploitable non plus).
--
-- Ne traite QUE les offres qui en ont besoin (niveau_formation OU niveau_experience
-- = 'Non Renseigné' ET description non vide), pour limiter le coût LLM aux cas
-- utiles. Découvert en test manuel sur un échantillon de 20 offres : le texte libre
-- contient parfois un vrai signal ("4 à 5 ans d'expérience", "niveau BAC+5"), mais
-- de façon trop irrégulière pour une regex fiable (le "niveau demandé" et le
-- "niveau du candidat" sont parfois mélangés dans la même phrase, et des mots comme
-- "Senior" dans un titre de poste ne sont pas forcément un vrai signal
-- d'expérience). D'où l'appel LLM plutôt qu'un pattern-matching.
--
-- Vocabulaire de sortie contraint et validé après coup (pas de seed dédié ici,
-- liste fixe car il n'y a que 2 taxonomies courtes à respecter) :
--   niveau_formation  : Sans diplome, Cap, Bac, Bac+2, Bac+3, Bac+4, Bac+5, Phd
--   niveau_experience : Junior, Confirmé, Senior, Expert
--   (ces deux listes DOIVENT rester synchronisées avec int_wttj_niveau_formation,
--   int_wttj_niveau_experience et leurs équivalents France Travail)
--
-- Ce modèle ne modifie PAS int_offres. Le résultat est à joindre séparément :
--   coalesce(o.niveau_formation, case when o.niveau_formation='Non Renseigné'
--     then ia.niveau_formation_ia else o.niveau_formation end)
-- (ou plus simplement : ne remplacer que quand la valeur d'origine est
-- 'Non Renseigné', jamais écraser une valeur déjà connue).

{{
    config(
        materialized='incremental',
        static_analysis='off',
        unique_key='id',
        job_execution_timeout_seconds=1800
    )
}}

with descriptions as (

    select id, description from {{ ref('int_wttj_merge_ville') }}
    union all
    select id, description from {{ ref('int_france_travail_merge_ville') }}

),

offres_a_enrichir as (

    select
        o.id,
        o.niveau_formation,
        o.niveau_experience,
        o.nom_plateforme,
        d.description
    from {{ ref('int_offres') }} as o
    inner join descriptions as d
        on o.id = d.id
    where d.description is not null
        and (o.niveau_formation = 'Non Renseigné' or o.niveau_experience = 'Non Renseigné')
    {% if is_incremental() %}
        and o.id not in (select distinct id from {{ this }})
    {% endif %}

),

reponses_llm as (

    select
        id,
        nom_plateforme,
        niveau_formation,
        niveau_experience,
        result as reponse_brute
    from ai.generate_text(
        model `dbt_hermann.gemini_flash`,
        (
            select
                concat(
                    """
                    Tu es un expert en recrutement. Le texte ci-dessous est une offre
                    d'emploi. Identifie, UNIQUEMENT si c'est explicitement mentionné :
                    - le niveau de formation minimum REQUIS pour le poste (pas le
                      parcours du candidat idéal en cours, le niveau final visé)
                    - le niveau d'expérience REQUIS en années (pas le niveau du titre
                      du poste type "Senior Engineer" si aucune durée n'est donnée)

                    Réponds STRICTEMENT sous ce format, une ligne chacune, sans aucun
                    texte avant ni après :
                    FORMATION: <une valeur parmi Sans diplome, Cap, Bac, Bac+2, Bac+3, Bac+4, Bac+5, Phd, ou Aucune si non mentionné>
                    EXPERIENCE: <une valeur parmi Junior, Confirmé, Senior, Expert, ou Aucune si non mentionné>
                    JUSTIFICATION_EXPERIENCE: <le passage exact du texte qui justifie EXPERIENCE, ou vide si Aucune>

                    ATTENTION, piège fréquent : le TITRE du poste contient souvent un
                    mot comme "Senior", "Junior" ou "Confirmé" (ex. "Data Engineer
                    Senior", "Senior MLOps Engineer"). ÇA NE COMPTE PAS comme un
                    signal d'expérience si aucune durée en années n'est donnée
                    ailleurs dans le texte. Dans ce cas, réponds EXPERIENCE: Aucune,
                    même si le mot "Senior" apparaît dans le titre.
                    Exemple : "Rejoignez Doctolib en tant que Senior MLOps Engineer.
                    Vous construirez des pipelines ML..." (aucune durée mentionnée)
                    -> EXPERIENCE: Aucune (le "Senior" est un titre de poste, pas une
                    exigence chiffrée).

                    Règle de conversion années -> niveau si un nombre d'années est
                    donné : 0-2 ans = Junior, 3-5 ans = Confirmé, 6-10 ans = Senior,
                    plus de 10 ans = Expert.

                    Règle de conversion formation : Master 1 = Bac+4, Master 2 = Bac+5
                    (réponds toujours avec la nomenclature Bac+X, jamais "Master").

                    Offre :
                    """,
                    substr(o.description, 1, 2000)
                ) as prompt,
                o.id,
                o.nom_plateforme,
                o.niveau_formation,
                o.niveau_experience
            from offres_a_enrichir as o
        ),
        struct(
            0.1 as temperature,
            150 as max_output_tokens
        )
    )

),

extraction as (

    select
        id,
        nom_plateforme,
        niveau_formation,
        niveau_experience,
        trim(regexp_extract(reponse_brute, r'FORMATION:\s*(.*)')) as formation_extraite,
        trim(regexp_extract(reponse_brute, r'EXPERIENCE:\s*(.*)')) as experience_extraite,
        trim(regexp_extract(reponse_brute, r'JUSTIFICATION_EXPERIENCE:\s*(.*)')) as justification_experience
    from reponses_llm

),

-- Validation : on ne garde que les valeurs qui matchent EXACTEMENT le vocabulaire
-- attendu, pour se prémunir d'une hallucination ou d'un format non respecté.
-- Garde-fou supplémentaire sur EXPERIENCE (découvert en test : le LLM reprenait
-- parfois "Senior"/"Junior" depuis le TITRE du poste malgré la consigne, quand
-- aucune durée réelle n'était mentionnée) : on exige que la justification citée
-- contienne effectivement un chiffre suivi de "an" (an/ans/année/années), sinon on
-- rejette la réponse même si elle matche le vocabulaire attendu.
validation as (

    select
        id,
        nom_plateforme,
        case
            when niveau_formation != 'Non Renseigné' then null
            when formation_extraite in (
                'Sans diplome', 'Cap', 'Bac', 'Bac+2', 'Bac+3', 'Bac+4', 'Bac+5', 'Phd'
            ) then formation_extraite
            else null
        end as niveau_formation_ia,
        case
            when niveau_experience != 'Non Renseigné' then null
            when experience_extraite not in ('Junior', 'Confirmé', 'Senior', 'Expert') then null
            when not regexp_contains(justification_experience, r'\d+\s*an(?:s|née|nées)?') then null
            else experience_extraite
        end as niveau_experience_ia
    from extraction

)

select *
from validation