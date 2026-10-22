-- Enrichissement ciblé par LLM de niveau_formation / niveau_experience des offres
-- WTTJ quand les champs structurés n'ont rien donné (valeur 'Non Renseigné').
-- Source  : int_wttj_ai_niveaux_a_traiter (offres concernées, voir ce modèle)
-- Sortie  : une ligne par offre traitée, niveau_formation_ia / niveau_experience_ia
--           (NULL si le LLM n'a rien trouvé d'exploitable non plus).
--
-- Fait dans wttj/, avant l'union int_offres, comme l'enrichissement France Travail
-- (int_france_travail_ai_*) : int_offres ne fait que réunir les sources.
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
--   niveau_formation  : Bac+2, Bac+3, Bac+4, Bac+5, Doctorat (vocabulaire de
--                       int_offres ; un niveau inférieur à Bac+2 reste Non Renseigné)
--   niveau_experience : Junior, Confirmé, Senior, Expert
--   (ces deux listes DOIVENT rester synchronisées avec int_wttj_niveau_formation,
--   int_wttj_niveau_experience et leurs équivalents France Travail)
--
-- Le résultat est réinjecté par int_wttj_offres_enrichies, qui ne remplace que les
-- valeurs 'Non Renseigné' et n'écrase jamais une valeur déjà connue.
--
-- Périmètre : offres WTTJ uniquement. Côté France Travail, int_france_travail_ai_*
-- interrogent déjà Gemini sur la description complète (formation, expérience...) :
-- un second appel sur le même texte coûterait sans rien apporter.

{{
    config(
        materialized='incremental',
        static_analysis='off',
        unique_key='id',
        job_execution_timeout_seconds=1800
    )
}}

with offres_a_enrichir as (

    -- Seules les offres sans réponse exploitable (a_traiter) : un appel en erreur
    -- (quota, délai) est retenté au run suivant, et sa ligne remplacée (unique_key).
    -- Filtre calculé dans la file d'attente et non ici : un `id not in {{ this }}`
    -- dans cette requête est appliqué APRÈS l'appel à Gemini (voir
    -- macros/ids_deja_traites.sql).
    select *
    from {{ ref('int_wttj_ai_niveaux_a_traiter') }}
    where a_traiter

),

reponses_llm as (

    select
        id,
        nom_plateforme,
        niveau_formation,
        niveau_experience,
        result as reponse_brute
    from ai.generate_text(
        model `prod.gemini_model`,
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
                    FORMATION: <une valeur parmi Bac+2, Bac+3, Bac+4, Bac+5, Doctorat, ou Aucune si non mentionné ou inférieur à Bac+2>
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
                    Doctorat / PhD = Doctorat.

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
        reponse_brute,
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
-- rejette la réponse même si elle matche le vocabulaire attendu. Justification
-- absente (ligne omise, réponse tronquée) : regexp_contains vaut NULL, d'où le
-- coalesce à false pour que le garde-fou rejette aussi ce cas.
validation as (

    select
        id,
        nom_plateforme,
        reponse_brute,
        case
            when niveau_formation != 'Non Renseigné' then null
            when formation_extraite in (
                'Bac+2', 'Bac+3', 'Bac+4', 'Bac+5', 'Doctorat'
            ) then formation_extraite
            else null
        end as niveau_formation_ia,
        case
            when niveau_experience != 'Non Renseigné' then null
            when experience_extraite not in ('Junior', 'Confirmé', 'Senior', 'Expert') then null
            when not coalesce(regexp_contains(justification_experience, r'\d+\s*an(?:s|née|nées)?'), false) then null
            else experience_extraite
        end as niveau_experience_ia
    from extraction

)

-- Appels en erreur (quota, délai) : reponse_brute NULL, enregistrés avec
-- reponse_ok = false pour être retentés au run suivant (voir offres_a_enrichir).
-- Ne PAS filtrer ici sur reponse_brute : BigQuery appliquerait ce filtre dans
-- l'étape Gemini, avant le filtre incrémental, et rappellerait Gemini sur toutes
-- les offres à traiter à chaque run (mesuré : 288 appels pour 0 offre nouvelle).
select
    id,
    nom_plateforme,
    niveau_formation_ia,
    niveau_experience_ia,
    reponse_brute is not null as reponse_ok
from validation