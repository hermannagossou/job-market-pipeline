-- Extraction des compétences techniques via Gemini Flash + liste fixe (seed competences).
-- Source  : int_france_travail_offres (offres validées) + stg_france_travail_offres
-- Sortie  : une ligne par couple (offre, compétence) ; toutes les compétences sont dans le seed.
--
-- Optimisations coût vs version sans liste fixe :
--   1. Texte envoyé au LLM limité à 4500 caractères (vs 2000 initialement).
--      Une première version tentait de repérer par regex une section dédiée
--      "Compétences techniques"/"Profil recherché" au-delà des 2000 premiers
--      caractères plutôt que d'augmenter la coupure -- abandonnée : cas
--      confirmé (Sopra Steria) où le stack technique (SQL, Spark, Pyspark,
--      Kafka...) est cité au fil d'un paragraphe "Vos missions", sans aucun
--      intitulé de section détectable, et donc invisible pour cette regex.
--      Sur les 5 cas réels diagnostiqués aujourd'hui (REXEL, Article 1,
--      Sopra Steria, Alice & Bob, WE DO GOOD), la dernière compétence utile
--      se situe au plus tard vers 4040 caractères -- 4500 couvre tous les
--      cas observés sans dépendre d'une structure d'annonce particulière.
--   2. Liste injectée depuis {{ ref('competences') }} via STRING_AGG : le LLM choisit dans
--      un vocabulaire contraint — plus de normalisation canonique à maintenir manuellement.
--   3. JOIN de validation post-LLM (inner join sur le seed) : filtre dur qui garantit
--      l'absence d'hallucinations sans post-traitement applicatif.
--   4. temperature à 0.1 (vs 0.2) + max_output_tokens à 200 (vs 256) :
--      la tâche de matching est déterministe, on n'a pas besoin de variabilité.
--   5. Incrémental sur (id, competence) : seules les offres de la file d'attente
--      int_france_travail_ai_offres_a_traiter (pas encore traitées) sont envoyées au LLM.

{{
    config(
        materialized='incremental',
        static_analysis='off',
        unique_key=['id', 'competence']
    )
}}

-- Offres à envoyer à Gemini : lues dans une file d'attente déjà filtrée, sans filtre
-- incrémental ici (voir macros/ids_deja_traites.sql).
with offres as (
    select id, nom_plateforme, description
    from {{ ref('int_france_travail_ai_offres_a_traiter') }}
    where a_traiter_competences
),

competences_seed as (
    select string_agg(skill_name, ', ' order by skill_name) as liste
    from {{ ref('competences') }}
),

reponses_llm as (
    select distinct
        id,
        result as competences_csv,
        nom_plateforme
    from ai.generate_text(
        model `prod.gemini_model`,
        (
            select
                concat(
                    """
                    Tu es un expert en recrutement data. Extrait les compétences techniques mentionnées dans l'offre ci-dessous.

                    Règle 1 : réponds UNIQUEMENT avec les compétences séparées par des virgules.
                    Règle 2 : chaque compétence doit être choisie EXACTEMENT dans cette liste (respecte la casse) :
                    """,
                    cs.liste,
                    """

                    Règle 3 : si aucune compétence de la liste n'est présente, réponds uniquement : Non Renseigné
                    Règle 4 : pas de texte avant ni après. Pas de ponctuation finale.

                    Offre :
                    """,
                    substr(o.description, 1, 4500)
                ) as prompt,
                o.id,
                o.nom_plateforme
            from offres as o
            cross join competences_seed as cs
        ),
        struct(
            0.1 as temperature,
            200 as max_output_tokens
        )
    )
),

-- Split CSV -> une ligne par élément. Réponses NULL (appel en erreur) exclues : pas de
-- ligne, l'offre sera retentée au run suivant. Ce filtre sur la sortie de Gemini est sans
-- risque depuis la file d'attente : seules les offres à traiter atteignent Gemini.
-- reponses_llm n'est lu qu'ici : BigQuery ne matérialise pas les CTE, une seconde lecture
-- rappellerait Gemini.
competences_splittees as (
    select distinct
        id,
        nom_plateforme,
        trim(competence) as competence_llm
    from reponses_llm
    left join unnest(split(competences_csv, ',')) as competence
    where competences_csv is not null
),

-- Validation : seules les compétences du seed sont gardées (filtre dur contre les
-- hallucinations résiduelles, 'Non Renseigné' et éléments vides compris).
competences_par_offre as (
    select
        cs.id,
        cs.nom_plateforme,
        array_agg(distinct c.skill_name ignore nulls) as competences
    from competences_splittees as cs
    left join {{ ref('competences') }} as c
        on lower(cs.competence_llm) = lower(c.skill_name)
    group by cs.id, cs.nom_plateforme
)

-- Gemini a répondu sans compétence reconnue : une ligne 'Non Renseigné' marque l'offre
-- comme traitée (sinon elle serait renvoyée à Gemini à chaque run). Filtrée dans
-- int_offres_competences.
select
    id,
    competence,
    nom_plateforme
from competences_par_offre,
    unnest(
        if(coalesce(array_length(competences), 0) = 0, ['Non Renseigné'], competences)
    ) as competence
