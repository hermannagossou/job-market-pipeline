-- Extraction du nombre d'années d'expérience exigées du candidat, via Gemini Flash.
-- Source  : int_france_travail_merge_ville (toutes les offres)
-- Sortie  : une ligne par offre avec le nombre d'années extrait (brut, non plafonné),
--           ou "Non Renseigné" si la description ne précise aucune exigence chiffrée.
-- Paramètres : temperature=0.0 (déterministe), max_output_tokens=20 (réponse courte attendue).
--
-- Contrairement à une regex, le modèle sait distinguer l'expérience exigée du CANDIDAT
-- des mentions d'ancienneté de l'ENTREPRISE/agence ("Fort de 20 ans d'expérience, notre
-- cabinet...", "leader depuis 15 ans dans le secteur"), qui sont ignorées explicitement
-- dans le prompt.

{{
    config(
        materialized='incremental',
        unique_key='id',
        static_analysis='off'
    )
}}

with source as (
    select * from {{ ref('int_france_travail_merge_ville') }}

    {% if is_incremental() %}
    where id not in (select id from {{ this }})
    {% endif %}
)

select
    id,
    trim(result) as niveau_experience_ia
from ai.generate_text(
    model `prod.gemini_model`,
    (
        select
            concat(
                """
                Tu es un expert RH qui analyse des offres d'emploi françaises.
                Extrait UNIQUEMENT le nombre d'années d'expérience professionnelle exigées
                ou souhaitées du CANDIDAT pour ce poste.

                Règles :
                1. Ignore complètement les mentions de l'ancienneté de l'ENTREPRISE, de
                   l'agence ou du cabinet de recrutement (ex. «Fort de 20 ans d'expérience,
                   notre cabinet...», «leader depuis 15 ans dans le secteur») : ce n'est pas
                   le profil recherché, uniquement une présentation de l'employeur.
                2. Si une fourchette est donnée pour le candidat (ex. «3 à 5 ans»), réponds
                   avec le minimum de la fourchette.
                3. Si aucune expérience chiffrée n'est exigée du candidat, réponds exactement :
                   Non Renseigné
                4. Réponds SEULEMENT avec un nombre entier (ex. 5), sans unité ni ponctuation,
                   ou avec «Non Renseigné». Rien d'autre.

                Description :
                """, substr(description, 1, 2000)
            ) as prompt,
            id
        from source
    ),
    struct(
        0.0 as temperature,
        20 as max_output_tokens
    )
)
