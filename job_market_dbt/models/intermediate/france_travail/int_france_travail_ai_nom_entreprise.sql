-- Extraction du nom d'entreprise via Gemini Flash pour les offres sans nom_entreprise.
-- Source  : int_france_travail_offres_data, filtrée sur nom_entreprise IS NULL
-- Sortie  : une ligne par offre anonyme avec le nom extrait de la description,
--           ou "Non Renseigné" si non identifiable.
-- Paramètres : temperature=0.0 (déterministe), max_output_tokens=30 (nom court attendu).

{{
    config(
        materialized='incremental',
        unique_key='id',
        static_analysis='off'
    )
}}

-- Offres à envoyer à Gemini : lues dans une file d'attente déjà filtrée, sans filtre
-- incrémental ici (voir macros/ids_deja_traites.sql). Un appel en erreur (réponse NULL)
-- est retenté au run suivant ; sa ligne est alors remplacée (unique_key).
with source as (
    select * from {{ ref('int_france_travail_ai_a_traiter') }}
    where a_traiter_entreprise
)

select
    id,
    initcap(trim(result)) as nom_entreprise
from ai.generate_text(
    model `prod.gemini_model`,
    (
        select
            concat(
                """
                Extrait uniquement le nom de l'entreprise depuis cette description d'offre d'emploi francaise.
                Règles :
                1. Réponds avec seulement le nom de l'entreprise, rien d'autre.
                2. Pas de guillemets, pas de ponctuation, pas de phrase.
                3. Si le nom n'est pas clairement identifiable, reponds exactement : Non Renseigné
                \n\nDescription :\n
                """,
                substr(description, 1, 1500)
            ) as prompt,
            id
        from source
    ),
    struct(
        0.0 as temperature,
        30 as max_output_tokens
    )
)