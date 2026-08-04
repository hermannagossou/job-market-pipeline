-- Extraction des langues exigées du candidat, via Gemini Flash.
-- Source  : int_france_travail_offres (offres filtrées) + stg_france_travail_offres (description)
-- Sortie  : une ligne par offre avec la liste des langues exigées, séparées par une virgule
--           dans la réponse brute (ex. "Français, Anglais"), ou "Non Renseigné".
-- Paramètres : temperature=0.0 (déterministe), max_output_tokens=30 (plusieurs langues possibles).
--
-- Contrairement à une regex par mots-clés, le modèle distingue une langue EXIGÉE DU CANDIDAT
-- d'une mention de nationalité de l'entreprise, des clients ou du marché — la principale
-- source de faux positifs de l'ancienne regex, en particulier sur "français" (ex. "leader
-- français de...", "marché du recrutement français et européen").

{{
    config(
        materialized='incremental',
        unique_key='id',
        static_analysis='off'
    )
}}

with offres as (
    select id from {{ ref('int_france_travail_offres') }}
),

source as (
    select offres.id, stg.description
    from offres
    left join {{ ref('stg_france_travail_offres') }} as stg on offres.id = stg.id

    {% if is_incremental() %}
    where offres.id not in (select id from {{ this }})
    {% endif %}
)

select
    id,
    trim(result) as langues_ia
from ai.generate_text(
    model `prod.gemini_model`,
    (
        select
            concat(
                """
                Tu es un expert RH qui analyse des offres d'emploi françaises.
                Liste TOUTES les langues explicitement exigées ou souhaitées DU CANDIDAT pour
                CE POSTE (ex. "anglais courant requis", "maîtrise du français et de l'anglais").

                Principe général : ignore toute mention de langue qui ne décrit PAS une
                compétence attendue du candidat — par exemple la nationalité de l'entreprise,
                des clients ou du marché (ex. «leader français de...», «marché du recrutement
                français et européen», «clients grands comptes français et internationaux»).

                Règles :
                1. Si aucune langue n'est explicitement exigée du candidat, réponds Non Renseigné.
                2. Réponds avec les langues en français, séparées par une virgule (ex. «Français,
                   Anglais»), rien d'autre : pas de phrase, pas de ponctuation superflue.

                Description :
                """, description
            ) as prompt,
            id
        from source
    ),
    struct(
        0.0 as temperature,
        30 as max_output_tokens
    )
)
