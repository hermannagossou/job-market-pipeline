-- Classification du niveau de formation minimum requis, via Gemini Flash.
-- Source  : int_france_travail_merge_ville (toutes les offres)
-- Sortie  : une ligne par offre avec le niveau classifié parmi Bac+2/Bac+3/Bac+4/Bac+5/Doctorat,
--           ou "Non Renseigné" si aucun niveau n'est identifiable.
-- Paramètres : temperature=0.0 (déterministe), max_output_tokens=15 (réponse courte attendue).
--
-- Contrairement à une regex par mots-clés, le modèle comprend les équivalences de diplômes
-- (école d'ingénieur, BUT, grande école...) sans qu'il faille énumérer chaque synonyme, et sait
-- distinguer un vrai diplôme ("Master en informatique") d'une mention métier sans rapport avec
-- le niveau d'études ("Master Data Management", "Scrum Master"). Le Doctorat (Bac+8) est traité
-- comme un niveau à part, jamais confondu avec Bac+5.
--
-- Description transmise en entier (non tronquée) : la section "profil recherché" où figure le
-- niveau de formation arrive souvent après une longue présentation d'entreprise et la liste des
-- missions, donc au-delà des premiers ~2000 caractères. Les descriptions sont déjà plafonnées
-- à 5000 caractères en amont, donc le coût/volume envoyé au modèle reste borné.

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
    initcap(trim(result)) as niveau_formation_ia
from ai.generate_text(
    model `prod.gemini_model`,
    (
        select
            concat(
                """
                Tu es un expert RH qui analyse des offres d'emploi françaises.
                Détermine le niveau de formation MINIMUM requis pour CE poste, parmi :
                Bac+2, Bac+3, Bac+4, Bac+5, Doctorat.

                Correspondances usuelles :
                - Bac+2 : BTS, DUT
                - Bac+3 : Licence, Bachelor, BUT
                - Bac+4 : Master 1, M1
                - Bac+5 : Master, Master 2, M2, diplôme d'ingénieur, école d'ingénieur,
                  grande école, MBA
                - Doctorat : doctorat, thèse, PhD, Bac+6 à Bac+9 (et au-delà)

                Règles :
                1. Le Doctorat est un niveau À PART, distinct de Bac+5 : ne le classe JAMAIS
                   comme Bac+5.
                2. Un simple mot «diplôme» seul, sans précision, ne signifie PAS Bac+5.
                3. Si une fourchette est donnée (ex. «Bac+3 à Bac+5»), réponds avec le niveau
                   MAXIMUM de la fourchette.
                4. Ignore les mentions qui contiennent ces mots mais ne décrivent PAS un niveau
                   d'études du candidat (ex. «Master Data Management», «Scrum Master» : ce sont
                   des sujets ou des rôles métier, pas des diplômes).
                5. Réponds SEULEMENT avec l'une de ces valeurs : Bac+2, Bac+3, Bac+4, Bac+5,
                   Doctorat, ou Non Renseigné si aucun niveau n'est identifiable. Rien d'autre,
                   pas de ponctuation.

                Description :
                """, description
            ) as prompt,
            id
        from source
    ),
    struct(
        0.0 as temperature,
        15 as max_output_tokens
    )
)
