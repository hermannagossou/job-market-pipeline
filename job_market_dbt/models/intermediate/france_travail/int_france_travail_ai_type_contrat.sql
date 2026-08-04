-- Classification du type de contrat, via Gemini Flash.
-- Source  : int_france_travail_merge_ville (toutes les offres)
-- Sortie  : une ligne par offre avec le type de contrat classifié parmi CDI/CDD/Stage/
--           Alternance/Freelance/Intérim, ou "Non Renseigné" si non identifiable.
-- Paramètres : temperature=0.0 (déterministe), max_output_tokens=15 (réponse courte attendue).
--
-- Contrairement à une regex par mots-clés, le modèle distingue le contrat qui régit CE poste
-- précis des mentions non pertinentes ailleurs dans le texte (façon dont une expérience passée
-- a été acquise, sens non contractuel d'un mot comme "alternance" - rotation d'équipe -, ou
-- encadrement de personnes employées sous un autre type de contrat). Intitulé + description
-- transmis en entier (non tronqués) : la déclaration du contrat peut arriver n'importe où dans
-- le texte, et apparaît souvent dans l'intitulé pour les alternances/stages (ex. "(ALTERNANCE)
-- - Ingénieur data") sans être répétée dans le corps de la description.

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
    -- Mapping explicite plutôt que initcap() : CDI/CDD sont des acronymes qu'initcap()
    -- casserait ("Cdi", "Cdd"). Toute réponse inattendue retombe sur "Non Renseigné".
    case lower(trim(result))
        when 'cdi' then 'CDI'
        when 'cdd' then 'CDD'
        when 'stage' then 'Stage'
        when 'alternance' then 'Alternance'
        when 'freelance' then 'Freelance'
        when 'intérim' then 'Intérim'
        when 'interim' then 'Intérim'
        else 'Non Renseigné'
    end as type_contrat_ia
from ai.generate_text(
    model `prod.gemini_model`,
    (
        select
            concat(
                """
                Tu es un expert RH qui analyse des offres d'emploi françaises.
                Détermine le TYPE DE CONTRAT sous lequel la personne recrutée pour CE POSTE
                précis travaillera, parmi : CDI, CDD, Stage, Alternance, Freelance, Intérim.

                Principe général : ne retiens que le contrat proposé POUR CE POSTE. Ignore
                toute mention d'un type de contrat qui ne concerne pas l'embauche visée ici —
                par exemple une façon dont une expérience passée a pu être acquise, un mot
                utilisé dans un sens non contractuel, ou le fait que le poste consiste à
                encadrer des personnes employées sous un autre type de contrat.

                Règles :
                1. Si plusieurs types sont explicitement proposés comme options pour ce poste,
                   réponds avec celui présenté comme l'offre principale/par défaut.
                2. Si les indices disponibles sont compatibles avec plusieurs catégories
                   différentes sans que le texte ne permette de trancher clairement entre
                   elles (ex. un profil étudiant qui pourrait correspondre à un Stage OU une
                   Alternance ; une durée de mission qui pourrait correspondre à un CDD OU un
                   Intérim sans autre précision), ne choisis pas au hasard — réponds Non
                   Renseigné.
                3. N'invente rien : si le type de contrat de CE poste n'est pas indiqué de
                   façon claire et explicite dans le texte, ne devine pas à partir d'indices
                   indirects (ex. l'activité de l'agence de recrutement, le profil du
                   candidat recherché) — réponds Non Renseigné plutôt qu'une supposition.
                4. Une durée précisément chiffrée (ex. "12 mois") indique un contrat à durée
                   déterminée, donc PAS un CDI — mais ne permet pas à elle seule de choisir
                   entre CDD et Intérim si rien d'autre ne les distingue (voir règle 2). Des
                   qualificatifs vagues sur la nature du poste (ex. "long terme", "stable",
                   "durable", "pérenne") ne sont eux jamais une preuve suffisante de CDI —
                   seule une déclaration explicite (CDI, "durée indéterminée") doit être
                   retenue comme telle.
                5. Si le contrat indiqué ne correspond clairement à AUCUNE des catégories
                   listées (ex. un type de contrat différent, non couvert ici — même s'il
                   ressemble à l'une d'elles par le nom, comme le CDIC, un contrat de chantier
                   légalement différent du CDD), ne le force pas dans la catégorie qui te
                   semble la plus proche — réponds Non Renseigné.
                6. Réponds SEULEMENT avec l'une de ces valeurs : CDI, CDD, Stage, Alternance,
                   Freelance, Intérim, ou Non Renseigné si le contrat n'est pas identifiable.
                   Rien d'autre, pas de ponctuation.

                Intitulé du poste :
                """, intitule, """

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
