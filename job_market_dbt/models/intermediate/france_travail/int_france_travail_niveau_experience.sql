-- Extraction et classification du niveau d'expérience requis.
-- Source  : int_france_travail_merge_ville (champ niveau_experience) + int_france_travail_ai_niveau_experience
--           (nombre d'années extrait par Gemini depuis la description)
-- Sortie  : une ligne par offre avec le niveau classifié : Junior / Confirmé / Senior / Expert.
--
-- Stratégie : priorité au nombre d'années extrait par Gemini (comprend le contexte, ignore les
-- mentions d'ancienneté de l'entreprise), fallback sur le libellé API si absent (Non Renseigné)
-- ou non numérique. Enfin on mappe vers les 4 classes métier.

with base as (
    select id, niveau_experience
    from {{ ref('int_france_travail_merge_ville') }}

),

ai_extraction as (
    select id, niveau_experience_ia
    from {{ ref('int_france_travail_ai_niveau_experience') }}
),

-- Combinaison de l'API (libellé ou nombre d'années) et de l'extraction Gemini.
experience_extraite as (
    select
        base.id,
        case
            when lower(base.niveau_experience) like "%débutant accepté%" then "débutant accepté"
            when lower(base.niveau_experience) like "%expérience exigée%" then "expérience exigée"
            -- "12 Mois", "6 Mois"... : toujours < 1 an dans les données observées, donc Junior (0)
            when lower(base.niveau_experience) like "%mois%" then "0"
            else regexp_extract(lower(base.niveau_experience), r"\b(\d{1,2})\b")
        end as niveau_experience_api,
        ai_extraction.niveau_experience_ia as niveau_experience_extrait
    from base
    left join ai_extraction on base.id = ai_extraction.id
),

-- Priorité à l'extraction Gemini si elle a renvoyé un nombre exploitable (le modèle sait
-- déjà écarter les mentions d'ancienneté d'entreprise, donc pas de plafond arbitraire ici) ;
-- sinon (Non Renseigné) on retombe sur le libellé API.
experience_combinee as (
    select
        id,
        case
            when safe_cast(niveau_experience_extrait as int64) is not null
                then niveau_experience_extrait
            else niveau_experience_api
        end as niveau_experience_brut
    from experience_extraite
)

select
    id,
    case
        when lower(niveau_experience_brut) = "débutant accepté" then "Junior"
        when safe_cast(niveau_experience_brut as int64) <= 2 then "Junior"
        when safe_cast(niveau_experience_brut as int64) between 3 and 5 then "Confirmé"
        when safe_cast(niveau_experience_brut as int64) between 6 and 10 then "Senior"
        when safe_cast(niveau_experience_brut as int64) > 10 then "Expert"
        when lower(niveau_experience_brut) = "expérience exigée" then "Expert"
        else "Non Renseigné"
    end as niveau_experience
from experience_combinee
