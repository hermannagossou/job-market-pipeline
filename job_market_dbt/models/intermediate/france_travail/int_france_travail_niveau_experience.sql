-- Extraction et classification du niveau d'expérience requis.
-- Source  : int_france_travail_offres_data (champs niveau_experience et description)
-- Sortie  : une ligne par offre avec le niveau classifié : Junior / Confirmé / Senior / Expert.
--
-- Stratégie : on extrait d'abord le nombre d'années depuis la description (regex "X ans d'expérience"),
-- prioritaire sur le libellé API (moins précis). On plafonne à 15 ans pour filtrer les anomalies.
-- Enfin on mappe vers les 4 classes métier.

with base as (
    select id, niveau_experience, description
    from {{ ref('int_france_travail_merge_ville') }}

),

-- Extraction depuis l'API (libellé ou nombre d'années) et depuis la description (regex).
experience_extraite as (
    select
        id,
        case
            when lower(niveau_experience) = "débutant accepté" then niveau_experience
            when lower(niveau_experience) = "expérience exigée" then niveau_experience
            else regexp_extract(lower(niveau_experience), r"\d{1,2}")
        end as niveau_experience_api,
        regexp_extract(
            lower(description),
            r"(\d{1,2})(?:\s*(?:à\s*\d{1,2})?\s*an\(?s\)?\s*(?:minimum)?\s*(?:d'expérience)?)"
        ) as niveau_experience_extrait
    from base
),

-- Priorité à la valeur extraite de la description si cohérente (≤ 15 ans), sinon on garde l'API.
experience_combinee as (
    select
        id,
        case
            when niveau_experience_extrait is not null
                and safe_cast(niveau_experience_extrait as int64) <= 15
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
