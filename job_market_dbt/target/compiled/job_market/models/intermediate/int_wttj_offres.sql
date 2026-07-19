with source_brute as (

    select * from `job-market-de-492514`.`dbt_maxime`.`stg_wttj_offres`

),

-- Une offre WTTJ peut être recapturee plusieurs jours de suite par l'ingestion
-- tant qu'elle reste active/republiee côte WTTJ. On ne garde qu'une ligne par id
-- pour ne pas la compter plusieurs fois dans les analyses.
-- NB : le staging n'expose pas date_chargement (date reelle de scraping), donc on
-- deduplique sur date_publication à defaut. En cas d'egalite stricte (même id, même
-- date_publication, ce qui est le cas courant vu que c'est la date de publication de
-- l'offre et non la date de scraping), le choix entre les lignes candidates reste
-- arbitraire mais deterministe (via to_json_string en tie-break) pour que le modèle
-- soit stable d'un run à l'autre.
source as (

    select *
    from source_brute
    qualify row_number() over (
        partition by id
        order by date_publication desc, to_json_string(source_brute) desc
    ) = 1

),

-- Une offre WTTJ peut avoir plusieurs sites (offices). On duplique une ligne par site
-- reel (ville renseignee) pour ne pas perdre les localisations secondaires.
-- Piège identifie en test : les offres "full remote / ouvert à tous pays" encodent
-- `offices` comme la liste quasi complète des pays du monde, chacun avec city=null
-- (y compris l'entree France). Un UNNEST naïf explose alors 1 offre en ~249 lignes
-- vides. On separe donc : bureaux reels (city non nul) vs offres sans aucun bureau
-- reel, qu'on garde à 1 seule ligne (ville/departement/region = null = "remote").
-- (nom_secteur reste volontairement sur le 1er element de `sectors` pour eviter un
-- produit cartesien offices x sectors ; si le multi-secteur devient un besoin,
-- creer un modèle dedie type bridge/fact_offre_secteur plutôt que d'etendre celui-ci.)
offices_reels as (

    select
        source.*,
        office
    from source
    cross join unnest(json_extract_array(offices, '$')) as office
    where json_value(office, '$.city') is not null
    -- defense contre des entrees office dupliquees dans le JSON source lui-même
    -- (observe en test : une même ville presente 2x dans le tableau offices d'une offre)
    qualify row_number() over (
        partition by id, json_value(office, '$.city')
        order by 1
    ) = 1

),

offres_sans_office_reel as (

    -- offres où aucun office n'a de ville renseignee (100% remote / worldwide)
    select
        source.*,
        cast(null as json) as office
    from source
    where not exists (
        select 1
        from unnest(json_extract_array(source.offices, '$')) as o
        where json_value(o, '$.city') is not null
    )

),

offices_eclates as (

    select * from offices_reels
    union all
    select * from offres_sans_office_reel

),

wttj_intermediate as (

    select
        id,
        metier as nom_metier,
        new_profession_pivot_reference,
        intitule,
        nom_entreprise,

        case
            when type_contrat = 'internship' then 'Stage'
            when type_contrat = 'full_time' then 'CDI'
            when type_contrat = 'freelance' then 'Freelance'
            when type_contrat = 'apprenticeship' then 'Alternance'
            when type_contrat = 'temporary' then 'Contrat temporaire'
            when type_contrat = 'other' then 'Autre'
            when type_contrat = 'vie' then 'VIE'
            when type_contrat = 'graduate_program' then 'Programme jeune diplomes'
            when type_contrat = 'part_time' then 'Temps partiel'
            when type_contrat is null then 'Non Renseigne'
            else 'Non Renseigne'
        end as type_contrat,

        case
            when niveau_formation = 'bac_5' then 'Bac+5'
            when niveau_formation = 'phd' then 'Phd'
            when niveau_formation = 'bac_4' then 'Bac+4'
            when niveau_formation = 'bac_3' then 'Bac+3'
            when niveau_formation = 'bac_2' then 'Bac+2'
            when niveau_formation = 'bac' then 'Bac'
            when niveau_formation = 'no_diploma' then 'Sans diplome'
            when niveau_formation = 'cap' then 'Cap'
            when niveau_formation is null then 'Non Renseigne'
            else 'Non Renseigne'
        end as niveau_formation,

-- niveau_experience = nombre d'annees d'experience minimum requis (donnee
        -- brute WTTJ va de 0 à 15+ ans). Mappe vers des libelles de seniorite pour
        -- s'aligner sur le style France Travail (Confirme/Expert...). Seuils proposes
        -- par defaut (convention courante FR), à valider contre la distribution
        -- reelle des libelles France Travail si possible.
        case
            when niveau_experience is null then 'Non Renseigne'
            when niveau_experience = 0 then 'Debutant'
            when niveau_experience <= 2 then 'Junior'
            when niveau_experience <= 5 then 'Confirme'
            when niveau_experience <= 10 then 'Expert'
            else 'Expert confirme'
        end as niveau_experience,

        niveau_langue,

        trim(json_value(office, '$.city')) as ville,
        trim(json_value(office, '$.district')) as departement,
        trim(json_value(office, '$.local_state')) as region,

        salaire_min,
        salaire_max,

        case
            when lower(salaire_periodicite) = 'yearly' then 'Annuel'
            when lower(salaire_periodicite) = 'monthly' then 'Mensuel'
            when lower(salaire_periodicite) = 'daily' then 'Journalier'
            when lower(salaire_periodicite) = 'hourly' then 'Horaire'
            else 'Non Renseigne'
        end as statut_salaire,

        trim(json_value(sectors, '$[0].name')) as nom_secteur,

        -- textes libres : principaux porteurs de signal pour le matching CV ↔ offre
        -- (description = resume du poste, profile = profil recherche dejà nettoye
        -- de son HTML en staging, missions_cles = missions structurees en JSON)
        description,
        profile,
        missions_cles,

        date_publication,
        nom_plateforme,
        nbre_postes

    from offices_eclates

)

select *
from wttj_intermediate