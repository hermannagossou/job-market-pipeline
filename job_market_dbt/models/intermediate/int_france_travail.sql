-- Normalisation des offres France Travail vers le schéma commun (aligné sur
-- int_wttj_offres : mêmes noms de colonnes, même casse, mêmes tranches
-- niveau_experience pour rendre les deux sources comparables).
--
-- ATTENTION points de vigilance connus :
--   - niveau_formation : aucun champ source exploité pour l'instant (le JSON brut
--     a bien un champ `formations`, mais c'est une liste de codes/libellés à
--     mapper qui n'a pas encore été fait). Laissé à 'Non Renseigne' partout, ce
--     n'est pas un manque de données réel mais un manque de mapping.
--   - statut_salaire = périodicité (Annuel/Mensuel/Horaire), extraite par regex
--     du texte libre de l'API. Ce n'est PAS une info de fiabilité du salaire
--     (estimé/déclaré) — confirmé absent de la source actuellement exploitée.
--   - niveau_experience est reconstruit depuis experienceLibelle (texte libre
--     type "Débutant accepté" / "Expérience exigée de 2 An(s)") vers un nombre
--     d'années brut, puis catégorisé avec EXACTEMENT les mêmes seuils que
--     int_wttj_offres (Débutant/Junior/Confirmé/Expert/Expert confirmé) pour que
--     les deux sources restent comparables. Seuils par défaut, à valider.
--   - la jointure géo sur code_commune peut laisser ville/departement/region à
--     NULL si le code commune brut est absent/mal formé côté source (offres
--     100% télétravail sans lieu précis, ou code non trouvé dans le référentiel
--     INSEE).

with source_brute as (

    select * from {{ ref('stg_france_travail_offres') }}

),

-- Défense par cohérence avec int_wttj_offres : une offre pourrait en théorie être
-- recapturée plusieurs jours de suite. Pas confirmé sur France Travail (le script
-- ne récupère que les offres de la veille), mais on sécurise quand même.
source as (

    select *
    from source_brute
    qualify row_number() over (
        partition by id
        order by date_publication desc, to_json_string(source_brute) desc
    ) = 1

),

geo as (

    select * from {{ ref('stg_ville_dept_reg') }}

),

-- Repli pour les offres sans code_commune (mais avec code_departement) : au moins
-- récupérer département/région sans la ville précise. Observé en test : ~9,5% des
-- offres France Travail n'ont pas de code_commune en source, dont une partie a
-- quand même un code_departement exploitable dans le libellé brut.
-- LIMITE CONNUE (héritée du staging, non corrigée ici) : code_departement est
-- extrait par regex `^[0-9]{2,3}` sur un texte libre, sans zero-padding ni gestion
-- de la Corse (2A/2B) — un département comme le "01" pourrait être extrait "1" et
-- ne pas matcher le référentiel INSEE (qui utilise "01"). Ce repli ne rattrape donc
-- pas 100% des cas, seulement ceux où le format coïncide.
geo_departements as (

    select distinct
        code_departement,
        nom_departement,
        nom_region
    from geo

),

-- Le champ `formations` est un tableau JSON : plusieurs formations candidates par
-- offre, chacune avec un niveau (niveauLibelle) et une exigence ('E'=exigé,
-- 'S'=souhaité). On choisit 1 formation par offre : priorité à une formation
-- EXIGEE sur une SOUHAITEE, puis au niveau le plus élevé en cas d'égalité.
-- Granularité plus grossière que WTTJ : "Bac+3, Bac+4 ou équivalents" mélange les
-- deux niveaux en une seule valeur source, d'où la catégorie fusionnée Bac+3/Bac+4
-- plutôt qu'un choix arbitraire entre les deux.
formations_normalisees as (

    select
        source.id,

        case
            -- 'Bac+5' ici englobe aussi le niveau doctorat/Bac+6+ (bucket France
            -- Travail plus large que WTTJ, qui isole 'Phd' séparément) — préféré à un
            -- libellé distinct pour rester comparable avec int_wttj_offres.
            when json_value(f, '$.niveauLibelle') like 'Bac+5%' then 'Bac+5'
            when json_value(f, '$.niveauLibelle') like 'Bac+3%' then 'Bac+3/Bac+4'
            when json_value(f, '$.niveauLibelle') like 'Bac+2%' then 'Bac+2'
            when json_value(f, '$.niveauLibelle') like 'CAP%' then 'Cap'
            when json_value(f, '$.niveauLibelle') like 'Bac%' then 'Bac'
            else null
        end as niveau_formation,

        case
            when json_value(f, '$.niveauLibelle') like 'Bac+5%' then 4
            when json_value(f, '$.niveauLibelle') like 'Bac+3%' then 3
            when json_value(f, '$.niveauLibelle') like 'Bac+2%' then 2
            when json_value(f, '$.niveauLibelle') like 'Bac%' then 1
            else 0
        end as niveau_rank,

        case json_value(f, '$.exigence')
            when 'E' then 2
            when 'S' then 1
            else 0
        end as exigence_rank

    from source
    cross join unnest(json_extract_array(source.formations, '$')) as f
    where json_value(f, '$.niveauLibelle') is not null
    qualify row_number() over (
        partition by source.id
        order by exigence_rank desc, niveau_rank desc
    ) = 1

),

france_travail_intermediate as (

    select
        source.id,
        source.libelle_rome as nom_metier,
        -- code_rome = taxonomie officielle des métiers (ROME). Conservé comme clé de
        -- rapprochement entre offres similaires et point de jointure futur avec le
        -- référentiel de compétences (seed competences.csv).
        source.code_rome,
        source.intitule,
        source.nom_entreprise,

        case
            when source.type_contrat like 'CDI%' then 'CDI'
            when source.type_contrat like 'CDD%' then 'Contrat temporaire'
            when source.type_contrat like 'Int_rim%' then 'Contrat temporaire'
            when source.type_contrat like 'Stage%' then 'Stage'
            when source.type_contrat like 'Apprentissage%' then 'Alternance'
            when source.type_contrat like 'Professionnalisation%' then 'Alternance'
            when source.type_contrat like '%Mois' then 'Contrat temporaire'
            when source.type_contrat like 'VIE%' then 'VIE'
            when source.type_contrat like 'Profession lib_rale%' then 'Freelance'
            when source.type_contrat like 'Profession commerciale%' then 'Profession commerciale'
            when source.type_contrat is null then 'Non Renseigne'
            else 'Autre'
        end as type_contrat,

        -- pas de champ source exploité pour l'instant (voir `formations` en JSON brut).
        -- Valeur littérale 'Non Renseigne' plutôt que NULL, pour rester cohérent avec
        -- int_wttj_offres et éviter un comportement différent dans les filtres/GROUP BY.
        coalesce(fn.niveau_formation, 'Non Renseigne') as niveau_formation,

        case
            when source.niveau_experience is null then 'Non Renseigne'
            when source.niveau_experience like '%D_butant%' then 'Debutant'
            when regexp_contains(source.niveau_experience, r'\d+\s*Mois') then 'Debutant'
            when regexp_contains(source.niveau_experience, r'\d+\s*An\(s\)') then
                case
                    when safe_cast(regexp_extract(source.niveau_experience, r'(\d+)\s*An\(s\)') as int64) <= 2
                        then 'Junior'
                    when safe_cast(regexp_extract(source.niveau_experience, r'(\d+)\s*An\(s\)') as int64) <= 5
                        then 'Confirme'
                    when safe_cast(regexp_extract(source.niveau_experience, r'(\d+)\s*An\(s\)') as int64) <= 10
                        then 'Expert'
                    else 'Expert confirme'
                end
            else 'Non Renseigne'
        end as niveau_experience,

        geo.nom_ville as ville,
        coalesce(geo.nom_departement, gd.nom_departement) as departement,
        coalesce(geo.nom_region, gd.nom_region) as region,

        source.salaire_min,
        source.salaire_max,

        case
            when source.salaire_periodicite = 'Annuel' then 'Annuel'
            when source.salaire_periodicite = 'Mensuel' then 'Mensuel'
            when source.salaire_periodicite = 'Horaire' then 'Horaire'
            else 'Non Renseigne'
        end as statut_salaire,

        source.nom_secteur,

        -- texte libre : principal porteur de signal pour le matching CV ↔ offre
        -- (compétences, stack technique, missions attendues)
        source.description,

        source.date_publication,
        source.nom_plateforme,
        source.nbre_postes

    from source
    left join geo
        on source.code_commune = geo.code_commune
    left join geo_departements as gd
        on source.code_departement = gd.code_departement
    left join formations_normalisees as fn
        on source.id = fn.id

)

select *
from france_travail_intermediate