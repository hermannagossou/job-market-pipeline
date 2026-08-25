-- Déduplication des offres WTTJ et résolution du site principal.
-- Source  : stg_wttj_offres
-- Sortie  : une ligne par offre (doublons éliminés par QUALIFY), avec la ville /
--           département / région du site principal.
--
-- Choix de grain IMPORTANT : une offre WTTJ peut lister plusieurs bureaux (offices).
-- Le contrat int_offres impose 1 ligne = 1 offre (test unique sur id), donc on
-- retient UN SEUL site par offre. Conséquence assumée : les localisations
-- secondaires d'une offre multi-sites ne sont pas représentées.
--
-- Piège observé en test : les offres "full remote / ouvert à tous pays" encodent
-- `offices` comme la liste quasi complète des pays du monde (~249 entrées), toutes
-- avec city = null. D'où le filtre sur city non nul avant de choisir le site.

with stg_wttj_offres as (
    select * from {{ ref('stg_wttj_offres') }}
),

-- Dédoublonnage : en cas d'id dupliqué, on conserve la version la plus récente.
int_wttj_dedup as (
    select *
    from stg_wttj_offres
    qualify row_number() over (partition by id order by date_publication desc) = 1
),

-- Un seul site retenu par offre : on ne garde que les bureaux réels (ville
-- renseignée) et on prend le premier par ordre alphabétique pour rester
-- déterministe d'un run à l'autre.
office_principal as (
    select
        id,
        json_value(office, '$.city') as ville,
        json_value(office, '$.district') as departement,
        -- local_state est souvent NULL alors que state porte la même info
        -- (observé en tout début de session : "local_state":null,"state":"Occitanie"
        -- pour une offre à Toulouse). D'où le repli.
        coalesce(
            json_value(office, '$.local_state'),
            json_value(office, '$.state')
        ) as region
    from int_wttj_dedup
    cross join unnest(json_extract_array(offices, '$')) as office
    where json_value(office, '$.city') is not null
    qualify row_number() over (
        partition by id
        order by json_value(office, '$.city')
    ) = 1
)

select
    w.id,
    w.intitule,
    w.new_profession_pivot_reference,
    w.metier,
    w.nom_entreprise,
    w.type_contrat,
    w.niveau_formation,
    w.niveau_experience,
    w.niveau_langue,
    op.ville,
    op.departement,
    op.region,
    w.salaire_min,
    w.salaire_max,
    w.salaire_periodicite,
    w.sectors,
    w.date_publication,
    w.description,
    w.profile,
    w.missions_cles,
    w.nom_plateforme,
    w.nbre_postes
from int_wttj_dedup as w
left join office_principal as op
    on w.id = op.id