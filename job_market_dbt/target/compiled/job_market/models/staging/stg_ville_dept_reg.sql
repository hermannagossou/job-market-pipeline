-- Table de référence géographique : une ligne par commune avec son département et sa région.
-- Source  : seeds INSEE (liste_communes, liste_departements, liste_regions)
-- Sortie  : jointure triple commune ↔ département ↔ région, filtrée sur les types COM et ARM
--           (communes ordinaires et arrondissements municipaux Paris/Lyon/Marseille).

with villes as (
    select * from `job-market-de-492514`.`dbt_maxime`.`liste_communes`
),

departements as (
    select * from `job-market-de-492514`.`dbt_maxime`.`liste_departements`
),

regions as (
    select * from `job-market-de-492514`.`dbt_maxime`.`liste_regions`
)

select
    trim(cast(v.COM as string)) as code_commune,
    trim(cast(v.DEP as string)) as code_departement,
    trim(cast(v.REG as string)) as code_region,
    trim(cast(v.LIBELLE as string)) as nom_ville,
    trim(cast(d.LIBELLE as string)) as nom_departement,
    trim(cast(r.LIBELLE as string)) as nom_region
from villes as v
left join departements as d
    on v.DEP = d.DEP
    and v.REG = d.REG
left join regions as r
    on v.REG = r.REG
where v.typecom in ('ARM', 'COM')