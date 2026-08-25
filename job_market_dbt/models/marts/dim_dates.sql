-- Dimension dates : une ligne par date de publication, avec décomposition temporelle complète.
-- Clé : id_date (surrogate key sur date_publication)

with source as (
    select * from {{ ref('int_offres') }}
)

select distinct
    {{ dbt_utils.generate_surrogate_key(['date_publication']) }} as id_date,
    date_publication,
    extract(year from date_publication) as annee,
    extract(month from date_publication) as mois,
    extract(quarter from date_publication) as trimestre,
    extract(week from date_publication) as semaine,
    extract(dayofweek from date_publication) as jour_semaine,
    format_date('%B', date_publication) as nom_mois,
    format_date('%A', date_publication) as nom_jour
from source