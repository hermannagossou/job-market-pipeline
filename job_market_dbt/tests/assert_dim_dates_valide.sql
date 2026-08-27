select
    id_date,
    date_publication,
    annee,
    mois,
    trimestre,
    semaine,
    jour_semaine,
    nom_mois,
    nom_jour
from {{ ref('dim_dates') }}
where annee < 2013
    or mois not between 1 and 12
    or trimestre not between 1 and 4
    or semaine not between 1 and 53
    or jour_semaine not between 1 and 7
    or nom_mois not in ('January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December')
    or nom_jour not in ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')