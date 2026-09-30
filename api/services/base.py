"""Clause FROM/JOIN commune : dénormalise `fact_offres` avec ses 8 dimensions.

Réutilisée par tous les services pour éviter de dupliquer les jointures dans
chaque requête. Les dimensions sont de petites tables (quelques dizaines à
quelques milliers de lignes) : les joindre systématiquement n'a pas d'impact
notable sur les coûts/performances BigQuery, et simplifie beaucoup l'écriture
et la maintenance des requêtes par rapport à des jointures conditionnelles.

Colonnes exposées par cette vue (alias) :
    f.id_offre, f.nom_plateforme, f.salaire_min, f.salaire_max,
    f.statut_salaire, f.nbre_postes,
    met.nom AS nom_metier, ent.nom AS nom_entreprise, con.contrat AS type_contrat,
    form.niveau AS niveau_formation, exp.niveau AS niveau_experience,
    loc.ville, loc.departement, loc.region, sec.nom AS nom_secteur,
    dat.date_publication, dat.annee, dat.mois, dat.trimestre,
    dat.nom_mois, dat.nom_jour
"""
from api.core.config import get_settings


def table(name: str) -> str:
    """Retourne un nom de table pleinement qualifié `` `project.dataset.table` ``."""
    settings = get_settings()
    return f"`{settings.bq_project_id}.{settings.bq_dataset}.{name}`"


def base_from_clause() -> str:
    return f"""
    FROM {table('fact_offres')} f
    JOIN {table('dim_metiers')} met ON f.id_metier = met.id_metier
    JOIN {table('dim_entreprises')} ent ON f.id_entreprise = ent.id_entreprise
    JOIN {table('dim_contrats')} con ON f.id_contrat = con.id_contrat
    JOIN {table('dim_formations')} form ON f.id_formation = form.id_formation
    JOIN {table('dim_experiences')} exp ON f.id_experience = exp.id_experience
    JOIN {table('dim_localisations')} loc ON f.id_localisation = loc.id_localisation
    JOIN {table('dim_secteurs')} sec ON f.id_secteur = sec.id_secteur
    JOIN {table('dim_dates')} dat ON f.id_date = dat.id_date
    """
