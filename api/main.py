"""
API Job Market Pipeline 
Lancement : uvicorn main:app --reload --port 8000
Doc auto-générée une fois lancée : http://localhost:8000/docs
"""

import os
from functools import lru_cache
from typing import Literal

import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, Query
from google.cloud import bigquery
from google.oauth2 import service_account

load_dotenv()

GCP_CREDENTIALS_PATH = os.environ["GCP_CREDENTIALS_PATH"]
BQ_DATASET = os.environ["BQ_DATASET"]

app = FastAPI(title="Job Market Pipeline API")

@lru_cache
def get_bq_client() -> bigquery.Client:
    """Client BigQuery mis en cache (créé une seule fois par processus)."""
    credentials = service_account.Credentials.from_service_account_file(GCP_CREDENTIALS_PATH)
    return bigquery.Client(credentials=credentials, project=credentials.project_id)

# ---------------------------------------------------------------------------
# Base filtrée réutilisable 
# ---------------------------------------------------------------------------

def build_filtered_query(
    client: bigquery.Client,
    dataset: str,
    metiers: list[str] | None = None,
    contrats: list[str] | None = None,
    experiences: list[str] | None = None,
    formations: list[str] | None = None,
    regions: list[str] | None = None,
) -> tuple[str, list]:
    conditions = []
    params = []

    def add_filter(colonne: str, valeurs: list[str] | None, nom_param: str) -> None:
        if valeurs:
            conditions.append(f"{colonne} IN UNNEST(@{nom_param})")
            params.append(bigquery.ArrayQueryParameter(nom_param, "STRING", valeurs))

    add_filter("m.nom", metiers, "metiers")
    add_filter("c.contrat", contrats, "contrats")
    add_filter("e.niveau", experiences, "experiences")
    add_filter("fo.niveau", formations, "formations")
    add_filter("l.region", regions, "regions")

    where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    sql = f"""
        WITH offres_filtrees AS (
            SELECT
                f.id_offre,
                f.id_metier, m.nom AS nom_metier,
                f.id_entreprise, ent.nom AS nom_entreprise,
                f.id_secteur, s.nom AS nom_secteur,
                f.id_contrat, c.contrat AS nom_contrat,
                f.id_experience, e.niveau AS niveau_experience,
                f.id_formation, fo.niveau AS niveau_formation,
                f.id_localisation, l.ville, l.departement, l.region,
                f.salaire_min, f.salaire_max,
                d.date_publication
            FROM `{client.project}.{dataset}.fact_offres` f
            LEFT JOIN `{client.project}.{dataset}.dim_secteurs` s 
                ON f.id_secteur = s.id_secteur
            LEFT JOIN `{client.project}.{dataset}.dim_metiers` m 
                ON f.id_metier = m.id_metier
            LEFT JOIN `{client.project}.{dataset}.dim_contrats` c 
                ON f.id_contrat = c.id_contrat
            LEFT JOIN `{client.project}.{dataset}.dim_experiences` e 
                ON f.id_experience = e.id_experience
            LEFT JOIN `{client.project}.{dataset}.dim_formations` fo 
                ON f.id_formation = fo.id_formation
            LEFT JOIN `{client.project}.{dataset}.dim_localisations` l 
                ON f.id_localisation = l.id_localisation
            LEFT JOIN `{client.project}.{dataset}.dim_entreprises` ent 
                ON f.id_entreprise = ent.id_entreprise
            LEFT JOIN `{client.project}.{dataset}.dim_dates` d 
                ON f.id_date = d.id_date
            {where_clause}
        )
    """
    return sql, params


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/health")
def health() -> dict:
    """Vérifie que l'API tourne"""
    return {"status": "ok"}


@app.get("/kpis")
def kpis(
    metiers: list[str] | None = Query(default=None),
    contrats: list[str] | None = Query(default=None),
    experiences: list[str] | None = Query(default=None),
    formations: list[str] | None = Query(default=None),
    regions: list[str] | None = Query(default=None),
) -> dict:
    client = get_bq_client()
    base_sql, params = build_filtered_query(client, BQ_DATASET, metiers, contrats, experiences, formations, regions)
    query = base_sql + """
        SELECT
            COUNT(*) AS total_offres,
            COUNT(DISTINCT id_entreprise) AS nb_entreprises,
            COUNT(DISTINCT id_metier) AS nb_metiers,
            APPROX_QUANTILES(SAFE_DIVIDE(salaire_min + salaire_max, 2), 100)[OFFSET(50)] AS salaire_median
        FROM offres_filtrees
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    df = client.query(query, job_config=job_config).to_dataframe()
    row = df.iloc[0].to_dict()
    return {k: (None if pd.isna(v) else v) for k, v in row.items()}


@app.get("/offres/par-departement")
def offers_by_department(
    metiers: list[str] | None = Query(default=None),
    contrats: list[str] | None = Query(default=None),
    experiences: list[str] | None = Query(default=None),
    formations: list[str] | None = Query(default=None),
    regions: list[str] | None = Query(default=None),
) -> list[dict]:
    client = get_bq_client()
    base_sql, params = build_filtered_query(client, BQ_DATASET, metiers, contrats, experiences, formations, regions)
    query = base_sql + """
        SELECT departement, COUNT(*) AS nb_offres
        FROM offres_filtrees
        WHERE departement IS NOT NULL
        GROUP BY departement
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    df = client.query(query, job_config=job_config).to_dataframe()
    return df.to_dict(orient="records")


# Filtres (pour les menus déroulant du dashboard)
@app.get("/filtres/metiers")
def filter_job_titles() -> list[str]:
    client = get_bq_client()
    query = f"SELECT DISTINCT nom FROM `{client.project}.{BQ_DATASET}.dim_metiers` ORDER BY nom"
    return client.query(query).to_dataframe()["nom"].tolist()


@app.get("/filtres/contrats")
def filter_contract_types() -> list[str]:
    client = get_bq_client()
    query = f"SELECT DISTINCT contrat FROM `{client.project}.{BQ_DATASET}.dim_contrats` ORDER BY contrat"
    return client.query(query).to_dataframe()["contrat"].tolist()


@app.get("/filtres/experiences")
def filter_experience_levels() -> list[str]:
    client = get_bq_client()
    query = f"SELECT DISTINCT niveau FROM `{client.project}.{BQ_DATASET}.dim_experiences` ORDER BY niveau"
    return client.query(query).to_dataframe()["niveau"].tolist()


@app.get("/filtres/formations")
def filter_education_levels() -> list[str]:
    client = get_bq_client()
    query = f"SELECT DISTINCT niveau FROM `{client.project}.{BQ_DATASET}.dim_formations` ORDER BY niveau"
    return client.query(query).to_dataframe()["niveau"].tolist()


@app.get("/filtres/regions")
def filter_regions() -> list[str]:
    client = get_bq_client()
    query = f"SELECT DISTINCT region FROM `{client.project}.{BQ_DATASET}.dim_localisations` ORDER BY region"
    return client.query(query).to_dataframe()["region"].tolist()


# --- Salaires ---
@app.get("/salaires/par-secteur")
def salaries_by_sector(
    metiers: list[str] | None = Query(default=None),
    contrats: list[str] | None = Query(default=None),
    experiences: list[str] | None = Query(default=None),
    formations: list[str] | None = Query(default=None),
    regions: list[str] | None = Query(default=None),
) -> list[dict]:
    client = get_bq_client()
    base_sql, params = build_filtered_query(client, BQ_DATASET, metiers, contrats, experiences, formations, regions)
    query = base_sql + """
        SELECT 
            nom_secteur, 
            AVG(SAFE_DIVIDE(salaire_min + salaire_max, 2)) AS salaire_moyen
        FROM offres_filtrees
        WHERE nom_secteur IS NOT NULL 
            AND salaire_min IS NOT NULL 
            AND salaire_max IS NOT NULL
        GROUP BY nom_secteur
        ORDER BY salaire_moyen DESC
        LIMIT 10
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return client.query(query, job_config=job_config).to_dataframe().to_dict(orient="records")


@app.get("/salaires/par-departement")
def salaries_by_department(
    metiers: list[str] | None = Query(default=None),
    contrats: list[str] | None = Query(default=None),
    experiences: list[str] | None = Query(default=None),
    formations: list[str] | None = Query(default=None),
    regions: list[str] | None = Query(default=None),
) -> list[dict]:
    client = get_bq_client()
    base_sql, params = build_filtered_query(client, BQ_DATASET, metiers, contrats, experiences, formations, regions)
    query = base_sql + """
        SELECT 
            departement, 
            AVG(SAFE_DIVIDE(salaire_min + salaire_max, 2)) AS salaire_moyen, 
            COUNT(*) AS nb_offres
        FROM offres_filtrees
        WHERE departement IS NOT NULL 
            AND salaire_min IS NOT NULL 
            AND salaire_max IS NOT NULL
        GROUP BY departement
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return client.query(query, job_config=job_config).to_dataframe().to_dict(orient="records")


@app.get("/salaires/contrat-experience")
def salaries_by_contract_experience(
    metiers: list[str] | None = Query(default=None),
    contrats: list[str] | None = Query(default=None),
    experiences: list[str] | None = Query(default=None),
    formations: list[str] | None = Query(default=None),
    regions: list[str] | None = Query(default=None),
) -> list[dict]:
    client = get_bq_client()
    base_sql, params = build_filtered_query(client, BQ_DATASET, metiers, contrats, experiences, formations, regions)
    query = base_sql + """
        SELECT
            nom_contrat,
            niveau_experience,
            AVG(SAFE_DIVIDE(salaire_min + salaire_max, 2)) AS salaire_moyen,
            COUNT(*) AS nb_offres
        FROM offres_filtrees
        WHERE nom_contrat IS NOT NULL
            AND niveau_experience IS NOT NULL
            AND salaire_min IS NOT NULL 
            AND salaire_max IS NOT NULL
        GROUP BY nom_contrat, niveau_experience
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return client.query(query, job_config=job_config).to_dataframe().to_dict(orient="records")


@app.get("/salaires/par-competence")
def salaries_by_skill(
    metiers: list[str] | None = Query(default=None),
    contrats: list[str] | None = Query(default=None),
    experiences: list[str] | None = Query(default=None),
    formations: list[str] | None = Query(default=None),
    regions: list[str] | None = Query(default=None),
    limite: int = 30,
) -> list[dict]:
    client = get_bq_client()
    base_sql, params = build_filtered_query(client, BQ_DATASET, metiers, contrats, experiences, formations, regions)
    query = base_sql + f"""
        SELECT
            c.competence AS nom_competence,
            c.categorie AS categorie_competence,
            AVG(SAFE_DIVIDE(o.salaire_min + o.salaire_max, 2)) AS salaire_moyen,
            COUNT(*) AS nb_offres
        FROM offres_filtrees o
        JOIN `{client.project}.{BQ_DATASET}.bridge_offres_competences` b 
            ON o.id_offre = b.id_offre
        JOIN `{client.project}.{BQ_DATASET}.dim_competences` c 
            ON b.id_competence = c.id_competence
        WHERE 
            c.competence IS NOT NULL 
            AND o.salaire_min IS NOT NULL
        GROUP BY c.competence, c.categorie
        ORDER BY nb_offres DESC
        LIMIT {limite}
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return client.query(query, job_config=job_config).to_dataframe().to_dict(orient="records")


@app.get("/salaires/bruts-par-experience")
def raw_salaries_by_experience(
    metiers: list[str] | None = Query(default=None),
    contrats: list[str] | None = Query(default=None),
    experiences: list[str] | None = Query(default=None),
    formations: list[str] | None = Query(default=None),
    regions: list[str] | None = Query(default=None),
) -> list[dict]:
    """Salaires min/max ligne par ligne (non agrégés), pour calculer l'amplitude
    salaire_max - salaire_min côté dashboard."""
    client = get_bq_client()
    base_sql, params = build_filtered_query(client, BQ_DATASET, metiers, contrats, experiences, formations, regions)
    query = base_sql + """
        SELECT niveau_experience, salaire_min, salaire_max
        FROM offres_filtrees
        WHERE niveau_experience IS NOT NULL
            AND salaire_min IS NOT NULL
            AND salaire_max IS NOT NULL
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return client.query(query, job_config=job_config).to_dataframe().to_dict(orient="records")


# --- Compétences, entreprises, villes ---

@app.get("/competences/top")
def top_skills(
    metiers: list[str] | None = Query(default=None),
    contrats: list[str] | None = Query(default=None),
    experiences: list[str] | None = Query(default=None),
    formations: list[str] | None = Query(default=None),
    regions: list[str] | None = Query(default=None),
    limite: int = 15,
) -> list[dict]:
    client = get_bq_client()
    base_sql, params = build_filtered_query(client, BQ_DATASET, metiers, contrats, experiences, formations, regions)
    query = base_sql + f"""
        SELECT 
            c.competence AS nom_competence, 
            c.categorie AS categorie_competence, 
            COUNT(*) AS nb_offres
        FROM offres_filtrees o
        JOIN `{client.project}.{BQ_DATASET}.bridge_offres_competences` b 
            ON o.id_offre = b.id_offre
        JOIN `{client.project}.{BQ_DATASET}.dim_competences` c 
            ON b.id_competence = c.id_competence
        WHERE c.competence IS NOT NULL
        GROUP BY c.competence, c.categorie
        ORDER BY nb_offres DESC
        LIMIT {limite}
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return client.query(query, job_config=job_config).to_dataframe().to_dict(orient="records")


@app.get("/entreprises/top")
def top_companies(
    metiers: list[str] | None = Query(default=None),
    contrats: list[str] | None = Query(default=None),
    experiences: list[str] | None = Query(default=None),
    formations: list[str] | None = Query(default=None),
    regions: list[str] | None = Query(default=None),
    limite: int = 10,
) -> list[dict]:
    client = get_bq_client()
    base_sql, params = build_filtered_query(client, BQ_DATASET, metiers, contrats, experiences, formations, regions)
    query = base_sql + f"""
        SELECT nom_entreprise, COUNT(*) AS nb_offres
        FROM offres_filtrees
        WHERE nom_entreprise IS NOT NULL
        GROUP BY nom_entreprise
        ORDER BY nb_offres DESC
        LIMIT {limite}
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return client.query(query, job_config=job_config).to_dataframe().to_dict(orient="records")


@app.get("/villes/top")
def top_cities(
    metiers: list[str] | None = Query(default=None),
    contrats: list[str] | None = Query(default=None),
    experiences: list[str] | None = Query(default=None),
    formations: list[str] | None = Query(default=None),
    regions: list[str] | None = Query(default=None),
    limite: int = 15,
) -> list[dict]:
    client = get_bq_client()
    base_sql, params = build_filtered_query(client, BQ_DATASET, metiers, contrats, experiences, formations, regions)
    query = base_sql + f"""
        SELECT ville, COUNT(*) AS nb_offres
        FROM offres_filtrees
        WHERE ville IS NOT NULL
        GROUP BY ville
        ORDER BY nb_offres DESC
        LIMIT {limite}
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return client.query(query, job_config=job_config).to_dataframe().to_dict(orient="records")


# --- Évolution ---
@app.get("/offres/evolution")
def offers_trend(
    metiers: list[str] | None = Query(default=None),
    contrats: list[str] | None = Query(default=None),
    experiences: list[str] | None = Query(default=None),
    formations: list[str] | None = Query(default=None),
    regions: list[str] | None = Query(default=None),
) -> list[dict]:
    client = get_bq_client()
    base_sql, params = build_filtered_query(client, BQ_DATASET, metiers, contrats, experiences, formations, regions)
    query = base_sql + """
        SELECT DATE_TRUNC(date_publication, WEEK) AS semaine, COUNT(*) AS nb_offres
        FROM offres_filtrees
        WHERE date_publication IS NOT NULL
        GROUP BY semaine
        ORDER BY semaine
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    df = client.query(query, job_config=job_config).to_dataframe()
    df["semaine"] = df["semaine"].astype(str)
    return df.to_dict(orient="records")


@app.get("/salaires/par-metier-brut")
def raw_salaries_by_job_title(
    metiers: list[str] | None = Query(default=None),
    contrats: list[str] | None = Query(default=None),
    experiences: list[str] | None = Query(default=None),
    formations: list[str] | None = Query(default=None),
    regions: list[str] | None = Query(default=None),
    limite_metiers: int = 10,
) -> list[dict]:
    """Salaires (moyenne min/max) ligne par ligne, pour les N métiers les plus
    représentés — permet à un boxplot de calculer lui-même les quartiles."""
    client = get_bq_client()
    base_sql, params = build_filtered_query(client, BQ_DATASET, metiers, contrats, experiences, formations, regions)
    query = base_sql + f"""
        , top_metiers AS (
            SELECT nom_metier, COUNT(*) AS nb
            FROM offres_filtrees
            WHERE nom_metier IS NOT NULL 
                AND salaire_min IS NOT NULL 
                AND salaire_max IS NOT NULL
            GROUP BY nom_metier
            ORDER BY nb DESC
            LIMIT {limite_metiers}
        )
        SELECT o.nom_metier, SAFE_DIVIDE(o.salaire_min + o.salaire_max, 2) AS salaire
        FROM offres_filtrees o
        JOIN top_metiers tm 
            ON o.nom_metier = tm.nom_metier
        WHERE o.salaire_min IS NOT NULL
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return client.query(query, job_config=job_config).to_dataframe().to_dict(orient="records")


# --- Métiers (juniors, stage/alternance) ---
@app.get("/metiers/postes-juniors")
def junior_positions_by_job_title(
    metiers: list[str] | None = Query(default=None),
    contrats: list[str] | None = Query(default=None),
    experiences: list[str] | None = Query(default=None),
    formations: list[str] | None = Query(default=None),
    regions: list[str] | None = Query(default=None),
    limite: int = 15,
) -> list[dict]:
    """Répartition des métiers parmi les offres de niveau Junior uniquement."""
    client = get_bq_client()
    base_sql, params = build_filtered_query(client, BQ_DATASET, metiers, contrats, experiences, formations, regions)
    query = base_sql + f"""
        SELECT nom_metier, COUNT(*) AS nb_offres
        FROM offres_filtrees
        WHERE niveau_experience = 'Junior' 
        GROUP BY nom_metier
        ORDER BY nb_offres DESC
        LIMIT {limite}
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return client.query(query, job_config=job_config).to_dataframe().to_dict(orient="records")


@app.get("/metiers/stage-alternance")
def internships_by_job_title(
    metiers: list[str] | None = Query(default=None),
    contrats: list[str] | None = Query(default=None),
    experiences: list[str] | None = Query(default=None),
    formations: list[str] | None = Query(default=None),
    regions: list[str] | None = Query(default=None),
    limite: int = 15,
) -> list[dict]:
    """Offres de type Stage ou Alternance, par métier."""
    client = get_bq_client()
    base_sql, params = build_filtered_query(client, BQ_DATASET, metiers, contrats, experiences, formations, regions)
    query = base_sql + f"""
        SELECT nom_metier, nom_contrat, COUNT(*) AS nb_offres
        FROM offres_filtrees
        WHERE nom_contrat IN ('Stage', 'Alternance') AND nom_metier IS NOT NULL
        GROUP BY nom_metier, nom_contrat
        ORDER BY nb_offres DESC
        LIMIT {limite}
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return client.query(query, job_config=job_config).to_dataframe().to_dict(orient="records")


# --- Profil (formation / expérience) ---
@app.get("/profil/repartition")
def profile_distribution(
    colonne: Literal["niveau_experience", "niveau_formation", "nom_contrat"],
    metiers: list[str] | None = Query(default=None),
    contrats: list[str] | None = Query(default=None),
    experiences: list[str] | None = Query(default=None),
    formations: list[str] | None = Query(default=None),
    regions: list[str] | None = Query(default=None),
) -> list[dict]:
    client = get_bq_client()
    base_sql, params = build_filtered_query(client, BQ_DATASET, metiers, contrats, experiences, formations, regions)
    query = base_sql + f"""
        SELECT {colonne} AS niveau, COUNT(*) AS nb_offres
        FROM offres_filtrees
        WHERE {colonne} IS NOT NULL
        GROUP BY {colonne}
        ORDER BY nb_offres DESC
    """
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    return client.query(query, job_config=job_config).to_dataframe().to_dict(orient="records")