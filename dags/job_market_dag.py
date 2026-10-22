import os
import shutil
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from dotenv import load_dotenv

# ingestion/ vit à la racine du projet, à côté de dags/ (pas dedans) :
# on l'ajoute explicitement au sys.path pour pouvoir réutiliser ses fonctions.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

from ingestion.apis import france_travail as ft
from ingestion.apis import welcome_to_the_jungle as wttj

load_dotenv()

GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
BQ_LOCATION = "US"

# Racine du projet : le repo en local, /usr/local/airflow dans le conteneur Astro.
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# dbt Fusion est un binaire installé dans ~/.local/bin (local) ou dans l'image (Astro).
DBT_BIN = os.getenv("DBT_BIN") or shutil.which("dbt") or str(Path.home() / ".local" / "bin" / "dbt")
DBT_PROJECT_DIR = PROJECT_ROOT / "job_market_dbt"
# Profil dbt versionné (keyfile/projet/dataset lus dans l'environnement) : pas de dépendance à ~/.dbt.
DBT_PROFILES_DIR = PROJECT_ROOT / "include" / "dbt"

# Jour collecté = la VEILLE de la date logique du run. Sous Airflow 3, "@daily" est un
# CronTriggerTimetable : le run de minuit du jour J a data_interval_start = J (et non
# J-1 comme sous Airflow 2). Collecter J à 00:00 ne renvoyait rien (run du 21/10/2026 :
# aucune offre, chargement sauté) ou une demi-journée (run du 20/10 lancé à 07:00 :
# 18 offres). La veille est, elle, une journée complète.
JOUR_COLLECTE = timedelta(days=1)
# Même jour, côté templates Jinja (nom du fichier GCS chargé dans BigQuery).
JOUR_COLLECTE_DS = "{{ (data_interval_start - macros.timedelta(days=1)) | ds }}"


@dag(
    dag_id="job_market_dag",
    schedule="@daily",
    # 28/09/2026 : les offres publiées du 01/09 au 27/09 sont déjà en prod (rattrapage
    # du 27/09). Une nouvelle installation (base Airflow vierge, ex. la VM) ne rattrape
    # donc que les jours manquants. Remonter plus tôt n'apporte rien : les API ne
    # renvoient que les offres encore en ligne (0 offre pour janvier-mai, mesuré le 18/10),
    # et chaque run rejoue un dbt build complet.
    start_date=pendulum.datetime(2026, 9, 28, tz="UTC"),
    catchup=True,
    max_active_runs=1,  # backfill séquentiel : évite de saturer la machine locale (RAM/CPU)
    default_args={
        # Relances utiles pour l'ingestion (API ou réseau momentanément indisponibles).
        # dbt_build les désactive : voir plus bas.
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["france_travail", "welcome_to_the_jungle", "job_market"],
)
def job_market_dag():

    # --- France Travail ---

    @task(task_id="fetch_france_travail")
    def fetch_france_travail(data_interval_start=None):
        return ft.fetch_jobs(data_interval_start - JOUR_COLLECTE)

    @task(task_id="upload_france_travail")
    def upload_france_travail(data, data_interval_start=None):
        return ft.upload_to_gcs(GCS_BUCKET_NAME, data, data_interval_start - JOUR_COLLECTE)

    # Un jour sans offre = pas de fichier uploadé : on saute alors le chargement.
    @task.short_circuit(task_id="has_file_france_travail", ignore_downstream_trigger_rules=False)
    def has_file_france_travail(gcs_uri):
        return bool(gcs_uri)

    load_france_travail_to_bq = GCSToBigQueryOperator(
        task_id="load_france_travail_to_bq",
        bucket=GCS_BUCKET_NAME,
        source_objects=[f"france-travail/offres_{JOUR_COLLECTE_DS}.json"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.raw_france_travail_offres",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_NEVER",
        autodetect=None,  # s'appuie sur le schéma de la table existante
        project_id=GCP_PROJECT_ID,
        location=BQ_LOCATION,
    )

    # --- Welcome to the Jungle ---

    @task(task_id="fetch_wttj")
    def fetch_wttj(data_interval_start=None):
        return wttj.fetch_jobs(data_interval_start - JOUR_COLLECTE)

    @task(task_id="upload_wttj")
    def upload_wttj(data, data_interval_start=None):
        return wttj.upload_to_gcs(GCS_BUCKET_NAME, data, data_interval_start - JOUR_COLLECTE)

    @task.short_circuit(task_id="has_file_wttj", ignore_downstream_trigger_rules=False)
    def has_file_wttj(gcs_uri):
        return bool(gcs_uri)

    load_wttj_to_bq = GCSToBigQueryOperator(
        task_id="load_wttj_to_bq",
        bucket=GCS_BUCKET_NAME,
        source_objects=[f"welcome-to-the-jungle/offres_{JOUR_COLLECTE_DS}.json"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.raw_wttj_offres",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_NEVER",
        autodetect=None,  # s'appuie sur le schéma de la table existante
        project_id=GCP_PROJECT_ID,
        location=BQ_LOCATION,
    )

    # --- Transformation dbt ---
    # dbt Fusion est un binaire compilé (pas de package Python à importer) :
    # on l'invoque via BashOperator, comme n'importe quel outil CLI.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=(
            f"{DBT_BIN} build --target prod "
            f"--project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
        ),
        # BashOperator exécute sinon la commande depuis un dossier temporaire :
        # dbt Fusion résout les CSV des seeds relativement au cwd, pas à --project-dir.
        cwd=str(DBT_PROJECT_DIR),
        # Tourne dès qu'au moins une source a chargé (l'autre peut avoir été sautée).
        trigger_rule="none_failed_min_one_success",
        # Pas de relance automatique : un test en échec échoue pareil 5 minutes plus
        # tard, et chaque relance rejoue tout le dbt build, appels Gemini compris
        # (runs du 18 au 20/10/2026 : 3 tentatives chacun, jusqu'à 3 h par tentative,
        # ~650 000 appels refusés pour quota en une journée). Relancer à la main après
        # correction (Clear sur la tâche).
        retries=0,
    )

    # Les deux sources sont indépendantes : elles s'exécutent en parallèle,
    # dbt attend que les deux chargements BigQuery soient terminés.
    offres_ft = fetch_france_travail()
    gcs_uri_ft = upload_france_travail(offres_ft)
    offres_ft >> gcs_uri_ft
    gcs_uri_ft >> has_file_france_travail(gcs_uri_ft) >> load_france_travail_to_bq

    offres_wttj = fetch_wttj()
    gcs_uri_wttj = upload_wttj(offres_wttj)
    offres_wttj >> gcs_uri_wttj
    gcs_uri_wttj >> has_file_wttj(gcs_uri_wttj) >> load_wttj_to_bq

    [load_france_travail_to_bq, load_wttj_to_bq] >> dbt_build


job_market_dag()
