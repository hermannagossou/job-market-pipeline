"""Configuration commune des tests.

Les tests n'appellent jamais BigQuery (accès aux données simulés), mais l'API
lit sa configuration dès l'import : on fournit des valeurs factices pour qu'ils
tournent sans .env ni identifiants GCP. setdefault : une valeur déjà présente
dans l'environnement est conservée.
"""
import os

os.environ.setdefault("GCP_PROJECT_ID", "projet-de-test")
os.environ.setdefault("BIGQUERY_DATASET", "dataset_de_test")
