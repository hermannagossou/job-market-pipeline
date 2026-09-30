"""Client BigQuery partagé + exécution de requêtes paramétrées.

Toutes les requêtes de l'API passent par `run_query()`, qui impose l'usage de
paramètres nommés (jamais de f-string injectée directement dans les valeurs
utilisateur) et centralise la gestion des erreurs.
"""
from __future__ import annotations

import logging

from google.cloud import bigquery
from google.oauth2 import service_account

from api.core.config import get_settings

logger = logging.getLogger(__name__)

_client: bigquery.Client | None = None


class BigQueryQueryError(RuntimeError):
    """Levée quand une requête BigQuery échoue (réseau, permissions, SQL invalide...)."""


def get_client() -> bigquery.Client:
    """Retourne un client BigQuery unique, réutilisé sur toute la durée de vie du process."""
    global _client
    if _client is None:
        settings = get_settings()
        credentials = None
        if settings.google_application_credentials:
            credentials = service_account.Credentials.from_service_account_file(
                settings.google_application_credentials
            )
        _client = bigquery.Client(
            project=settings.bq_project_id,
            credentials=credentials,
        )
    return _client

def run_query(
    sql: str,
    params: list[bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter] | None = None,
) -> list[dict]:
    """Exécute une requête paramétrée et retourne les lignes sous forme de liste de dicts.

    Args:
        sql: requête SQL, avec des placeholders `@nom_param` pour toute valeur variable.
        params: liste de `bigquery.ScalarQueryParameter` correspondant aux placeholders.

    Raises:
        BigQueryQueryError: si la requête échoue pour quelque raison que ce soit.
    """
    try:
        client = get_client()
        job_config = bigquery.QueryJobConfig(query_parameters=params or [])
        result = client.query(sql, job_config=job_config).result()
    except Exception as exc:  # noqa: BLE001 - on centralise volontairement ici
        logger.exception("Échec de la requête BigQuery (connexion, auth, ou SQL)")
        raise BigQueryQueryError(str(exc)) from exc
    return [dict(row) for row in result]
