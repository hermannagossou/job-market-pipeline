import os
import json
import logging
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from google.cloud import storage

# Charger le .env
load_dotenv()

logger = logging.getLogger(__name__)

ALGOLIA_APP_ID = os.getenv("WTTJ_ALGOLIA_APP_ID")
ALGOLIA_API_KEY = os.getenv("WTTJ_ALGOLIA_API_KEY")
ALGOLIA_URL = f"https://{ALGOLIA_APP_ID}-dsn.algolia.net/1/indexes/*/queries"
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

def make_wttj_api_call(min_ts, max_ts, page):
    # Faire appel à l'API Welcome to the Jungle via Algolia

    headers = {
        "x-algolia-api-key": ALGOLIA_API_KEY,
        "x-algolia-application-id": ALGOLIA_APP_ID,
        "Content-Type": "application/json",
        "Referer": "https://www.welcometothejungle.com/",
        "Origin": "https://www.welcometothejungle.com"
    }

    params = "&".join([
        "query=data",
        "hitsPerPage=30",
        f"page={page}",
        f"filters=published_at_timestamp>={min_ts} AND published_at_timestamp<={max_ts} AND offices.country_code:FR"
    ])

    body = {
        "requests": [
            {
                "indexName": "wttj_jobs_production_fr",
                "params": params
            }
        ]
    }

    response = requests.post(
        ALGOLIA_URL,
        headers=headers,
        json=body
    )

    if not response.ok:
        logger.warning(f"Erreur API [{response.status_code}] sur page {page}: {response.text[:200]}")
        return None

    return response.json()["results"][0]

def fetch_jobs(target_date):
    # Récupérer toutes les offres d'hier

    min_ts = int(target_date.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    max_ts = int(target_date.replace(hour=23, minute=59, second=59, microsecond=0).timestamp())

    # Premier appel
    result = make_wttj_api_call(min_ts, max_ts, 0)

    if not result or not result.get("hits"):
        logger.info("Aucune offre trouvée pour cette journée")
        return []

    offres = result.get("hits")
    nb_pages = result.get("nbPages")
    nb_hits = result.get("nbHits")

    logger.info(f"Page 0 — {len(offres)} offres récupérées sur {nb_hits} au total ({nb_pages} pages)")

    # Pages suivantes
    for page in range(1, nb_pages):
        result = make_wttj_api_call(min_ts, max_ts, page)
        if not result or not result.get("hits"):
            break
        offres.extend(result.get("hits"))
        logger.info(f"Page {page} — {len(result.get('hits'))} offres récupérées")

    return offres

def upload_to_gcs(bucket_name, data, target_date):

    if not data:
        logger.info("Aucune offre à uploader")
        return

    # Définir le nom du fichier à uploader
    date_str = target_date.strftime("%Y-%m-%d")
    blob_name = f"welcome-to-the-jungle/offres_{date_str}.json"

    # Créer le client GCS
    client = storage.Client()

    # Cibler le bucket
    bucket = client.bucket(bucket_name)

    # Créer le fichier au sein du bucket
    blob = bucket.blob(blob_name)

    # Construire le ndJSON
    algolia_internal_fields = {"_highlightResult", "_snippetResult", "_rankingInfo"}
    date_chargement = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ndjson_content = "\n".join([
        json.dumps(
            {k: v for k, v in offre.items() if k not in algolia_internal_fields} | {"date_chargement": date_chargement},
            ensure_ascii=False
        )
        for offre in data
    ])

    # Uploader le ndJSON
    blob.upload_from_string(
        data=ndjson_content,
        content_type="application/json"
    )

    logger.info(f"{len(data)} offres uploadées vers gs://{bucket_name}/{blob_name}")

def run():
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    offres = fetch_jobs(yesterday)
    upload_to_gcs(GCS_BUCKET_NAME, data=offres, target_date=yesterday)

if __name__ == "__main__":
    run()
