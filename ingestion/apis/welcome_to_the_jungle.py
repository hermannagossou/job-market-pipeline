import os
import json
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from google.cloud import storage

# Charger le .env
load_dotenv()

ALGOLIA_APP_ID = os.getenv("WTTJ_ALGOLIA_APP_ID")
ALGOLIA_API_KEY = os.getenv("WTTJ_ALGOLIA_API_KEY")
ALGOLIA_URL = f"https://{ALGOLIA_APP_ID}-dsn.algolia.net/1/indexes/*/queries"

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
        print(f"Erreur API [{response.status_code}] sur page {page}: {response.text[:200]}")
        return None

    return response.json()["results"][0]

def fetch_jobs():
    # Récupérer toutes les offres d'hier

    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    min_ts = int(yesterday.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    max_ts = int(yesterday.replace(hour=23, minute=59, second=59, microsecond=0).timestamp())

    # Premier appel
    result = make_wttj_api_call(min_ts, max_ts, 0)

    if not result or not result.get("hits"):
        print("Aucune offre trouvée pour cette journée")
        return None

    offres = result.get("hits")
    nb_pages = result.get("nbPages")
    nb_hits = result.get("nbHits")

    print(f"Page 0 — {len(offres)} offres récupérées sur {nb_hits} au total ({nb_pages} pages)")

    # Pages suivantes
    for page in range(1, nb_pages):
        result = make_wttj_api_call(min_ts, max_ts, page)
        if not result or not result.get("hits"):
            break
        offres.extend(result.get("hits"))
        print(f"Page {page} — {len(result.get('hits'))} offres récupérées")

    return offres

def upload_to_gcs(bucket_name, data):

    if not data:
        print("Aucune offre à uploader")
        return

    # Définir le nom du fichier à uploader
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")
    blob_name = f"welcome-to-the-jungle/offres_{date_str}.json"

    # Créer le client GCS
    client = storage.Client()

    # Cibler le bucket
    bucket = client.bucket(bucket_name)

    # Créer le fichier au sein du bucket
    blob = bucket.blob(blob_name)

    # Construire le ndJSON
    ndjson_content = "\n".join([json.dumps(offre, ensure_ascii=False) for offre in data])

    # Uploader le ndJSON
    blob.upload_from_string(
        data=ndjson_content,
        content_type="application/json"
    )

    print(f"{len(data)} offres uploadées vers gs://{bucket_name}/{blob_name}")

if __name__ == "__main__":
    upload_to_gcs("job-market-raw", data=fetch_jobs())
