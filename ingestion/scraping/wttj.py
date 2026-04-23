import os
import re
import json
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from google.cloud import storage

# Charger le .env
load_dotenv()

def make_request_wttj(url, num_page):
    try:
        headers = {
            "X-Algolia-Api-Key": os.getenv("AGOLIA_WTTJ_API_KEY"),
            "X-Algolia-Application-Id": os.getenv("AGOLIA_WTTJ_APPLICATION_ID"),
            "Content-Type": "application/json",
            "Referer": "https://www.welcometothejungle.com/",
            "Origin": "https://www.welcometothejungle.com",
        }

        body = json.dumps({
            "requests": [
                {
                    "indexName": "wttj_jobs_production_fr",
                    "params": f'query=data&filters=("offices.country_code":"FR")&hitsPerPage=30&page={num_page}'
                }
            ]
        })

        response = requests.post(url, headers=headers, data=body)
        if response.status_code == 200:
            return response.json()
        else:
            return None

    except requests.exceptions.Timeout:
        print(f"Timeout: le serveur {url} ne répond pas")
    except requests.exceptions.HTTPError as e:
        print(f"Erreur HTTP : {e.response.status_code}")
    except requests.exceptions.ConnectionError:
        print("Erreur de connexion : vérifie ta connexion internet")
    except requests.exceptions.RequestException as e:
        print(f"Erreur inconnue : {e}")

def fetch_jobs(url):
    # Faire une permière requete vers WTTJ
    nb_pages = make_request_wttj(url, num_page=0)["results"][0]["nbPages"]
    nb_offres = make_request_wttj(url, num_page=0)["results"][0]["nbHits"]
    offres = []
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    for num_page in range(nb_pages):
        offres_page = make_request_wttj(url, num_page=num_page)["results"][0]["hits"]
        for offre in offres_page:
            if offre["published_at_date"] == yesterday_str:
                offres.append(offre)
            else:
                continue
    return offres

def clean_empty_objects(obj):
    if isinstance(obj, dict):
        if not obj:  # {} → None
            return None
        return {k: clean_empty_objects(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_empty_objects(i) for i in obj]
    return obj

def upload_to_gcs(bucket_name, data):
    # Uploader une liste d'objet JSON vers un bucket GCS

    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    blob_name = f"welcome-to-the-jungle/offres_{yesterday_str}.json"

    # Créer le client GCS
    client = storage.Client()

    # Créer le bucket
    bucket = client.bucket(bucket_name)

    # Créer l'objet au sein du bucket
    blob = bucket.blob(blob_name)

    # Construire le ndJSON une offre par ligne
    ndjson_content = "\n".join([json.dumps(clean_empty_objects(offre), ensure_ascii=False) for offre in data])

    # Uploader le ndJSON
    if not data:
        print("Aucune donnée à uploader")
    else:
        blob.upload_from_string(
            data = ndjson_content,
            content_type="application/json"
        )

    print(f"{len(data)} offres uploadées vers gs://{bucket_name}/{blob_name}")

if __name__ == "__main__":
   upload_to_gcs("job-market-raw", data=fetch_jobs("https://csekhvms53-dsn.algolia.net/1/indexes/*/queries"))