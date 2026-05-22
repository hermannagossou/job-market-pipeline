import os
import re
import json
import logging
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from google.cloud import storage

# Charger le .env
load_dotenv()

logger = logging.getLogger(__name__)

CLIENT_ID = os.getenv("FRANCE_TRAVAIL_CLIENT_ID")
CLIENT_SECRET = os.getenv("FRANCE_TRAVAIL_CLIENT_SECRET")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
TOKEN_URL = "https://entreprise.francetravail.fr/connexion/oauth2/access_token?realm=/partenaire"

def get_access_token():
    # Récuperer un access token OAuth2 (Client Credentials)
    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "scope": "o2dsoffre api_offresdemploiv2"
        }
    )
    response.raise_for_status()
    token_data = response.json()
    return token_data["access_token"]

def make_france_travail_api_call(token_info, endpoint, offset, min_date_str, max_date_str):
    # Faire appel à l'API France Travail

    headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token_info}"
    }
    querystring = {
        "motsCles":"data",
        "minCreationDate": min_date_str,
        "maxCreationDate": max_date_str,
        "range": offset
    }


    response = requests.get(
        f"https://api.francetravail.io/partenaire/offresdemploi{endpoint}",
        headers = headers,
        params = querystring
    )

    if response.status_code == 204:  # No Content = plus de résultats
        return {"resultats": None, "content-range": None}

    if not response.ok:  # 4xx ou 5xx
        logger.warning(f"Erreur API [{response.status_code}] sur range {offset}: {response.text[:200]}")
        return {"resultats": None, "content-range": None}

    return {
        "resultats": response.json().get("resultats"),
        "content-range": response.headers.get("Content-Range")
    }

def fetch_jobs(target_date):
    min_date = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
    max_date = target_date.replace(hour=23, minute=59, second=59, microsecond=0)

    min_date_str = min_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    max_date_str = max_date.strftime("%Y-%m-%dT%H:%M:%SZ")

    token = get_access_token()
    offres = []

    # Premier appel de l'API
    data = make_france_travail_api_call(token, "/v2/offres/search", "0-149", min_date_str, max_date_str)

    if not data.get("resultats"):
        logger.info("Aucune offre trouvée pour cette journée")
        return []
    offres.extend(data.get("resultats"))

    # Récupération du premier élément, du dernier élément et du nombre total d'élément de la recherche
    match = re.search(r"(\d+)-(\d+)/(\d+)", data.get("content-range") or "")
    if not match:
        logger.warning("Content-Range manquant ou invalide, pagination arrêtée")
        return offres
    p = int(match.group(1))
    d = int(match.group(2))
    t = int(match.group(3))

    while p < t and p <= 3000:
        p += 150
        d += 150
        data = make_france_travail_api_call(token, "/v2/offres/search", f"{p}-{d}", min_date_str, max_date_str)
        if not data.get("resultats") or not data.get("content-range"):
            break
        offres.extend(data.get("resultats"))

    return offres

def clean_empty_objects(obj):
    if isinstance(obj, dict):
        if not obj:  # {} → None
            return None
        return {k: clean_empty_objects(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_empty_objects(i) for i in obj]
    return obj

def upload_to_gcs(bucket_name, data, target_date):

    # Uploader une liste d'objet JSON vers un bucket GCS

    if not data:
        logger.info("Aucune donnée à uploader")
        return

    # Définir le nom du fichier à uploader
    date_str = target_date.strftime("%Y-%m-%d")
    blob_name = f"france-travail/offres_{date_str}.json"

    # Créer le client GCS
    client = storage.Client()

    # Cibler le bucket
    bucket = client.bucket(bucket_name)

    # Créer le fichier au sein du bucket
    blob = bucket.blob(blob_name)

    # Construire le ndJSON: une offre par ligne, dans indentation
    date_chargement = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    ndjson_content = "\n".join([json.dumps({**clean_empty_objects(offre), "date_chargement": date_chargement}, ensure_ascii=False) for offre in data])

    blob.upload_from_string(
        data = ndjson_content,
        content_type="application/json"
    )

    logger.info(f"{len(data)} offres uploadées vers gs://{bucket_name}/{blob_name}")

def run():
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    offres = fetch_jobs(yesterday)
    upload_to_gcs(GCS_BUCKET_NAME, data=offres, target_date=yesterday)

if __name__ == "__main__":
    run()
