import os
import json
import time
import requests
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from google.cloud import storage

# Charger le .env
load_dotenv()

APP_ID = os.getenv("ADZUNA_APP_ID", "").strip()
APP_KEY = os.getenv("ADZUNA_API_KEY", "").strip()
BASE_URL = "https://api.adzuna.com/v1/api/jobs"

# Paramètres de collecte
COUNTRY = "fr"
RESULTS_PER_PAGE = 50  # max autorisé par l'API
MAX_PAGES = 20         # 20 × 50 = 1000 offres max
KEYWORDS = "data"      # aligné avec france_travail.py (motsCles="data")
REQUEST_DELAY = 1.0    # secondes entre chaque appel

# Racine du projet (remonte depuis ingestion/apis/ vers la racine)
BASE_DIR = Path(__file__).resolve().parent.parent.parent


def make_adzuna_api_call(page, max_days_old):
    # Faire appel à l'API Adzuna
    url = f"{BASE_URL}/{COUNTRY}/search/{page}"
    params = {
        "app_id": APP_ID,
        "app_key": APP_KEY,
        "what": KEYWORDS,
        "results_per_page": RESULTS_PER_PAGE,
        "max_days_old": max_days_old,
        "content-type": "application/json",
    }

    response = requests.get(url, params=params, timeout=15)

    if response.status_code == 429:  # Rate limit
        print("Rate limit atteint (429). Pause de 10s...")
        time.sleep(10)
        return make_adzuna_api_call(page, max_days_old)

    if not response.ok:  # 4xx ou 5xx
        print(f"Erreur API [{response.status_code}] sur page {page}: {response.text[:200]}")
        return {"results": None, "count": None}

    body = response.json()
    return {
        "results": body.get("results"),
        "count": body.get("count"),
    }


def fetch_jobs():
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)

    # Bornes de la journée d'hier UTC
    min_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    max_date = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)

    # Adzuna ne supporte pas un filtre fin par dates : on demande les 2 derniers jours
    # puis on filtre côté Python sur le champ `created` pour ne garder que la journée d'hier.
    # Cela évite de manquer des offres en bordure de journée selon l'heure d'exécution.
    offres_brutes = []

    # Premier appel pour récupérer le total disponible
    data = make_adzuna_api_call(page=1, max_days_old=2)

    if not data.get("results"):
        print("Aucune offre trouvée pour cette journée")
        return []

    offres_brutes.extend(data["results"])
    total = data.get("count") or 0
    print(f"Total offres disponibles (48h glissantes) : {total}")

    # Pages suivantes
    page = 1
    while len(offres_brutes) < total and page < MAX_PAGES:
        page += 1
        time.sleep(REQUEST_DELAY)
        data = make_adzuna_api_call(page=page, max_days_old=2)
        if not data.get("results"):
            break
        offres_brutes.extend(data["results"])

    # Filtrer sur la journée d'hier UTC à partir du champ `created` (ISO 8601)
    offres = []
    for o in offres_brutes:
        created_str = o.get("created")
        if not created_str:
            continue
        try:
            # Adzuna renvoie typiquement "2026-04-20T17:32:23Z" ou avec offset
            created = datetime.fromisoformat(created_str.replace("Z", "+00:00"))
            if created.tzinfo is None:
                created = created.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if min_date <= created <= max_date:
            offres.append(o)

    print(f"Offres conservées après filtrage sur la journée d'hier UTC : {len(offres)}")
    return offres


def clean_empty_objects(obj):
    if isinstance(obj, dict):
        if not obj:  # {} → None
            return None
        return {k: clean_empty_objects(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_empty_objects(i) for i in obj]
    return obj


def build_ndjson(data):
    # Construire le ndJSON : une offre par ligne, sans indentation
    return "\n".join([json.dumps(clean_empty_objects(offre), ensure_ascii=False) for offre in data])


def save_local(content, date_str):
    # Sauvegarder le ndJSON en local
    local_dir = BASE_DIR / "data" / "adzuna"
    local_dir.mkdir(parents=True, exist_ok=True)

    local_path = local_dir / f"offres_{date_str}.json"
    local_path.write_text(content, encoding="utf-8")

    print(f"Sauvegardé localement : {local_path}")
    return local_path


def upload_to_gcs(bucket_name, content, date_str):
    # Uploader le ndJSON vers un bucket GCS
    blob_name = f"adzuna/offres_{date_str}.json"

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    blob.upload_from_string(
        data=content,
        content_type="application/json"
    )

    print(f"Uploadé vers gs://{bucket_name}/{blob_name}")


if __name__ == "__main__":
    if not APP_ID or not APP_KEY:
        raise RuntimeError(
            "ADZUNA_APP_ID ou ADZUNA_APP_KEY manquant dans le .env"
        )

    offres = fetch_jobs()

    if not offres:
        print("Aucune donnée à traiter")
    else:
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        date_str = yesterday.strftime("%Y-%m-%d")

        content = build_ndjson(offres)

        save_local(content, date_str)
        upload_to_gcs("job-market-raw", content, date_str)

        print(f"{len(offres)} offres traitées au total")
