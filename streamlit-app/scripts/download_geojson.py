"""Télécharge une fois pour toutes les fonds de carte GeoJSON (régions et
départements français) et les stocke dans streamlit-app/assets/.

À lancer UNE SEULE FOIS après le clone du projet :
    python streamlit-app/scripts/download_geojson.py

Les fichiers produits sont ensuite versionnés (ou conservés localement) et lus
directement par le dashboard, qui n'a donc plus besoin d'accès réseau à
l'exécution.

Source : gregoiredavid/france-geojson (noms et codes INSEE millésime 2018,
propriétés `nom` et `code` par feature). C'est le même référentiel INSEE que
celui utilisé par les seeds dbt du projet, ce qui garantit une correspondance
directe des noms de régions.
"""
from __future__ import annotations

import sys
import urllib.request
from pathlib import Path

ASSETS_DIR = Path(__file__).resolve().parent.parent / "assets"

FILES = {
    "regions.geojson": (
        "https://raw.githubusercontent.com/gregoiredavid/france-geojson/"
        "master/regions-version-simplifiee.geojson"
    ),
    "departements.geojson": (
        "https://raw.githubusercontent.com/gregoiredavid/france-geojson/"
        "master/departements-version-simplifiee.geojson"
    ),
}


def main() -> int:
    ASSETS_DIR.mkdir(parents=True, exist_ok=True)
    for filename, url in FILES.items():
        dest = ASSETS_DIR / filename
        if dest.exists():
            print(f"✓ {filename} déjà présent ({dest}), ignoré.")
            continue
        print(f"Téléchargement de {filename}…")
        try:
            urllib.request.urlretrieve(url, dest)
        except Exception as exc:  # noqa: BLE001
            print(f"✗ Échec du téléchargement de {filename} : {exc}", file=sys.stderr)
            print(f"  Vous pouvez le télécharger manuellement depuis :\n  {url}", file=sys.stderr)
            return 1
        print(f"✓ {filename} enregistré dans {dest}")
    print("\nTerminé. Les cartes sont prêtes à être utilisées par le dashboard.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
