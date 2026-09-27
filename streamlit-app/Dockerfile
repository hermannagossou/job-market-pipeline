# Image de l'app Streamlit — construite avec streamlit-app/ comme contexte (voir
# compose.app.yml). L'app ne parle qu'à l'API : aucun identifiant GCP dans ce conteneur.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Fonds de carte GeoJSON (régions, départements) téléchargés au build : image
# autonome, sans accès réseau à l'exécution. Le script ignore les fichiers déjà
# présents. En cas d'échec, les cartes se replient sur des graphiques en barres.
RUN python scripts/download_geojson.py || \
    echo "Avertissement : téléchargement GeoJSON échoué, cartes remplacées par des barres."

RUN useradd --create-home --uid 1000 app
USER app

EXPOSE 8501

# 0.0.0.0 pour être joignable hors du conteneur ; headless : pas de tentative
# d'ouverture de navigateur ni d'invite email au démarrage.
CMD ["streamlit", "run", "app.py", \
     "--server.address=0.0.0.0", "--server.port=8501", \
     "--server.headless=true", "--browser.gatherUsageStats=false"]
