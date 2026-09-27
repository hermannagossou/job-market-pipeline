# Image de l'API FastAPI — construite avec api/ comme contexte (voir compose.app.yml).
FROM python:3.12-slim

# Pas de .pyc, logs non bufferisés (visibles en direct dans `docker compose logs`).
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Dépendances d'abord : couche mise en cache tant que requirements.txt ne change pas.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Le code est importé comme package `api` (api.main, api.services...) : il est
# copié dans /app/api pour conserver ce nom.
COPY . ./api

# Utilisateur non root : l'API n'a besoin d'écrire nulle part dans l'image.
RUN useradd --create-home --uid 1000 app
USER app

EXPOSE 8000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
