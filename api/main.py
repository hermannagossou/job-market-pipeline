"""Point d'entrée de l'API FastAPI.

Lancement local :
    uvicorn api.main:app --reload --port 8000

Documentation interactive une fois lancée : http://localhost:8000/docs
"""
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from api.core.config import get_settings
from api.db.bigquery import BigQueryQueryError
from api.routes import competences, contrats, geo, insights, kpis, metiers, offres, profil, secteurs

settings = get_settings()

app = FastAPI(title=settings.api_title, version=settings.api_version)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_allow_origins,
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.exception_handler(BigQueryQueryError)
async def bigquery_error_handler(request: Request, exc: BigQueryQueryError) -> JSONResponse:
    """Convertit toute erreur BigQuery en réponse 503 propre, sans exposer le
    détail technique de l'erreur (déjà loggé côté serveur par run_query())."""
    return JSONResponse(
        status_code=503,
        content={
            "detail": (
                "Le service de données est momentanément indisponible. "
                "Réessayez dans quelques instants."
            )
        },
    )


app.include_router(kpis.router)
app.include_router(geo.router)
app.include_router(metiers.router)
app.include_router(secteurs.router)
app.include_router(competences.router)
app.include_router(contrats.router)
app.include_router(profil.router)
app.include_router(offres.router)
app.include_router(insights.router)


@app.get("/", tags=["Santé"], summary="Vérifie que l'API est en ligne")
def read_root() -> dict:
    return {"status": "ok", "service": settings.api_title, "version": settings.api_version}
