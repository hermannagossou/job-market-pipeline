from fastapi import APIRouter, File, HTTPException, UploadFile

from api.core.config import get_settings
from api.schemas.cv import AnalyseCV
from api.services import cv as cv_service

router = APIRouter(prefix="/api/cv", tags=["CV"])


def read_pdf_upload(fichier: UploadFile) -> bytes:
    """Lit un PDF déposé en vérifiant type et taille. Partagé avec le dépôt de CV
    d'un client (routes/clients.py)."""
    if fichier.content_type != "application/pdf":
        raise HTTPException(status_code=415, detail="Format PDF uniquement.")
    max_mb = get_settings().max_cv_size_mb
    contenu = fichier.file.read(max_mb * 1024 * 1024 + 1)
    if len(contenu) > max_mb * 1024 * 1024:
        raise HTTPException(status_code=413, detail=f"Le fichier dépasse la limite de {max_mb} Mo.")
    return contenu


@router.post(
    "/analyse",
    response_model=AnalyseCV,
    summary="Analyse un CV (PDF) et suggère les champs du formulaire — n'enregistre rien",
)
def analyse_cv(fichier: UploadFile = File(..., description="CV au format PDF")) -> AnalyseCV:
    contenu = read_pdf_upload(fichier)
    try:
        return cv_service.analyser_cv(contenu)
    except cv_service.CvIllisibleError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except cv_service.AnalyseIAError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"{exc} Remplis les champs manuellement.",
        ) from exc
