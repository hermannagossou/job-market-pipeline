from fastapi import APIRouter, File, HTTPException, Query, UploadFile

from api.routes.cv import read_pdf_upload
from api.schemas.clients import ClientEnregistre, ClientTrouve, CvEnregistre, ProfilClient, ProfilClientIn
from api.schemas.recommandations import OffreRecommandee
from api.services import clients as clients_service
from api.services import recommandations as recommandations_service

router = APIRouter(prefix="/api/clients", tags=["Clients"])


def _client_ou_404(id_client: str) -> None:
    if not clients_service.client_exists(id_client):
        raise HTTPException(status_code=404, detail="Client introuvable.")


@router.get(
    "",
    response_model=ClientTrouve,
    summary="Retrouve un client par son email (simple recherche, pas une authentification)",
)
def find_client(email: str = Query(..., min_length=3, description="Email du client")) -> ClientTrouve:
    id_client = clients_service.find_client_by_email(email)
    if id_client is None:
        raise HTTPException(status_code=404, detail="Aucun profil trouvé avec cet email.")
    return ClientTrouve(id_client=id_client)


@router.put(
    "",
    response_model=ClientEnregistre,
    summary="Crée ou met à jour un profil client (identifié par son email)",
)
def upsert_client(profil: ProfilClientIn) -> ClientEnregistre:
    try:
        return clients_service.upsert_client_profile(profil)
    except clients_service.ReferenceInconnueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/{id_client}", response_model=ProfilClient, summary="Profil complet d'un client")
def read_client(id_client: str) -> ProfilClient:
    profil = clients_service.get_client_profile(id_client)
    if profil is None:
        raise HTTPException(status_code=404, detail="Client introuvable.")
    return profil


@router.put(
    "/{id_client}/cv",
    response_model=CvEnregistre,
    summary="Dépose (ou remplace) le CV d'un client existant",
)
def upload_cv(id_client: str, fichier: UploadFile = File(..., description="CV au format PDF")) -> CvEnregistre:
    contenu = read_pdf_upload(fichier)
    _client_ou_404(id_client)
    return CvEnregistre(cv_storage_path=clients_service.save_cv(id_client, contenu))


@router.get(
    "/{id_client}/recommandations",
    response_model=list[OffreRecommandee],
    summary="Offres recommandées pour un client, triées par pertinence",
)
def read_recommandations(
    id_client: str,
    top_n: int = Query(10, ge=1, le=50, description="Nombre d'offres à retourner"),
) -> list[OffreRecommandee]:
    _client_ou_404(id_client)
    return recommandations_service.get_recommendations(id_client, top_n=top_n)
