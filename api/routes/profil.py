from fastapi import APIRouter, Depends

from api.dependencies import FilterParams, get_filters
from api.schemas.profil import RepartitionProfil
from api.services import profil as profil_service

router = APIRouter(prefix="/api/profil", tags=["Profil recherché"])


@router.get(
    "/repartition",
    response_model=RepartitionProfil,
    summary="Répartition par niveau de formation et niveau d'expérience recherchés",
)
def read_repartition_profil(filters: FilterParams = Depends(get_filters)) -> RepartitionProfil:
    return profil_service.get_repartition_profil(filters)
