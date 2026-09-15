from fastapi import APIRouter, Depends

from api.dependencies import FilterParams, get_filters
from api.schemas.common import RepartitionItem
from api.services import contrats as contrats_service

router = APIRouter(prefix="/api/contrats", tags=["Contrats"])


@router.get(
    "/repartition",
    response_model=list[RepartitionItem],
    summary="Répartition des offres par type de contrat",
)
def read_repartition_contrats(filters: FilterParams = Depends(get_filters)) -> list[RepartitionItem]:
    return contrats_service.get_repartition_contrats(filters)
