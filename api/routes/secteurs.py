from fastapi import APIRouter, Depends

from api.dependencies import FilterParams, get_filters
from api.schemas.common import RepartitionItem
from api.services import secteurs as secteurs_service

router = APIRouter(prefix="/api/secteurs", tags=["Secteurs"])


@router.get(
    "/repartition",
    response_model=list[RepartitionItem],
    summary="Nombre d'offres par secteur d'activité",
)
def read_repartition_secteurs(filters: FilterParams = Depends(get_filters)) -> list[RepartitionItem]:
    return secteurs_service.get_repartition_secteurs(filters)
