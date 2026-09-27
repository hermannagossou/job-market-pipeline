from fastapi import APIRouter, Depends, Query

from api.dependencies import FilterParams, get_filters
from api.schemas.offres import OffresPage
from api.services import offres as offres_service

router = APIRouter(prefix="/api/offres", tags=["Offres"])


@router.get(
    "",
    response_model=OffresPage,
    summary="Liste paginée d'offres individuelles, pour les tableaux détaillés",
)
def read_offres(
    filters: FilterParams = Depends(get_filters),
    page: int = Query(1, ge=1, description="Numéro de page (commence à 1)"),
    page_size: int = Query(25, ge=1, le=100, description="Nombre d'offres par page"),
) -> OffresPage:
    return offres_service.get_offres(filters, page=page, page_size=page_size)
