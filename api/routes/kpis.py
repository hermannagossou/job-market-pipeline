from fastapi import APIRouter, Depends

from api.dependencies import FilterParams, get_filters
from api.schemas.kpis import KpiOverview
from api.services import kpis as kpis_service

router = APIRouter(prefix="/api/kpis", tags=["KPIs"])


@router.get(
    "/overview",
    response_model=KpiOverview,
    summary="Indicateurs clés du marché de l'emploi (page Vue d'ensemble)",
)
def read_kpi_overview(filters: FilterParams = Depends(get_filters)) -> KpiOverview:
    return kpis_service.get_kpi_overview(filters)
