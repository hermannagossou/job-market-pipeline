from fastapi import APIRouter, Depends, Query

from api.dependencies import FilterParams, get_filters
from api.schemas.common import RepartitionItem
from api.services import geo as geo_service

router = APIRouter(prefix="/api/geo", tags=["Géographie"])


@router.get(
    "/regions",
    response_model=list[RepartitionItem],
    summary="Nombre d'offres par région",
)
def read_repartition_regions(filters: FilterParams = Depends(get_filters)) -> list[RepartitionItem]:
    return geo_service.get_repartition_regions(filters)


@router.get(
    "/departements",
    response_model=list[RepartitionItem],
    summary="Nombre d'offres par département (filtrer par région recommandé)",
)
def read_repartition_departements(filters: FilterParams = Depends(get_filters)) -> list[RepartitionItem]:
    return geo_service.get_repartition_departements(filters)


@router.get(
    "/top-entreprises-departement",
    summary="Entreprise qui recrute le plus par département",
)
def read_top_entreprises_departement(
    filters: FilterParams = Depends(get_filters),
    limit_par_dep: int = Query(1, ge=1, le=5, description="Nombre d'entreprises par département"),
) -> list[dict]:
    return geo_service.get_top_entreprises_departement(filters, limit_par_dep=limit_par_dep)
