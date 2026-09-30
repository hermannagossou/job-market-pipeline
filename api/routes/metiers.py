from fastapi import APIRouter, Depends, Query

from api.dependencies import FilterParams, get_filters
from api.schemas.common import EvolutionPoint, RepartitionItem
from api.services import metiers as metiers_service

router = APIRouter(prefix="/api/metiers", tags=["Métiers"])


@router.get(
    "/repartition",
    response_model=list[RepartitionItem],
    summary="Répartition des offres par métier (top N)",
)
def read_repartition_metiers(
    filters: FilterParams = Depends(get_filters),
    limit: int = Query(20, ge=1, le=100, description="Nombre de métiers à retourner"),
) -> list[RepartitionItem]:
    return metiers_service.get_repartition_metiers(filters, limit=limit)


@router.get(
    "/evolution",
    response_model=list[EvolutionPoint],
    summary="Évolution mensuelle du volume d'offres (filtrer par métier pour une courbe unique)",
)
def read_evolution_metiers(filters: FilterParams = Depends(get_filters)) -> list[EvolutionPoint]:
    return metiers_service.get_evolution_metiers(filters)


@router.get(
    "/evolution-granulaire",
    summary="Nombre d'offres publiées dans le temps (jour / mois / année)",
)
def read_evolution_granulaire(
    filters: FilterParams = Depends(get_filters),
    granularite: str = Query("mois", pattern="^(jour|mois|annee)$", description="jour, mois ou annee"),
) -> list[dict]:
    return metiers_service.get_evolution_offres(filters, granularite=granularite)
