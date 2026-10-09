from fastapi import APIRouter, Depends, Query

from api.dependencies import FilterParams, get_filters
from api.schemas.common import EvolutionPoint
from api.schemas.competences import RepartitionCompetence
from api.services import competences as competences_service
from api.services.competences import GroupBy

router = APIRouter(prefix="/api/competences", tags=["Compétences"])


@router.get(
    "/repartition",
    response_model=list[RepartitionCompetence],
    summary="Top compétences demandées, avec ventilation optionnelle par métier/secteur/région",
)
def read_repartition_competences(
    filters: FilterParams = Depends(get_filters),
    group_by: GroupBy | None = Query(
        None, description="Ventiler le résultat par 'metier', 'secteur' ou 'region'"
    ),
    limit: int = Query(20, ge=1, le=100, description="Nombre de compétences à retourner"),
) -> list[RepartitionCompetence]:
    return competences_service.get_repartition_competences(filters, group_by=group_by, limit=limit)


@router.get(
    "/evolution",
    response_model=list[EvolutionPoint],
    summary="Évolution mensuelle de la demande pour une compétence donnée",
)
def read_evolution_competence(
    competence: str = Query(..., description="Nom exact de la compétence (ex: Python, dbt, Spark)"),
    filters: FilterParams = Depends(get_filters),
) -> list[EvolutionPoint]:
    return competences_service.get_evolution_competence(filters, competence=competence)
