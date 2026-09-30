from fastapi import APIRouter, Depends, Query

from typing import Literal

from api.dependencies import FilterParams, get_filters
from api.schemas.insights import (
    ComparaisonPlateforme,
    SalaireCompetence,
    SalaireMetier,
    SalaireParDimension,
    TensionMetier,
)
from api.services import insights as insights_service
from api.services.insights import DimensionComparaison, DimensionSalaire

router = APIRouter(prefix="/api/insights", tags=["Insights"])


@router.get(
    "/tension-metiers",
    response_model=list[TensionMetier],
    summary="Tension du marché par métier (offres par entreprise)",
)
def read_tension_metiers(
    filters: FilterParams = Depends(get_filters),
    limit: int = Query(15, ge=1, le=50),
) -> list[TensionMetier]:
    return insights_service.get_tension_metiers(filters, limit=limit)


@router.get(
    "/salaires-metiers",
    response_model=list[SalaireMetier],
    summary="Fourchettes salariales par métier, avec part de salaires déclarés",
)
def read_salaires_metiers(
    filters: FilterParams = Depends(get_filters),
    limit: int = Query(15, ge=1, le=50),
) -> list[SalaireMetier]:
    return insights_service.get_salaires_metiers(filters, limit=limit)


@router.get(
    "/comparaison-plateformes",
    response_model=list[ComparaisonPlateforme],
    summary="Répartition d'une dimension entre France Travail et WTTJ",
)
def read_comparaison_plateformes(
    filters: FilterParams = Depends(get_filters),
    dimension: DimensionComparaison = Query("metier", description="metier, secteur, contrat ou region"),
    limit: int = Query(15, ge=1, le=50),
) -> list[ComparaisonPlateforme]:
    return insights_service.get_comparaison_plateformes(filters, dimension=dimension, limit=limit)


@router.get(
    "/salaires-dimension",
    response_model=list[SalaireParDimension],
    summary="Salaire moyen et médian par région, département ou secteur",
)
def read_salaires_dimension(
    filters: FilterParams = Depends(get_filters),
    dimension: DimensionSalaire = Query("region", description="region, departement ou secteur"),
    limit: int = Query(30, ge=1, le=110),
) -> list[SalaireParDimension]:
    return insights_service.get_salaires_par_dimension(filters, dimension=dimension, limit=limit)


@router.get(
    "/salaires-competences",
    response_model=list[SalaireCompetence],
    summary="Salaire moyen et médian par compétence demandée",
)
def read_salaires_competences(
    filters: FilterParams = Depends(get_filters),
    limit: int = Query(20, ge=1, le=50),
    tri: Literal["salaire", "demande"] = Query("salaire"),
) -> list[SalaireCompetence]:
    return insights_service.get_salaires_competences(filters, limit=limit, tri=tri)
