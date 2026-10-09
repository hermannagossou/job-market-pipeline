from fastapi import APIRouter

from api.schemas.referentiels import ReferentielItem
from api.services import referentiels as referentiels_service
from api.services.referentiels import NomReferentiel

router = APIRouter(prefix="/api/referentiels", tags=["Référentiels"])


@router.get(
    "/{nom}",
    response_model=list[ReferentielItem],
    summary="Valeurs proposées dans le formulaire client (formations, métiers, villes...)",
)
def read_referentiel(nom: NomReferentiel) -> list[ReferentielItem]:
    return referentiels_service.get_referentiel(nom)
