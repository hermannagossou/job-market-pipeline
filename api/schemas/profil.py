from pydantic import BaseModel

from api.schemas.common import RepartitionItem


class RepartitionProfil(BaseModel):
    """Répartition combinée formation + expérience, pour éviter deux appels distincts."""

    niveau_formation: list[RepartitionItem]
    niveau_experience: list[RepartitionItem]
