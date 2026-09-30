from pydantic import BaseModel


class ReferentielItem(BaseModel):
    """Une valeur de référentiel (dimension), telle que proposée dans le formulaire client."""

    id: str
    label: str
