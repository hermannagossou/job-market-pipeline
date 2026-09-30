from pydantic import BaseModel


class OffreRecommandee(BaseModel):
    id_offre: str
    metier: str | None = None
    entreprise: str | None = None
    ville: str | None = None
    offre_salaire_min: float
    offre_salaire_max: float
    lien_offre: str | None = None
    score_exact: float
    score_embedding: float
    score_final: float
