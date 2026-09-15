from pydantic import BaseModel


class RepartitionCompetence(BaseModel):
    competence: str
    categorie: str | None = None
    # Renseigné uniquement quand l'appel a précisé `group_by` (metier/secteur/région)
    categorie_ventilation: str | None = None
    nb_offres: int


class EvolutionCompetence(BaseModel):
    annee: int
    mois: int
    nb_offres: int
