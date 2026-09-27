from pydantic import BaseModel


class OffreDetail(BaseModel):
    id_offre: str
    nom_metier: str
    nom_entreprise: str
    type_contrat: str
    niveau_formation: str
    niveau_experience: str
    ville: str | None = None
    departement: str | None = None
    region: str | None = None
    nom_secteur: str
    date_publication: str
    nom_plateforme: str
    salaire_min: float | None = None
    salaire_max: float | None = None
    statut_salaire: str
    nbre_postes: int


class OffresPage(BaseModel):
    total: int
    page: int
    page_size: int
    resultats: list[OffreDetail]
