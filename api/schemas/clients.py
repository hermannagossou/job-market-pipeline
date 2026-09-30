from datetime import date

from pydantic import BaseModel, Field, field_validator, model_validator

from api.schemas.referentiels import ReferentielItem


class ProfilClientIn(BaseModel):
    """Profil soumis par le formulaire. Les ids renvoient aux référentiels
    exposés par `/api/referentiels/*` ; leur existence est vérifiée côté service."""

    nom: str = Field(min_length=1)
    prenom: str = Field(min_length=1)
    email: str
    id_formation: str
    id_experience: str
    id_contrat: str
    salaire_min: float = Field(ge=0)
    salaire_max: float = Field(ge=0)
    ids_competences: list[str] = Field(min_length=1)
    ids_metiers: list[str] = Field(min_length=1)
    ids_localisations: list[str] = Field(min_length=1)

    @field_validator("nom", "prenom")
    @classmethod
    def _non_vide(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("ne peut pas être vide")
        return value

    @field_validator("email")
    @classmethod
    def _email_normalise(cls, value: str) -> str:
        # Normalisé en minuscules : l'email sert de clé de reconnexion, "Jean@x.fr"
        # et "jean@x.fr" doivent retrouver le même profil.
        value = value.strip().lower()
        if "@" not in value:
            raise ValueError("email invalide")
        return value

    @model_validator(mode="after")
    def _salaires_coherents(self) -> "ProfilClientIn":
        if self.salaire_min > self.salaire_max:
            raise ValueError("le salaire minimum ne peut pas dépasser le salaire maximum")
        return self


class ClientEnregistre(BaseModel):
    id_client: str
    est_nouveau: bool


class ClientTrouve(BaseModel):
    id_client: str


class CvEnregistre(BaseModel):
    cv_storage_path: str


class ProfilClient(BaseModel):
    """Profil complet avec libellés résolus — sert à l'affichage et au
    pré-remplissage du formulaire en mode modification."""

    id_client: str
    nom: str
    prenom: str
    email: str
    id_formation: str
    formation_label: str
    id_experience: str
    experience_label: str
    id_contrat: str
    contrat_label: str
    salaire_min: float
    salaire_max: float
    cv_storage_path: str | None = None
    date_soumission: date | None = None
    competences: list[ReferentielItem]
    metiers: list[ReferentielItem]
    localisations: list[ReferentielItem]
