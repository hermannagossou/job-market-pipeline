from pydantic import BaseModel


class ResolutionReferentiel(BaseModel):
    """Un terme extrait du CV, rapproché de la valeur la plus proche du référentiel."""

    extrait: str
    id: str
    label: str
    distance: float


class AnalyseCV(BaseModel):
    """Suggestions tirées d'un CV — jamais enregistrées telles quelles : le client
    les valide ou les corrige dans le formulaire avant toute soumission.

    Les champs `erreur_*` sont renseignés quand la résolution automatique a
    échoué : l'analyse reste exploitable, le client complète alors à la main.
    """

    nom: str | None = None
    prenom: str | None = None
    email: str | None = None
    id_formation: str | None = None
    id_experience: str | None = None
    competences: list[ResolutionReferentiel] = []
    metiers: list[ResolutionReferentiel] = []
    erreur_competences: str | None = None
    erreur_metiers: str | None = None
