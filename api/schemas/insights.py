from pydantic import BaseModel


class TensionMetier(BaseModel):
    """Indicateur de tension : plus il y a d'offres par entreprise sur un métier,
    plus le marché est disputé côté recruteurs (et favorable côté candidats)."""

    metier: str
    nb_offres: int
    nb_entreprises: int
    offres_par_entreprise: float


class SalaireMetier(BaseModel):
    """Fourchette salariale agrégée pour un métier, avec transparence sur la part
    de salaires réellement déclarés (vs imputés par médiane côté dbt)."""

    metier: str
    salaire_min_moyen: float | None = None
    salaire_max_moyen: float | None = None
    salaire_median_bas: float | None = None
    salaire_median_haut: float | None = None
    nb_offres: int
    nb_declares: int
    part_declares: float  # 0..1


class ComparaisonPlateforme(BaseModel):
    """Répartition d'une dimension entre les deux plateformes sources, pour
    l'angle méthodologique (chaque source couvre le marché différemment)."""

    label: str
    france_travail: int
    wttj: int


class SalaireParDimension(BaseModel):
    """Salaire moyen et médian agrégés pour une valeur de dimension (région,
    département, secteur…). Générique pour être réutilisé par plusieurs endpoints.
    `label` porte le nom de la zone/du secteur ; la part de déclarés indique la
    fiabilité (le reste étant imputé par médiane côté dbt)."""

    label: str
    salaire_moyen: float | None = None
    salaire_median: float | None = None
    nb_offres: int
    nb_declares: int
    part_declares: float  # 0..1


class SalaireCompetence(BaseModel):
    """Salaire moyen/médian pour les offres demandant une compétence donnée."""

    competence: str
    categorie: str | None = None
    salaire_moyen: float | None = None
    salaire_median: float | None = None
    nb_offres: int
