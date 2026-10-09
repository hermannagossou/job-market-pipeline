from pydantic import BaseModel


class KpiOverview(BaseModel):
    nb_offres: int
    nb_entreprises: int
    nb_metiers: int
    nb_regions: int
    salaire_min_moyen: float | None = None
    salaire_max_moyen: float | None = None
    nb_offres_salaire_declare: int
    nb_offres_salaire_estime: int
    date_min: str | None = None
    date_max: str | None = None
