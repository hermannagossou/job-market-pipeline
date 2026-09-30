"""Schémas Pydantic partagés par plusieurs domaines (géo, métiers, contrats...)."""
from pydantic import BaseModel


class RepartitionItem(BaseModel):
    """Une valeur de dimension + son nombre d'offres associées."""

    label: str
    nb_offres: int


class EvolutionPoint(BaseModel):
    """Un point de la série temporelle mensuelle."""

    annee: int
    mois: int
    nb_offres: int
