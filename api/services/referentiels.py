"""Référentiels (dimensions) proposés dans le formulaire client.

Mis en cache en mémoire 1h par process : ces dimensions ne changent qu'au run
quotidien du pipeline, inutile de les relire à chaque ouverture du formulaire.
"""
import time
from typing import Literal

from api.db.bigquery import run_query
from api.schemas.referentiels import ReferentielItem
from api.services.base import table

NomReferentiel = Literal["formations", "experiences", "metiers", "contrats", "competences", "localisations"]

# nom exposé -> (table, colonne id, colonne libellé)
_REFERENTIELS: dict[str, tuple[str, str, str]] = {
    "formations": ("dim_formations", "id_formation", "niveau"),
    "experiences": ("dim_experiences", "id_experience", "niveau"),
    "metiers": ("dim_metiers", "id_metier", "nom"),
    "contrats": ("dim_contrats", "id_contrat", "contrat"),
    "competences": ("dim_competences", "id_competence", "competence"),
    "localisations": ("dim_localisations", "id_localisation", "ville"),
}

# "Non Renseigné" existe dans ces dimensions pour les offres sans exigence (rang 0,
# accessibles à tous) — ce n'est pas un niveau qu'un client peut avoir : avec le
# rang 0, il ne verrait que les offres sans aucune exigence.
_EXCLUS: dict[str, set[str]] = {
    "formations": {"Non Renseigné"},
    "experiences": {"Non Renseigné"},
}

_TTL_SECONDES = 3600
_cache: dict[str, tuple[float, list[ReferentielItem]]] = {}


def get_referentiel(nom: NomReferentiel) -> list[ReferentielItem]:
    """Valeurs du référentiel, triées par libellé."""
    en_cache = _cache.get(nom)
    if en_cache and time.monotonic() - en_cache[0] < _TTL_SECONDES:
        return en_cache[1]

    nom_table, id_col, label_col = _REFERENTIELS[nom]
    # Noms de table/colonnes issus du dictionnaire ci-dessus, jamais de l'appelant.
    sql = f"""
    SELECT {id_col} AS id, {label_col} AS label
    FROM {table(nom_table)}
    WHERE {label_col} IS NOT NULL
    ORDER BY {label_col}
    """
    exclus = _EXCLUS.get(nom, set())
    items = [ReferentielItem(**row) for row in run_query(sql) if row["label"] not in exclus]
    _cache[nom] = (time.monotonic(), items)
    return items


def as_dict(nom: NomReferentiel) -> dict[str, str]:
    """{id: libellé} — pour la résolution et la validation des ids côté services."""
    return {item.id: item.label for item in get_referentiel(nom)}
