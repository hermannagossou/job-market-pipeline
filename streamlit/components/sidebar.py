"""Sidebar de filtres, partagée par toutes les pages pour rester cohérente.

Les options des menus déroulants sont récupérées depuis l'API elle-même
(listes de valeurs déjà présentes dans les données), plutôt que codées en dur
— si une nouvelle région ou un nouveau secteur apparaît dans les données, il
apparaît automatiquement dans le filtre sans modification du code.
"""
from __future__ import annotations

import streamlit as st

from services.api_client import ApiError, get_contrats_repartition, get_departements, get_metiers_repartition, get_profil_repartition, get_regions, get_secteurs_repartition


def _safe_labels(fetch_fn, *args, **kwargs) -> list[str]:
    """Récupère une liste de labels depuis l'API ; retourne une liste vide et
    affiche un avertissement discret en cas d'échec, plutôt que de casser
    toute la sidebar pour un seul menu déroulant en panne."""
    try:
        items = fetch_fn(*args, **kwargs)
        return sorted({item["label"] for item in items if item.get("label")})
    except ApiError:
        return []


def render_sidebar() -> dict:
    """Affiche les filtres dans la sidebar et retourne un dict des filtres actifs
    (clés absentes ou None si "Tous" est sélectionné, prêt à être passé tel
    quel aux fonctions de `services.api_client`)."""
    st.sidebar.header("Filtres")

    api_unreachable = False

    regions = _safe_labels(get_regions, {})
    if not regions:
        api_unreachable = True

    region = st.sidebar.multiselect("Région", ["Toutes"] + regions)
    region_filter = None if region == "Toutes" else region

    # Les départements ne sont proposés qu'une fois une région choisie, pour
    # éviter un menu de plus de 90 entrées non filtrées.
    departement_filter = None
    if region_filter:
        departements = _safe_labels(get_departements, {"region": region_filter})
        departement = st.sidebar.multiselect("Département", ["Tous"] + departements)
        departement_filter = None if departement == "Tous" else departement

    secteurs = _safe_labels(get_secteurs_repartition, {})
    secteur = st.sidebar.multiselect("Secteur", ["Tous"] + secteurs)
    secteur_filter = None if secteur == "Tous" else secteur

    metiers = _safe_labels(get_metiers_repartition, {}, limit=100)
    metier = st.sidebar.multiselect("Métier", ["Tous"] + metiers)
    metier_filter = None if metier == "Tous" else metier

    contrats = _safe_labels(get_contrats_repartition, {})
    contrat = st.sidebar.multiselect("Type de contrat", ["Tous"] + contrats)
    contrat_filter = None if contrat == "Tous" else contrat

    try:
        profil = get_profil_repartition({})
        formations = sorted({item["label"] for item in profil.get("niveau_formation", [])})
        experiences = sorted({item["label"] for item in profil.get("niveau_experience", [])})
    except ApiError:
        formations, experiences = [], []
        api_unreachable = True

    formation = st.sidebar.multiselect("Niveau de formation", ["Tous"] + formations)
    formation_filter = None if formation == "Tous" else formation

    experience = st.sidebar.multiselect("Niveau d'expérience", ["Tous"] + experiences)
    experience_filter = None if experience == "Tous" else experience

    st.sidebar.subheader("Période")
    col1, col2 = st.sidebar.columns(2)
    date_debut = col1.date_input("Du", value=None, format="YYYY-MM-DD")
    date_fin = col2.date_input("Au", value=None, format="YYYY-MM-DD")

    if api_unreachable:
        st.sidebar.warning(
            "Certains filtres n'ont pas pu être chargés depuis l'API — "
            "vérifiez qu'elle est bien démarrée.",
            icon="⚠️",
        )

    return {
        "region": region_filter,
        "departement": departement_filter,
        "secteur": secteur_filter,
        "metier": metier_filter,
        "type_contrat": contrat_filter,
        "niveau_formation": formation_filter,
        "niveau_experience": experience_filter,
        "date_debut": date_debut if date_debut else None,
        "date_fin": date_fin if date_fin else None,
    }
