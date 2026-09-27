"""Espace Analyste / Marché — tendances de fond, structure, comparaison des sources."""
import streamlit as st

from components import charts
from components.sidebar import render_sidebar
from components.theme import PRIMARY, fmt_int, inject_css, kpi_card, persona_hero
from api_client import (
    ApiError,
    get_comparaison_plateformes,
    get_competences_repartition,
    get_kpi_overview,
    get_metiers_evolution,
    get_metiers_repartition,
    get_secteurs_repartition,
)

st.set_page_config(page_title="Analyste / Marché", page_icon="📈", layout="wide")
inject_css()
persona_hero(
    "📈 Espace Analyste / Marché",
    "Tendances temporelles, structure sectorielle et comparaison méthodologique des deux sources.",
)

filters = render_sidebar()

with st.spinner("Chargement…"):
    try:
        kpis = get_kpi_overview(filters)
        evolution = get_metiers_evolution(filters)
        metiers = get_metiers_repartition(filters, limit=13)
        secteurs = get_secteurs_repartition(filters)
        competences = get_competences_repartition(filters, limit=15)
        comp_plateforme_metier = get_comparaison_plateformes(filters, dimension="metier", limit=13)
        comp_plateforme_contrat = get_comparaison_plateformes(filters, dimension="contrat", limit=10)
    except ApiError as exc:
        st.error(f"Erreur lors du chargement : {exc}")
        st.stop()

# --- Indicateurs macro -------------------------------------------------------
c1, c2, c3 = st.columns(3)
c1.markdown(kpi_card("Volume total d'offres", fmt_int(kpis["nb_offres"]), "sur la période couverte"),
            unsafe_allow_html=True)
c2.markdown(kpi_card("Diversité des métiers", fmt_int(kpis["nb_metiers"]), "métiers Data distincts"),
            unsafe_allow_html=True)
c3.markdown(kpi_card("Secteurs représentés", fmt_int(len(secteurs)), "classés par IA"),
            unsafe_allow_html=True)

st.divider()

# --- Tendance temporelle -----------------------------------------------------
st.subheader("📅 Évolution du volume d'offres")
st.caption("Tendance mensuelle des publications (filtrable par métier/secteur/région dans la barre latérale).")
charts.evolution(evolution, "Offres publiées par mois")

# --- Structure du marché -----------------------------------------------------
st.subheader("🏗️ Structure du marché")
col1, col2 = st.columns(2)
with col1:
    charts.bar(metiers, "Répartition par métier", label_key="label", horizontal=True, color=PRIMARY)
with col2:
    charts.donut(secteurs, "Répartition par secteur", label_key="label", top_n=12)

# --- Comparaison des sources -------------------------------------------------
st.subheader("🔬 Comparaison des deux sources")
st.caption(
    "Angle méthodologique : France Travail et Welcome to the Jungle ne couvrent "
    "pas le marché de la même façon. Le volume brut (à gauche) montre la "
    "contribution de chaque source ; la structure en part (à droite) montre "
    "lesquelles sont sur-représentées dans l'une ou l'autre."
)
col3, col4 = st.columns(2)
with col3:
    charts.grouped_platform_bar(comp_plateforme_metier, "Volume par métier et par source")
with col4:
    charts.horizontal_stacked_platform(comp_plateforme_contrat, "Répartition des contrats par source (%)")
