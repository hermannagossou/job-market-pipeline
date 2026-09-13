"""Analyse géographique — cartes choroplèthes de la concentration des offres."""
import streamlit as st

from components import charts
from components.map_france import choropleth
from components.sidebar import render_sidebar
from components.theme import PRIMARY, fmt_int, inject_css, kpi_card, persona_hero
from services.api_client import (
    ApiError,
    get_competences_repartition,
    get_departements,
    get_kpi_overview,
    get_metiers_repartition,
    get_regions,
    get_secteurs_repartition,
)

st.set_page_config(page_title="Analyse géographique", page_icon="🗺️", layout="wide")
inject_css()
persona_hero(
    "🗺️ Analyse géographique",
    "Où se concentrent les offres en France ? Cartes interactives par région et département.",
)

filters = render_sidebar()

with st.spinner("Chargement des données géographiques…"):
    try:
        kpis = get_kpi_overview(filters)
        regions = get_regions(filters)
    except ApiError as exc:
        st.error(f"Erreur lors du chargement : {exc}")
        st.stop()

# --- Indicateurs -------------------------------------------------------------
c1, c2, c3 = st.columns(3)
c1.markdown(kpi_card("Offres localisées", fmt_int(kpis["nb_offres"]),
                     filters.get("metier") or "tous métiers"),
            unsafe_allow_html=True)
top_region = regions[0]["label"] if regions else "—"
c2.markdown(kpi_card("Région n°1", top_region,
                     f"{fmt_int(regions[0]['nb_offres']) if regions else '—'} offres"),
            unsafe_allow_html=True)
if regions and kpis["nb_offres"]:
    part = regions[0]["nb_offres"] / kpis["nb_offres"]
    c3.markdown(kpi_card("Concentration", f"{part:.0%}",
                         f"des offres dans {top_region}"),
                unsafe_allow_html=True)
else:
    c3.markdown(kpi_card("Concentration", "—", ""), unsafe_allow_html=True)

st.divider()

# --- Carte des régions -------------------------------------------------------
st.subheader("Carte par région")
rendered = choropleth(regions, kind="regions", title="Concentration des offres par région")
if not rendered:
    # Repli : si le GeoJSON n'est pas installé, on montre au moins le bar chart.
    charts.bar(regions, "Offres par région", label_key="label", horizontal=True, top_n=15, color=PRIMARY)

# --- Carte des départements --------------------------------------------------
st.subheader("Carte par département")
st.caption(
    "Sélectionnez une région dans la barre latérale pour zoomer sur ses "
    "départements, ou laissez vide pour la vue nationale."
)
with st.spinner("Chargement des départements…"):
    try:
        departements = get_departements(filters)
    except ApiError as exc:
        st.error(f"Erreur lors du chargement des départements : {exc}")
        departements = []

rendered_dep = choropleth(departements, kind="departements",
                          title="Concentration des offres par département")
if not rendered_dep and departements:
    charts.bar(departements, "Offres par département", label_key="label",
               horizontal=True, top_n=20, color=PRIMARY)

st.divider()

# --- Détail régional (si une région est sélectionnée) ------------------------
if filters.get("region"):
    st.subheader(f"Zoom sur {filters['region']}")
    with st.spinner("Chargement du détail régional…"):
        try:
            metiers = get_metiers_repartition(filters, limit=10)
            secteurs = get_secteurs_repartition(filters)
            competences = get_competences_repartition(filters, limit=10)
        except ApiError as exc:
            st.error(f"Erreur lors du chargement du détail : {exc}")
            st.stop()

    col1, col2 = st.columns(2)
    with col1:
        charts.bar(metiers, "Métiers les plus recherchés", label_key="label", horizontal=True)
    with col2:
        charts.donut(secteurs, "Secteurs principaux", label_key="label", top_n=10)
    charts.bar(competences, "Compétences les plus demandées dans la région",
               label_key="competence", horizontal=True)
else:
    st.info(
        "💡 Sélectionnez une région dans la barre latérale pour obtenir le détail "
        "de ses métiers, secteurs et compétences les plus demandés.",
    )
