"""Espace RH / Recruteur — tension du marché, benchmark salaire, compétences différenciantes."""
import pandas as pd
import streamlit as st

from components import charts
from components.sidebar import render_sidebar
from components.theme import ACCENT, PRIMARY, fmt_euro, fmt_int, inject_css, kpi_card, persona_hero
from api_client import (
    ApiError,
    get_competences_repartition,
    get_evolution_granulaire,
    get_kpi_overview,
    get_salaires_dimension,
    get_salaires_metiers,
    get_tension_metiers,
)

st.set_page_config(page_title="RH / Recruteur", page_icon="👔", layout="wide")
inject_css()
persona_hero(
    "👔 Espace RH / Recruteur",
    "Évaluez la tension du marché, votre compétitivité salariale et les compétences différenciantes.",
)

filters = render_sidebar()

with st.spinner("Chargement…"):
    try:
        kpis = get_kpi_overview(filters)
        tension = get_tension_metiers(filters, limit=15)
        salaires = get_salaires_metiers(filters, limit=15)
        competences = get_competences_repartition(filters, limit=25)
        sal_region = get_salaires_dimension(filters, dimension="region", limit=20)
        sal_secteur = get_salaires_dimension(filters, dimension="secteur", limit=20)
    except ApiError as exc:
        st.error(f"Erreur lors du chargement : {exc}")
        st.stop()

# --- Indicateurs de haut de page --------------------------------------------
c1, c2, c3 = st.columns(3)
metier_plus_tendu = tension[0]["metier"] if tension else "—"
ratio_max = f"{tension[0]['offres_par_entreprise']:.1f} offres/entreprise" if tension else "—"
c1.markdown(kpi_card("Métier le plus disputé", metier_plus_tendu, ratio_max), unsafe_allow_html=True)
c2.markdown(kpi_card("Entreprises en concurrence", fmt_int(kpis["nb_entreprises"]),
                     f"sur {fmt_int(kpis['nb_offres'])} offres"),
            unsafe_allow_html=True)
if salaires:
    sal_haut = max((s["salaire_max_moyen"] or 0) for s in salaires)
    c3.markdown(kpi_card("Salaire max moyen le plus élevé", fmt_euro(sal_haut),
                         "métier le mieux rémunéré de votre sélection"),
                unsafe_allow_html=True)
else:
    c3.markdown(kpi_card("Salaire max moyen", "—", "aucune donnée salariale"), unsafe_allow_html=True)

st.divider()

# --- Tension du marché -------------------------------------------------------
st.subheader("🌡️ Où est la tension ?")
st.caption(
    "Chaque point est un métier. En haut à gauche (beaucoup d'offres, peu "
    "d'entreprises) = marché tendu, difficile à recruter. La taille encode le "
    "ratio offres/entreprise."
)
charts.tension_scatter(tension, "Tension par métier")

# --- Benchmark salaire -------------------------------------------------------
st.subheader("💶 Êtes-vous compétitif sur les salaires ?")
st.caption("Fourchettes du marché par métier. Positionnez votre grille par rapport à ces repères.")
charts.salaire_range(salaires, "Fourchettes salariales du marché")

with st.expander("Voir le détail chiffré (et la fiabilité des salaires)"):
    if salaires:
        df = pd.DataFrame(salaires)
        df_display = df.assign(
            **{
                "Métier": df["metier"],
                "Min moyen": df["salaire_min_moyen"].map(fmt_euro),
                "Max moyen": df["salaire_max_moyen"].map(fmt_euro),
                "Offres": df["nb_offres"],
                "Part déclarés": (df["part_declares"] * 100).round(0).astype("Int64").astype(str) + " %",
            }
        )[["Métier", "Min moyen", "Max moyen", "Offres", "Part déclarés"]]
        st.dataframe(df_display, width="stretch", hide_index=True)
    else:
        st.info("Aucune donnée salariale pour les filtres actuels.")

# --- Salaire par région et secteur ------------------------------------------
st.subheader("🗺️ Grille salariale par région et par secteur")
st.caption("Salaire moyen et médian (annuel brut) pour situer votre positionnement géographique et sectoriel.")
col_r, col_s = st.columns(2)
with col_r:
    charts.salaire_bar(sal_region, "Par région", label_key="label", top_n=12)
with col_s:
    charts.salaire_bar(sal_secteur, "Par secteur", label_key="label", top_n=12)

# --- Compétences différenciantes --------------------------------------------
st.subheader("🧩 Quelles compétences valoriser dans vos offres ?")
st.caption(
    "Les compétences fréquentes attirent un large vivier ; les plus rares sont "
    "différenciantes mais réduisent le nombre de candidats potentiels."
)
col1, col2 = st.columns(2)
with col1:
    charts.bar(competences[:10], "Les 10 plus demandées (vivier large)",
               label_key="competence", horizontal=True, color=PRIMARY)
with col2:
    rares = sorted(competences, key=lambda x: x["nb_offres"])[:10]
    charts.bar(rares, "Les 10 plus rares (différenciantes)",
               label_key="competence", horizontal=True, color=ACCENT)

# --- Volume d'offres par compétence -----------------------------------------
st.subheader("📊 Nombre d'offres par compétence")
st.caption("Le volume de demande pour chaque compétence — utile pour calibrer vos attentes de sourcing.")
charts.bar(competences[:20], "Offres par compétence", label_key="competence",
           horizontal=True, color=PRIMARY)

# --- Rythme de publication ---------------------------------------------------
st.subheader("📅 Rythme de publication des offres")
st.caption("Suivez le volume d'offres publiées dans le temps, au grain de votre choix.")
gran_label = st.radio("Granularité", ["Jour", "Mois", "Année"], horizontal=True, index=1, key="rh_gran")
gran_map = {"Jour": "jour", "Mois": "mois", "Année": "annee"}
try:
    evo = get_evolution_granulaire(filters, granularite=gran_map[gran_label])
except ApiError as exc:
    st.error(f"Erreur : {exc}")
    evo = []
charts.evolution_periode(evo, f"Offres publiées par {gran_label.lower()}")
