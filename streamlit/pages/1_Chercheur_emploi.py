"""Espace Chercheur d'emploi — où postuler, quoi apprendre, à quel salaire prétendre."""
import streamlit as st

from components import charts
from components.map_france import choropleth
from components.sidebar import render_sidebar
from components.theme import ACCENT, PRIMARY, SUCCESS, fmt_euro, fmt_int, inject_css, kpi_card, persona_hero
from services.api_client import (
    ApiError,
    get_competences_repartition,
    get_contrats_repartition,
    get_kpi_overview,
    get_profil_repartition,
    get_regions,
    get_salaires_competences,
    get_salaires_metiers,
)

st.set_page_config(page_title="Chercheur d'emploi", page_icon="🔍", layout="wide")
inject_css()
persona_hero(
    "🔍 Espace Chercheur d'emploi",
    "Ciblez un métier et une région dans la barre latérale pour un plan d'action personnalisé.",
)

filters = render_sidebar()

with st.spinner("Chargement…"):
    try:
        kpis = get_kpi_overview(filters)
        regions = get_regions(filters)
        competences = get_competences_repartition(filters, limit=15)
        contrats = get_contrats_repartition(filters)
        profil = get_profil_repartition(filters)
        salaires = get_salaires_metiers(filters, limit=15)
        salaires_comp = get_salaires_competences(filters, limit=15)
        salaires_comp_scatter = get_salaires_competences(filters, limit=30, tri="demande")
    except ApiError as exc:
        st.error(f"Erreur lors du chargement : {exc}")
        st.stop()

# --- Contexte personnalisé ---------------------------------------------------
metier_label = filters.get("metier") or "tous métiers"
region_label = filters.get("region") or "toute la France"

c1, c2, c3 = st.columns(3)
c1.markdown(
    kpi_card("Offres correspondant à votre cible", fmt_int(kpis["nb_offres"]),
             f"{metier_label} · {region_label}"),
    unsafe_allow_html=True,
)
top_region = regions[0]["label"] if regions else "—"
c2.markdown(kpi_card("Région qui recrute le plus", top_region,
                     f"{fmt_int(regions[0]['nb_offres']) if regions else '—'} offres"),
            unsafe_allow_html=True)
top_comp = competences[0]["competence"] if competences else "—"
c3.markdown(kpi_card("Compétence n°1 à maîtriser", top_comp,
                     f"{fmt_int(competences[0]['nb_offres']) if competences else '—'} offres la demandent"),
            unsafe_allow_html=True)

st.divider()

# --- Où postuler -------------------------------------------------------------
st.subheader("📍 Où postuler ?")
st.caption("Concentration des offres par région pour votre cible — les zones foncées recrutent le plus.")
if not choropleth(regions, kind="regions", title="Offres par région"):
    charts.bar(regions, "Offres par région", label_key="label", horizontal=True, top_n=12, color=PRIMARY)

# --- Quelles compétences acquérir -------------------------------------------
st.subheader("🛠️ Quelles compétences acquérir ?")
st.caption("Les compétences les plus demandées : autant de leviers pour votre employabilité.")
charts.bar(competences, "Compétences les plus demandées", label_key="competence",
           horizontal=True, top_n=15, color=SUCCESS)

# --- Salaires ----------------------------------------------------------------
st.subheader("💰 À quel salaire prétendre ?")
st.caption(
    "Fourchettes moyennes par métier (annuel brut). Attention : une partie des "
    "salaires est estimée par le pipeline quand l'offre ne l'indiquait pas."
)
charts.salaire_range(salaires, "Fourchettes salariales par métier")

st.subheader("💡 Quelles compétences rapportent le plus ?")
st.caption("Salaire moyen et médian des offres demandant chaque compétence (min. 3 offres).")
charts.salaire_bar(salaires_comp, "Salaire par compétence", label_key="competence", top_n=15)

st.subheader("🔗 Salaire moyen par compétence (top 30 les plus demandées)")
st.caption(
    "Chaque point est une compétence : à droite = très demandée, en haut = "
    "bien rémunérée. Les compétences en haut à droite cumulent les deux."
)
charts.salaire_scatter(salaires_comp_scatter, "Salaire moyen par compétence", top_n=30)

with st.expander("📖 Comment lire ce graphique ?"):
    st.markdown(
        """
        Chaque point représente une compétence, positionnée selon deux axes :
        - **Axe horizontal** : le nombre d'offres qui la demandent (la *demande*).
        - **Axe vertical** : le salaire moyen des offres qui la demandent.

        Les deux lignes pointillées marquent la **médiane** de l'échantillon
        affiché. Elles découpent le graphique en 4 zones :

        - **⭐ En haut à droite** — très demandées et bien payées.
        - **🔝 En haut à gauche** — peu demandées mais bien payées : niche.
        - **↘️ En bas à droite** — très demandées mais salaire dans la fourchette
          basse.
        - **↙️ En bas à gauche** — ni demandées, ni rémunératrices.

        """
    )

# --- Profil attendu ----------------------------------------------------------
st.subheader("🎯 Quel profil est attendu ?")
col1, col2, col3 = st.columns(3)
with col1:
    charts.donut(profil.get("niveau_experience", []), "Expérience", label_key="label")
with col2:
    charts.donut(profil.get("niveau_formation", []), "Formation", label_key="label")
with col3:
    charts.donut(contrats, "Type de contrat", label_key="label")
