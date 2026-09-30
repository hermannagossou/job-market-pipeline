"""Page d'accueil : pouls du marché + vues nationales + orientation persona."""
import streamlit as st

from components import charts
from components.map_france import choropleth
from components.theme import ACCENT, PRIMARY, fmt_euro, fmt_int, inject_css, kpi_card
from services.api_client import (
    ApiError,
    get_competences_repartition,
    get_kpi_overview,
    get_salaires_dimension,
    get_secteurs_repartition,
    get_top_entreprises_departement,
)

st.set_page_config(page_title="Marché de l'emploi Data — France", page_icon="📊", layout="wide")
inject_css()

st.title("📊 Observatoire de l'emploi Data en France")
st.caption(
    "Analyse des offres publiées sur France Travail et Welcome to the Jungle. "
    "Choisissez l'espace correspondant à votre profil dans le menu de gauche."
)

try:
    kpis = get_kpi_overview({})
except ApiError as exc:
    st.error(
        f"Impossible de récupérer les indicateurs depuis l'API : {exc}\n\n"
        "Vérifiez que l'API est démarrée (`uvicorn api.main:app`) et que "
        "`API_BASE_URL` pointe vers la bonne adresse."
    )
    st.stop()

# --- Pouls du marché ---------------------------------------------------------
c1, c2, c3, c4 = st.columns(4)
c1.markdown(kpi_card("Offres analysées", fmt_int(kpis["nb_offres"])), unsafe_allow_html=True)
c2.markdown(kpi_card("Entreprises", fmt_int(kpis["nb_entreprises"])), unsafe_allow_html=True)
c3.markdown(kpi_card("Métiers suivis", fmt_int(kpis["nb_metiers"])), unsafe_allow_html=True)
c4.markdown(kpi_card("Régions couvertes", fmt_int(kpis["nb_regions"])), unsafe_allow_html=True)

salaire_bas = kpis.get("salaire_min_moyen")
salaire_haut = kpis.get("salaire_max_moyen")
declare = kpis.get("nb_offres_salaire_declare", 0)
estime = kpis.get("nb_offres_salaire_estime", 0)
total_sal = declare + estime
part_declare = f"{declare / total_sal:.0%} déclarés" if total_sal else "—"

st.markdown("")
c5, c6, c7 = st.columns(3)
c5.markdown(
    kpi_card("Fourchette salariale moyenne", f"{fmt_euro(salaire_bas)} – {fmt_euro(salaire_haut)}",
             "annuel brut, toutes offres"),
    unsafe_allow_html=True,
)
c6.markdown(
    kpi_card("Fiabilité des salaires", part_declare,
             f"{fmt_int(declare)} déclarés / {fmt_int(estime)} estimés"),
    unsafe_allow_html=True,
)
c7.markdown(
    kpi_card("Dernière publication", kpis.get("date_max", "—"),
             f"depuis {kpis.get('date_min', '—')}"),
    unsafe_allow_html=True,
)

st.divider()

# --- Vues nationales ---------------------------------------------------------
st.subheader("🗺️ Le marché en un coup d'œil")

tab_sal, tab_ent, tab_sec, tab_comp = st.tabs(
    ["Salaires par département", "Qui recrute où", "Secteurs qui recrutent", "Compétences par métier"]
)

with tab_sal:
    st.caption("Salaire moyen par département (annuel brut). Passez la souris pour le détail.")
    mesure = st.radio("Mesure", ["Moyen", "Médian"], horizontal=True, key="home_sal_mesure")
    vkey = "salaire_moyen" if mesure == "Moyen" else "salaire_median"
    try:
        sal_dep = get_salaires_dimension({}, dimension="departement", limit=110)
    except ApiError as exc:
        st.error(f"Erreur : {exc}"); sal_dep = []
    ok = choropleth(
        sal_dep, kind="departements",
        title=f"Salaire {mesure.lower()} par département",
        value_key=vkey, value_label="Salaire (€)",
        color_scale=["#DBEAFE", "#93C5FD", "#F59E0B", "#B45309"],
        hover_format=",.0f",
    )
    if not ok and sal_dep:
        charts.salaire_bar(sal_dep, f"Salaire {mesure.lower()} par département", label_key="label", top_n=20)

with tab_ent:
    st.caption("L'entreprise qui publie le plus d'offres dans chaque département.")
    try:
        top_ent = get_top_entreprises_departement({}, limit_par_dep=1)
    except ApiError as exc:
        st.error(f"Erreur : {exc}"); top_ent = []
    ok = choropleth(
        top_ent, kind="departements",
        title="Nombre d'offres du 1er recruteur, par département",
        value_key="nb_offres", value_label="Offres du 1er recruteur",
    )
    if top_ent:
        import pandas as pd
        df_ent = pd.DataFrame(top_ent)[["label", "entreprise", "nb_offres"]]
        df_ent.columns = ["Département", "Entreprise n°1", "Offres"]
        with st.expander("Voir le détail par département"):
            st.dataframe(df_ent, use_container_width=True, hide_index=True)

with tab_sec:
    st.caption("Les secteurs d'activité qui concentrent le plus d'offres.")
    try:
        secteurs = get_secteurs_repartition({})
    except ApiError as exc:
        st.error(f"Erreur : {exc}"); secteurs = []
    charts.bar(secteurs, "Offres par secteur", label_key="label", horizontal=True, top_n=15, color=PRIMARY)

with tab_comp:
    st.caption("Les compétences les plus demandées, ventilées par métier.")
    try:
        comp_metier = get_competences_repartition({}, group_by="metier", limit=40)
    except ApiError as exc:
        st.error(f"Erreur : {exc}"); comp_metier = []
    if comp_metier:
        import pandas as pd
        df = pd.DataFrame(comp_metier)
        # group_by=metier renvoie une colonne 'categorie_ventilation' = le métier
        if "categorie_ventilation" in df.columns:
            metiers_dispo = sorted(df["categorie_ventilation"].dropna().unique())
            metier_sel = st.selectbox("Métier", metiers_dispo, key="home_comp_metier")
            sous = df[df["categorie_ventilation"] == metier_sel].nlargest(12, "nb_offres")
            charts.bar(sous.to_dict("records"), f"Top compétences — {metier_sel}",
                       label_key="competence", horizontal=True, color=ACCENT)
        else:
            charts.bar(comp_metier, "Top compétences", label_key="competence", horizontal=True, top_n=12)
    else:
        st.info("Aucune donnée de compétence disponible.")

st.divider()

# --- Orientation par persona -------------------------------------------------
st.subheader("Trois lectures du même marché")
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown("### 🔍 Chercheur d'emploi")
    st.markdown(
        "Où postuler, quelles compétences acquérir, à quel salaire prétendre. "
        "Ciblez votre métier et votre région pour un plan d'action concret."
    )
with col2:
    st.markdown("### 👔 RH / Recruteur")
    st.markdown(
        "Où est la tension, êtes-vous compétitif sur les salaires, quelles "
        "compétences sont différenciantes. Benchmarkez votre recrutement."
    )
with col3:
    st.markdown("### 📈 Analyste / Marché")
    st.markdown(
        "Tendances de fond, structure sectorielle, comparaison entre sources. "
        "La vue macro du marché de l'emploi Data."
    )

st.info("Sélectionnez un espace dans le menu de gauche pour commencer.", icon="💡")
