"""
Dashboard Job Market Pipeline par audien
Lancement : streamlit run app.py
Nécessite l'API en local : uvicorn main:app --port 8000 (depuis le dossier api/)
"""

import streamlit as st
import requests
import re
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(page_title="Job Market Pipeline", page_icon="📊", layout="wide")

PALETTE = ["#4F46E5", "#0EA5E9", "#14B8A6", "#F59E0B", "#EC4899", "#7C3AED", "#84CC16", "#F97316"]
API_BASE_URL = "http://localhost:8000"

# ---------------------------------------------------------------------------
# Appel API générique
# ---------------------------------------------------------------------------

def call_api(endpoint: str, metiers=None, contrats=None, experiences=None, formations=None, regions=None, **extra):
    params = {}
    if metiers:
        params["metiers"] = metiers
    if contrats:
        params["contrats"] = contrats
    if experiences:
        params["experiences"] = experiences
    if formations:
        params["formations"] = formations
    if regions:
        params["regions"] = regions
    params.update(extra)

    try:
        response = requests.get(f"{API_BASE_URL}{endpoint}", params=params, timeout=60)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.ConnectionError:
        st.error(
            f"Impossible de contacter l'API sur {API_BASE_URL}. "
            "Vérifiez qu'elle est bien lancée (`uvicorn main:app --port 8000` depuis le dossier `api/`)."
        )
        st.stop()
    except requests.exceptions.HTTPError as e:
        st.error(f"Erreur de l'API sur {endpoint} : {e}")
        st.stop()


@st.cache_data(ttl=1800, show_spinner=False)
def load_filter_options(endpoint: str) -> list[str]:
    return call_api(endpoint)


@st.cache_data(ttl=86400, show_spinner=False)
def load_departments_geojson() -> dict:
    url = "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/departements.geojson"
    return requests.get(url, timeout=15).json()


@st.cache_data(ttl=1800, show_spinner=False)
def load_kpis(metiers, contrats, experiences, formations, regions) -> dict:
    return call_api("/kpis", metiers, contrats, experiences, formations, regions)


@st.cache_data(ttl=1800, show_spinner="Chargement de la carte…")
def load_offers_by_department(metiers, contrats, experiences, formations, regions) -> pd.DataFrame:
    data = call_api("/offres/par-departement", metiers, contrats, experiences, formations, regions)
    if not data:
        return pd.DataFrame(columns=["departement", "nb_offres"])
    return pd.DataFrame(data)


@st.cache_data(ttl=1800, show_spinner="Chargement de l'évolution…")
def load_offers_trend(metiers, contrats, experiences, formations, regions) -> pd.DataFrame:
    data = call_api("/offres/evolution", metiers, contrats, experiences, formations, regions)
    if not data:
        return pd.DataFrame(columns=["semaine", "nb_offres"])
    df = pd.DataFrame(data)
    df["semaine"] = pd.to_datetime(df["semaine"])
    return df


@st.cache_data(ttl=1800, show_spinner="Chargement des salaires…")
def load_salary_by_sector(metiers, contrats, experiences, formations, regions) -> pd.DataFrame:
    return pd.DataFrame(call_api("/salaires/par-secteur", metiers, contrats, experiences, formations, regions))


@st.cache_data(ttl=1800, show_spinner="Chargement des compétences…")
def load_top_skills(metiers, contrats, experiences, formations, regions, limite: int = 15) -> pd.DataFrame:
    return pd.DataFrame(
        call_api("/competences/top", metiers, contrats, experiences, formations, regions, limite=limite)
    )


@st.cache_data(ttl=1800, show_spinner="Chargement des entreprises…")
def load_top_companies(metiers, contrats, experiences, formations, regions, limite: int = 10) -> pd.DataFrame:
    return pd.DataFrame(
        call_api("/entreprises/top", metiers, contrats, experiences, formations, regions, limite=limite)
    )


@st.cache_data(ttl=1800, show_spinner="Chargement des salaires…")
def load_salary_by_department(metiers, contrats, experiences, formations, regions) -> pd.DataFrame:
    data = call_api("/salaires/par-departement", metiers, contrats, experiences, formations, regions)
    if not data:
        return pd.DataFrame(columns=["departement", "salaire_moyen", "nb_offres"])
    return pd.DataFrame(data)


@st.cache_data(ttl=1800, show_spinner="Chargement des villes…")
def load_top_cities(metiers, contrats, experiences, formations, regions, limite: int = 15) -> pd.DataFrame:
    return pd.DataFrame(call_api("/villes/top", metiers, contrats, experiences, formations, regions, limite=limite))


@st.cache_data(ttl=1800, show_spinner="Chargement des profils…")
def load_level_distribution(colonne: str, metiers, contrats, experiences, formations, regions) -> pd.DataFrame:
    return pd.DataFrame(
        call_api("/profil/repartition", metiers, contrats, experiences, formations, regions, colonne=colonne)
    )


@st.cache_data(ttl=1800, show_spinner="Chargement des salaires par contrat…")
def load_salary_by_contract_experience(metiers, contrats, experiences, formations, regions) -> pd.DataFrame:
    data = call_api("/salaires/contrat-experience", metiers, contrats, experiences, formations, regions)
    if not data:
        return pd.DataFrame(columns=["nom_contrat", "niveau_experience", "salaire_moyen", "nb_offres"])
    return pd.DataFrame(data)


@st.cache_data(ttl=1800, show_spinner="Chargement des salaires par compétence…")
def load_salary_by_skill(metiers, contrats, experiences, formations, regions, limite: int = 30) -> pd.DataFrame:
    return pd.DataFrame(
        call_api("/salaires/par-competence", metiers, contrats, experiences, formations, regions, limite=limite)
    )


@st.cache_data(ttl=1800, show_spinner="Chargement des distributions de salaires…")
def load_raw_salaries_by_experience(metiers, contrats, experiences, formations, regions) -> pd.DataFrame:
    return pd.DataFrame(
        call_api("/salaires/bruts-par-experience", metiers, contrats, experiences, formations, regions)
    )


@st.cache_data(ttl=1800, show_spinner="Chargement des salaires par métier…")
def load_raw_salaries_by_job_title(
    metiers, contrats, experiences, formations, regions, limite_metiers: int = 10
) -> pd.DataFrame:
    data = call_api(
        "/salaires/par-metier-brut",
        metiers, contrats, experiences, formations, regions,
        limite_metiers=limite_metiers,
    )
    if not data:
        return pd.DataFrame(columns=["nom_metier", "salaire"])
    return pd.DataFrame(data)


@st.cache_data(ttl=1800, show_spinner="Chargement des métiers…")
def load_junior_positions(metiers, contrats, experiences, formations, regions, limite: int = 15) -> pd.DataFrame:
    data = call_api(
        "/metiers/postes-juniors", metiers, contrats, experiences, formations, regions, limite=limite
    )
    if not data:
        return pd.DataFrame(columns=["nom_metier", "nb_offres"])
    return pd.DataFrame(data)


@st.cache_data(ttl=1800, show_spinner="Chargement des stages/alternances…")
def load_internships(metiers, contrats, experiences, formations, regions, limite: int = 15) -> pd.DataFrame:
    data = call_api(
        "/metiers/stage-alternance", metiers, contrats, experiences, formations, regions, limite=limite
    )
    if not data:
        return pd.DataFrame(columns=["nom_metier", "nom_contrat", "nb_offres"])
    return pd.DataFrame(data)

# Tri des niveau de formation de la valeur la plus petite à la plus grande en fonction du chiffre : +2, +5, ... 
def education_level_sort_key(valeur: str) -> int:
    match = re.search(r"\+(\d+)", str(valeur))
    return int(match.group(1)) if match else -1


# ---------------------------------------------------------------------------
# Blocs de visualisation réutilisables (un bloc = une fonction, appelée
# depuis plusieurs pages selon l'audience concernée).
# ---------------------------------------------------------------------------

def display_chart(fig):
    """Affiche un graphique Plotly."""
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="Segoe UI, Helvetica, Arial, sans-serif", color="#1E293B", size=13),
    )
    with st.container(border=True):
        st.plotly_chart(fig, use_container_width=True)


def render_offers_map(filtres):
    st.subheader("📍 Répartition des offres par département")

    geojson = load_departments_geojson()
    df_dept = load_offers_by_department(*filtres)

    noms_departements_fr = sorted({f["properties"]["nom"] for f in geojson["features"]})
    est_france = df_dept["departement"].isin(noms_departements_fr)
    df_france_brut = df_dept[est_france]
    df_hors_france = df_dept[~est_france]

    df_france = (
        pd.DataFrame({"departement": noms_departements_fr})
        .merge(df_france_brut, on="departement", how="left")
        .fillna({"nb_offres": 0})
    )
    df_france["nb_offres"] = df_france["nb_offres"].astype(int)
    df_france["rang_pct"] = df_france["nb_offres"].rank(pct=True) 

    fig = px.choropleth(
        df_france,
        geojson=geojson,
        locations="departement",
        featureidkey="properties.nom",
        color="rang_pct",
        color_continuous_scale="Purples",
        scope="europe",
        hover_name="departement",
        hover_data={"nb_offres": True, "rang_pct": False, "departement": False},
    )
    fig.update_geos(fitbounds="locations", visible=False)
    fig.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        coloraxis_colorbar=dict(title="Offres<br>(rang)", tickvals=[0, 0.5, 1], ticktext=["Peu", "Moyen", "Beaucoup"]),
    )
    display_chart(fig)

    total_hors_france = int(df_hors_france["nb_offres"].sum())
    if total_hors_france > 0:
        st.metric("Offres hors France", f"{total_hors_france:,}".replace(",", " "))
        with st.expander("Détail des offres hors France"):
            st.dataframe(
                df_hors_france.sort_values("nb_offres", ascending=False),
                use_container_width=True,
                hide_index=True,
            )


def render_offers_trend(filtres):
    st.subheader("📈 Évolution des offres publiées")

    df_evolution = load_offers_trend(*filtres)

    if df_evolution.empty:
        st.info("Pas assez de données pour ce filtre.")
        return

    fig_evolution = px.line(
        df_evolution,
        x="semaine",
        y="nb_offres",
        markers=True,
        color_discrete_sequence=[PALETTE[0]],
        labels={"semaine": "Semaine", "nb_offres": "Nombre d'offres publiées"},
    )
    fig_evolution.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    display_chart(fig_evolution)


def render_salary_by_sector(filtres):
    st.subheader("💰 Salaire moyen par secteur (top 10)")

    df_salaire = load_salary_by_sector(*filtres)

    if df_salaire.empty:
        st.info("Pas assez de données de salaire pour ce filtre.")
        return

    fig_salaire = px.bar(
        df_salaire,
        x="salaire_moyen",
        y="nom_secteur",
        orientation="h",
        text="salaire_moyen",
        color_discrete_sequence=[PALETTE[0]],
        labels={"salaire_moyen": "Salaire moyen (€)", "nom_secteur": ""},
    )
    fig_salaire.update_traces(texttemplate="%{text:,.0f} €", textposition="outside", cliponaxis=False)
    fig_salaire.update_layout(
        yaxis={"categoryorder": "total ascending"},
        xaxis={"visible": False},
        margin={"r": 40, "t": 0, "l": 0, "b": 0},
    )
    display_chart(fig_salaire)


def render_salary_map(filtres):
    st.subheader("💰 Salaire moyen par département")

    geojson = load_departments_geojson()
    df_salaire_dept = load_salary_by_department(*filtres)

    noms_departements_fr = sorted({f["properties"]["nom"] for f in geojson["features"]})
    df_salaire_dept_complet = pd.DataFrame({"departement": noms_departements_fr}).merge(
        df_salaire_dept, on="departement", how="left"
    )

    fig_salaire_carte = go.Figure()
    fig_salaire_carte.add_trace(
        go.Choropleth(
            geojson=geojson,
            locations=noms_departements_fr,
            z=[0] * len(noms_departements_fr),
            featureidkey="properties.nom",
            colorscale=[[0, "#F1F5F9"], [1, "#F1F5F9"]],
            showscale=False,
            marker_line_color="#94A3B8",
            marker_line_width=0.5,
            hoverinfo="skip",
        )
    )
    df_avec_donnee = df_salaire_dept_complet.dropna(subset=["salaire_moyen"])
    fig_salaire_carte.add_trace(
        go.Choropleth(
            geojson=geojson,
            locations=df_avec_donnee["departement"],
            z=df_avec_donnee["salaire_moyen"],
            featureidkey="properties.nom",
            colorscale="Purples",
            colorbar_title="Salaire<br>moyen (€)",
            marker_line_color="#94A3B8",
            marker_line_width=0.5,
            hovertemplate="%{location}<br>Salaire moyen : %{z:.0f} €<extra></extra>",
        )
    )
    fig_salaire_carte.update_geos(fitbounds="locations", visible=False, scope="europe")
    fig_salaire_carte.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    display_chart(fig_salaire_carte)


def render_salary_range_by_job_title(filtres, limite_metiers: int = 10):
    st.subheader("📊 Fourchettes salariales par métier")
    st.caption(
        f"Top {limite_metiers} métiers les plus représentés. Chaque boîte montre la "
        "médiane et les 1er/3e quartiles — utile pour situer une offre par rapport au marché."
    )

    df_brut = load_raw_salaries_by_job_title(*filtres, limite_metiers=limite_metiers)

    if df_brut.empty:
        st.info("Pas assez de données pour ce filtre.")
        return

    ordre_metiers = (
        df_brut.groupby("nom_metier")["salaire"].median().sort_values(ascending=False).index.tolist()
    )

    fig_fourchettes = px.box(
        df_brut,
        x="salaire",
        y="nom_metier",
        orientation="h",
        points=False,
        category_orders={"nom_metier": ordre_metiers},
        color_discrete_sequence=[PALETTE[0]],
        labels={"salaire": "Salaire (€)", "nom_metier": ""},
    )
    fig_fourchettes.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    display_chart(fig_fourchettes)


def render_top_skills(filtres):
    st.subheader("🛠️ Top compétences recherchées")

    df_competences = load_top_skills(*filtres)

    if df_competences.empty:
        st.info("Pas assez de données de compétences pour ce filtre.")
        return

    fig_competences = px.bar(
        df_competences,
        x="nb_offres",
        y="nom_competence",
        color="categorie_competence",
        orientation="h",
        text="nb_offres",
        color_discrete_sequence=PALETTE,
        labels={"nb_offres": "Nombre d'offres", "nom_competence": "", "categorie_competence": "Catégorie"},
    )
    fig_competences.update_traces(textposition="outside", cliponaxis=False)
    fig_competences.update_layout(
        yaxis={"categoryorder": "total ascending"},
        xaxis={"visible": False},
        margin={"r": 30, "t": 0, "l": 0, "b": 0},
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
    )
    display_chart(fig_competences)


def render_skills_table(filtres, limite: int = 50):
    st.subheader("🔍 Paysage des compétences (niches incluses)")
    st.caption(
        "Cliquez sur l'en-tête « Nombre d'offres » pour trier — les compétences en "
        "bas de liste (peu d'offres) sont les niches, présentes mais peu répandues."
    )

    df_competences = load_top_skills(*filtres, limite=limite)

    if df_competences.empty:
        st.info("Pas assez de données de compétences pour ce filtre.")
        return

    df_affichage = (
        df_competences.rename(
            columns={
                "categorie_competence": "Catégorie",
                "nom_competence": "Compétence",
                "nb_offres": "Nombre d'offres",
            }
        )[["Catégorie", "Compétence", "Nombre d'offres"]]
        .sort_values("Nombre d'offres", ascending=False)
    )

    st.dataframe(
        df_affichage,
        use_container_width=True,
        hide_index=True,
        height=500,
        column_config={
            "Nombre d'offres": st.column_config.ProgressColumn(
                "Nombre d'offres",
                min_value=0,
                max_value=int(df_affichage["Nombre d'offres"].max()),
                format="%d",
            )
        },
    )


def render_top_companies(filtres):
    st.subheader("🏢 Top entreprises qui recrutent")

    df_entreprises = load_top_companies(*filtres)

    if df_entreprises.empty:
        st.info("Pas assez de données d'entreprises pour ce filtre.")
        return

    fig_entreprises = px.bar(
        df_entreprises,
        x="nb_offres",
        y="nom_entreprise",
        orientation="h",
        text="nb_offres",
        color_discrete_sequence=[PALETTE[1]],
        labels={"nb_offres": "Nombre d'offres", "nom_entreprise": ""},
    )
    fig_entreprises.update_traces(textposition="outside", cliponaxis=False)
    fig_entreprises.update_layout(
        yaxis={"categoryorder": "total ascending"},
        xaxis={"visible": False},
        margin={"r": 30, "t": 0, "l": 0, "b": 0},
    )
    display_chart(fig_entreprises)


def render_top_cities(filtres):
    st.subheader("🏙️ Top villes qui recrutent")

    df_villes = load_top_cities(*filtres)

    if df_villes.empty:
        st.info("Pas assez de données pour ce filtre.")
        return

    fig_villes = px.bar(
        df_villes,
        x="nb_offres",
        y="ville",
        orientation="h",
        text="nb_offres",
        color_discrete_sequence=[PALETTE[0]],
        labels={"nb_offres": "Nombre d'offres", "ville": ""},
    )
    fig_villes.update_traces(textposition="outside", cliponaxis=False)
    fig_villes.update_layout(
        yaxis={"categoryorder": "total ascending"},
        xaxis={"visible": False},
        margin={"r": 30, "t": 0, "l": 0, "b": 0},
    )
    display_chart(fig_villes)


def render_contract_experience_heatmap(filtres):
    st.subheader("📄 Salaire moyen par contrat et niveau d'expérience")

    df_croise_contrat = load_salary_by_contract_experience(*filtres)

    if df_croise_contrat.empty:
        st.info("Pas assez de données pour ce filtre.")
        return

    pivot_contrat = df_croise_contrat.pivot(
        index="nom_contrat", columns="niveau_experience", values="salaire_moyen"
    )

    ordre_experience = ["Junior", "Confirmé", "Senior", "Expert"]
    colonnes_exp = [n for n in ordre_experience if n in pivot_contrat.columns]
    colonnes_exp += [c for c in pivot_contrat.columns if c not in ordre_experience]
    pivot_contrat = pivot_contrat[colonnes_exp]

    ordre_contrats = pivot_contrat.mean(axis=1).sort_values(ascending=False).index
    pivot_contrat = pivot_contrat.loc[ordre_contrats]

    fig_heatmap_contrat = px.imshow(
        pivot_contrat,
        color_continuous_scale="Purples",
        labels={"x": "Expérience", "y": "", "color": "Salaire moyen (€)"},
        text_auto=".0f",
        aspect="auto",
    )
    fig_heatmap_contrat.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    display_chart(fig_heatmap_contrat)


def render_skills_scatter(filtres):
    st.subheader("🔗 Salaire moyen par compétence (top 30 les plus demandées)")
    st.caption(
        "Chaque point est une compétence : à droite = très demandée, en haut = "
        "bien rémunérée. Les compétences en haut à droite cumulent les deux."
    )

    df_salaire_competence = load_salary_by_skill(*filtres)

    if df_salaire_competence.empty:
        st.info("Pas assez de données pour ce filtre.")
        return

    fig_scatter = px.scatter(
        df_salaire_competence,
        x="nb_offres",
        y="salaire_moyen",
        color="categorie_competence",
        text="nom_competence",
        color_discrete_sequence=PALETTE,
        labels={
            "nb_offres": "Nombre d'offres (demande)",
            "salaire_moyen": "Salaire moyen (€)",
            "categorie_competence": "Catégorie",
        },
    )
    fig_scatter.update_traces(textposition="top center", marker=dict(size=10))
    fig_scatter.add_hline(y=df_salaire_competence["salaire_moyen"].median(), line_dash="dot", line_color="rgba(0,0,0,0.3)")
    fig_scatter.add_vline(x=df_salaire_competence["nb_offres"].median(), line_dash="dot", line_color="rgba(0,0,0,0.3)")
    fig_scatter.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0}, height=550)
    display_chart(fig_scatter)

    with st.expander("📖 Comment lire ce graphique ?"):
        st.markdown(
            """
            Chaque point représente une compétence, positionnée selon deux axes :
            - **Axe horizontal** : le nombre d'offres qui la demandent (la *demande*).
            - **Axe vertical** : le salaire moyen des offres qui la demandent.

            Les deux lignes pointillées marquent la **médiane** de l'échantillon
            affiché. Elles découpent le graphique en 4 zones :

            - **🔝 En haut à droite** — compétences à la fois très demandées et bien
              rémunérées, les plus stratégiques.
            - **↖️ En haut à gauche** — peu demandées mais bien payées : niche.
            - **↘️ En bas à droite** — très demandées mais salaire dans la fourchette
              basse : compétences "de base".
            - **↙️ En bas à gauche** — ni demandées, ni rémunératrices.

            ⚠️ Ces repères sont relatifs à l'échantillon affiché, pas des seuils
            absolus du marché.
            """
        )


def render_salary_range_boxplot(filtres):
    st.subheader("📦 Amplitude de la fourchette salariale par expérience")
    st.caption(
        "Pour chaque offre, on calcule l'écart entre salaire max et salaire min "
        "proposés, puis on regarde comment cet écart varie selon l'expérience demandée."
    )

    df_brut = load_raw_salaries_by_experience(*filtres)

    if df_brut.empty:
        st.info("Pas assez de données pour ce filtre.")
        return

    df_brut = df_brut.copy()
    df_brut["amplitude"] = df_brut["salaire_max"] - df_brut["salaire_min"]

    ordre_experience_box = ["Junior", "Confirmé", "Senior", "Expert"]
    ordre_present = [n for n in ordre_experience_box if n in df_brut["niveau_experience"].unique()]
    ordre_present += [n for n in df_brut["niveau_experience"].unique() if n not in ordre_experience_box]

    fig_box = px.box(
        df_brut,
        x="niveau_experience",
        y="amplitude",
        points=False,
        category_orders={"niveau_experience": ordre_present},
        color_discrete_sequence=[PALETTE[0]],
        labels={"niveau_experience": "Expérience", "amplitude": "Amplitude (€, max - min)"},
    )
    fig_box.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    display_chart(fig_box)

    st.caption(
        "Chaque boîte montre la médiane (trait central) et les 1er/3e quartiles "
        "(bords de la boîte) — donc où se situent 50% des offres pour ce niveau d'expérience."
    )


def render_junior_positions(filtres):
    st.subheader("🌱 Répartition des postes pour les juniors")

    df_juniors = load_junior_positions(*filtres)

    if df_juniors.empty:
        st.info("Pas assez de données pour ce filtre.")
        return

    fig_juniors = px.treemap(
        df_juniors,
        path=["nom_metier"],
        values="nb_offres",
        color_discrete_sequence=PALETTE,
    )
    fig_juniors.update_traces(textinfo="label+value", texttemplate="%{label}<br>%{value} offres")
    fig_juniors.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    display_chart(fig_juniors)


def render_internships(filtres):
    st.subheader("🎒 Offres de stage et d'alternance par métier")

    df_sa = load_internships(*filtres)

    if df_sa.empty:
        st.info(
            "Pas assez de données pour ce filtre (ou les libellés 'Stage'/'Alternance' "
            "diffèrent de ce qui est attendu)."
        )
        return

    fig_sa = px.bar(
        df_sa,
        x="nb_offres",
        y="nom_metier",
        color="nom_contrat",
        orientation="h",
        text="nb_offres",
        color_discrete_sequence=[PALETTE[3], PALETTE[4]],
        labels={"nb_offres": "Nombre d'offres", "nom_metier": "", "nom_contrat": "Contrat"},
    )
    fig_sa.update_traces(textposition="outside", cliponaxis=False)
    fig_sa.update_layout(
        yaxis={"categoryorder": "total ascending"},
        xaxis={"visible": False},
        margin={"r": 30, "t": 0, "l": 0, "b": 0},
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0),
    )
    display_chart(fig_sa)


def render_education_level_distribution(filtres):
    st.subheader("🎓 Niveau de formation demandé")

    df_niveau = load_level_distribution("niveau_formation", *filtres)

    if df_niveau.empty:
        st.info("Pas assez de données pour ce filtre.")
        return

    df_niveau = df_niveau.copy()
    est_non_renseigne = df_niveau["niveau"].str.upper().str.strip() == "NON RENSEIGNÉ"
    df_non_renseigne = df_niveau[est_non_renseigne]
    df_connu = df_niveau[~est_non_renseigne]

    if not df_non_renseigne.empty:
        total = df_niveau["nb_offres"].sum()
        nb_non_renseigne = int(df_non_renseigne["nb_offres"].sum())
        st.caption(
            f"ℹ️ {nb_non_renseigne} offres sur {int(total)} ne précisent pas de niveau "
            "de formation — exclues du donut ci-dessous pour rester lisible sur les vrais niveaux."
        )

    if df_connu.empty:
        st.info("Pas de niveau de formation connu pour ce filtre.")
        return

    ordre_formation = sorted(df_connu["niveau"].unique(), key=education_level_sort_key)

    fig_niveau = px.pie(
        df_connu,
        names="niveau",
        values="nb_offres",
        hole=0.4,
        category_orders={"niveau": ordre_formation},
        color_discrete_sequence=PALETTE,
    )
    fig_niveau.update_traces(textinfo="percent", textposition="inside")
    fig_niveau.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
    display_chart(fig_niveau)


# ---------------------------------------------------------------------------
# Sidebar : navigation + filtres
# ---------------------------------------------------------------------------

PAGES = {
    "Overview": "🏠",
    "Candidat": "🎯",
    "Recruteur": "🏢",
    "École": "🎓",
}

if "page" not in st.session_state:
    st.session_state.page = list(PAGES)[0]

with st.sidebar:
    st.markdown("## 📊 Job Market")
    st.caption("Suivi du marché de l'emploi")

    st.markdown(
        """
        <style>
        [data-testid="stSidebar"] .stButton > button {
            text-align: left;
            justify-content: flex-start;
            border: none;
            background-color: transparent;
            color: #1E293B;
            font-weight: 500;
            padding: 0.5rem 0.75rem;
            box-shadow: none;
        }
        [data-testid="stSidebar"] .stButton > button:hover {
            background-color: rgba(79, 70, 229, 0.08);
            color: #4F46E5;
        }
        [data-testid="stSidebar"] .stButton > button[kind="primary"] {
            background-color: rgba(79, 70, 229, 0.12);
            color: #4F46E5;
            border-left: 3px solid #4F46E5;
            box-shadow: none;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    for nom_page, icone in PAGES.items():
        est_active = st.session_state.page == nom_page
        if st.button(
            f"{icone}  {nom_page}",
            key=f"nav_{nom_page}",
            use_container_width=True,
            type="primary" if est_active else "secondary",
        ):
            st.session_state.page = nom_page

    page = st.session_state.page

    st.markdown("---")
    st.markdown("**Filtres** *(sélection multiple possible)*")

    metiers = load_filter_options("/filtres/metiers")
    metiers_filtre = st.multiselect("Métier", metiers, key="filtre_metiers") or None

    contrats = load_filter_options("/filtres/contrats")
    contrats_filtre = st.multiselect("Contrat", contrats, key="filtre_contrats") or None

    experiences = load_filter_options("/filtres/experiences")
    experiences_filtre = st.multiselect("Expérience", experiences, key="filtre_experiences") or None

    formations = load_filter_options("/filtres/formations")
    formations_filtre = st.multiselect("Formation", formations, key="filtre_formations") or None

    regions = load_filter_options("/filtres/regions")
    regions_filtre = st.multiselect("Localisation (région)", regions, key="filtre_regions") or None

FILTRES = (metiers_filtre, contrats_filtre, experiences_filtre, formations_filtre, regions_filtre)


# ---------------------------------------------------------------------------
# Page : Overview
# ---------------------------------------------------------------------------

if page == "Overview":
    st.title("Overview du marché de l'emploi")

    kpis = load_kpis(*FILTRES)

    df_dept_apercu = load_offers_by_department(*FILTRES)
    df_salaire_apercu = load_salary_by_sector(*FILTRES)
    df_competences_apercu = load_top_skills(*FILTRES)

    morceaux_synthese = []
    if kpis and kpis.get("total_offres") is not None:
        morceaux_synthese.append(f"**{int(kpis['total_offres']):,}** offres actuellement".replace(",", " "))
    if not df_dept_apercu.empty:
        top_dept = df_dept_apercu.sort_values("nb_offres", ascending=False).iloc[0]["departement"]
        morceaux_synthese.append(f"concentrées surtout en **{top_dept}**")
    if not df_salaire_apercu.empty:
        top_secteur = df_salaire_apercu.iloc[0]
        morceaux_synthese.append(
            f"le secteur **{top_secteur['nom_secteur']}** paie le mieux "
            f"(**{int(top_secteur['salaire_moyen']):,} €**)".replace(",", " ")
        )
    if not df_competences_apercu.empty:
        top_competence = df_competences_apercu.iloc[0]["nom_competence"]
        morceaux_synthese.append(f"la compétence la plus demandée est **{top_competence}**")

    if morceaux_synthese:
        st.info(", ".join(morceaux_synthese) + ".")

    st.divider()

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        with st.container(border=True):
            st.metric("Offres totales", f"{int(kpis['total_offres']):,}".replace(",", " "))
    with col2:
        with st.container(border=True):
            st.metric("Entreprises distinctes", f"{int(kpis['nb_entreprises']):,}".replace(",", " "))
    with col3:
        with st.container(border=True):
            st.metric("Métiers distincts", f"{int(kpis['nb_metiers']):,}".replace(",", " "))
    with col4:
        with st.container(border=True):
            salaire = kpis["salaire_median"]
            valeur = f"{int(salaire):,} €".replace(",", " ") if salaire is not None else "N/A"
            st.metric("Salaire médian", valeur)

    st.divider()
    render_offers_trend(FILTRES)

    st.divider()
    col_carte, col_salaire = st.columns(2)
    with col_carte:
        render_offers_map(FILTRES)
    with col_salaire:
        render_salary_by_sector(FILTRES)

    st.divider()
    col_competences, col_entreprises = st.columns(2)
    with col_competences:
        render_top_skills(FILTRES)
    with col_entreprises:
        render_top_companies(FILTRES)


# ---------------------------------------------------------------------------
# Page : Candidat
# ---------------------------------------------------------------------------

elif page == "Candidat":
    st.title("Pour les candidats")
    st.caption("Où sont les offres, ce qu'elles paient, et quelles compétences mettre en avant.")

    col1, col2 = st.columns(2)
    with col1:
        render_offers_map(FILTRES)
    with col2:
        render_top_cities(FILTRES)

    st.divider()
    render_contract_experience_heatmap(FILTRES)

    st.divider()
    render_skills_scatter(FILTRES)


# ---------------------------------------------------------------------------
# Page : Recruteur
# ---------------------------------------------------------------------------

elif page == "Recruteur":
    st.title("Pour les recruteurs")
    st.caption("Benchmarker ses offres, aligner ses fiches de poste, situer la concurrence.")

    col1, col2 = st.columns(2)
    with col1:
        render_salary_by_sector(FILTRES)
    with col2:
        render_salary_map(FILTRES)

    st.divider()
    render_salary_range_by_job_title(FILTRES)

    st.divider()
    render_skills_table(FILTRES)

    st.divider()
    render_offers_trend(FILTRES)

    st.divider()
    render_top_companies(FILTRES)


# ---------------------------------------------------------------------------
# Page : École
# ---------------------------------------------------------------------------

elif page == "École":
    st.title("Pour les écoles et organismes de formation")
    st.caption("Adapter les cursus aux besoins réels du marché.")

    render_top_skills(FILTRES)

    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        render_junior_positions(FILTRES)
    with col2:
        render_education_level_distribution(FILTRES)

    st.divider()
    render_internships(FILTRES)


else:
    st.title(page)
    st.info("🚧 Cette page arrive dans une prochaine étape.")