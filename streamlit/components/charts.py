"""Graphiques réutilisables du dashboard, tous stylés via `components.theme`.

Chaque fonction gère l'état vide (message dédié plutôt qu'un graphique blanc) et
accepte une clé de label configurable pour s'adapter aux différents endpoints.
"""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from components.theme import ACCENT, COLOR_SEQUENCE, PLATFORM_COLORS, PRIMARY, apply_plotly_theme


def _empty(title: str) -> None:
    st.info(f"Aucune donnée pour « {title} » avec les filtres actuels.")


def bar(items: list[dict], title: str, label_key: str = "label", horizontal: bool = True, top_n: int | None = None, color: str = PRIMARY) -> None:
    if not items:
        _empty(title)
        return
    df = pd.DataFrame(items)
    if label_key not in df.columns:
        st.error(f"Colonne « {label_key} » absente pour « {title} » (reçu : {list(df.columns)}).")
        return
    if top_n:
        df = df.nlargest(top_n, "nb_offres")
    if horizontal:
        df = df.sort_values("nb_offres", ascending=True)
        fig = px.bar(df, x="nb_offres", y=label_key, orientation="h", title=title)
    else:
        df = df.sort_values("nb_offres", ascending=False)
        fig = px.bar(df, x=label_key, y="nb_offres", title=title)
    fig.update_traces(marker_color=color)
    st.plotly_chart(apply_plotly_theme(fig), use_container_width=True)


def donut(items: list[dict], title: str, label_key: str = "label", top_n: int | None = None) -> None:
    if not items:
        _empty(title)
        return
    df = pd.DataFrame(items)
    if top_n:
        df = df.nlargest(top_n, "nb_offres")
    fig = px.pie(df, names=label_key, values="nb_offres", hole=0.55, title=title, color_discrete_sequence=COLOR_SEQUENCE)
    fig.update_traces(textposition="inside", textinfo="percent")
    st.plotly_chart(apply_plotly_theme(fig), use_container_width=True)


def evolution(points: list[dict], title: str, color: str = PRIMARY) -> None:
    if not points:
        _empty(title)
        return
    df = pd.DataFrame(points)
    df["periode"] = pd.to_datetime(df["annee"].astype(str) + "-" + df["mois"].astype(str) + "-01")
    df = df.sort_values("periode")
    fig = px.area(df, x="periode", y="nb_offres", title=title)
    fig.update_traces(line_color=color, fillcolor="rgba(37,99,235,0.12)")
    fig.update_xaxes(title=None)
    fig.update_yaxes(title="Offres")
    st.plotly_chart(apply_plotly_theme(fig, height=360), use_container_width=True)


def salaire_range(items: list[dict], title: str) -> None:
    """Graphique en fourchettes (min→max moyen) par métier, trié par salaire max.
    L'épaisseur/position du segment matérialise l'amplitude salariale."""
    if not items:
        _empty(title)
        return
    df = pd.DataFrame(items)
    df = df[df["salaire_max_moyen"].notna()].sort_values("salaire_max_moyen", ascending=True)
    if df.empty:
        _empty(title)
        return

    fig = go.Figure()
    for _, row in df.iterrows():
        fig.add_trace(
            go.Scatter(
                x=[row["salaire_min_moyen"], row["salaire_max_moyen"]],
                y=[row["metier"], row["metier"]],
                mode="lines",
                line=dict(color="#CBD5E1", width=6),
                showlegend=False,
                hoverinfo="skip",
            )
        )
    fig.add_trace(
        go.Scatter(
            x=df["salaire_min_moyen"], y=df["metier"], mode="markers",
            marker=dict(color=PRIMARY, size=11), name="Min moyen",
            hovertemplate="%{y}<br>Min moyen : %{x:,.0f} €<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=df["salaire_max_moyen"], y=df["metier"], mode="markers",
            marker=dict(color=ACCENT, size=11), name="Max moyen",
            hovertemplate="%{y}<br>Max moyen : %{x:,.0f} €<extra></extra>",
        )
    )
    fig.update_layout(title=title, xaxis_title="Salaire annuel brut (€)")
    st.plotly_chart(apply_plotly_theme(fig, height=max(360, 34 * len(df))), use_container_width=True)


def tension_scatter(items: list[dict], title: str) -> None:
    """Nuage nb_entreprises (x) vs nb_offres (y) ; la taille et la couleur
    encodent le ratio de tension. En haut-à-gauche = beaucoup d'offres, peu
    d'entreprises = marché tendu."""
    if not items:
        _empty(title)
        return
    df = pd.DataFrame(items)
    fig = px.scatter(
        df, x="nb_entreprises", y="nb_offres", size="offres_par_entreprise",
        color="offres_par_entreprise", text="metier",
        color_continuous_scale=["#93C5FD", "#2563EB", "#1E3A8A"],
        title=title, size_max=40,
    )
    fig.update_traces(textposition="top center", textfont_size=10)
    fig.update_layout(xaxis_title="Nombre d'entreprises", yaxis_title="Nombre d'offres",
                      coloraxis_colorbar_title="Offres/entreprise")
    st.plotly_chart(apply_plotly_theme(fig, height=460), use_container_width=True)


def grouped_platform_bar(items: list[dict], title: str) -> None:
    """Barres groupées France Travail vs WTTJ par label."""
    if not items:
        _empty(title)
        return
    df = pd.DataFrame(items)
    df["total"] = df["france_travail"] + df["wttj"]
    df = df.sort_values("total", ascending=True)
    fig = go.Figure()
    fig.add_trace(go.Bar(y=df["label"], x=df["france_travail"], name="France Travail",
                         orientation="h", marker_color=PLATFORM_COLORS["france_travail"]))
    fig.add_trace(go.Bar(y=df["label"], x=df["wttj"], name="Welcome to the Jungle",
                         orientation="h", marker_color=PLATFORM_COLORS["wttj"]))
    fig.update_layout(barmode="group", title=title, xaxis_title="Offres")
    st.plotly_chart(apply_plotly_theme(fig, height=max(360, 30 * len(df))), use_container_width=True)


def horizontal_stacked_platform(items: list[dict], title: str) -> None:
    """Barres empilées 100% pour comparer la STRUCTURE (parts relatives) entre
    plateformes par label, indépendamment du volume brut."""
    if not items:
        _empty(title)
        return
    df = pd.DataFrame(items)
    df["total"] = df["france_travail"] + df["wttj"]
    df = df[df["total"] > 0].sort_values("total", ascending=True)
    df["pct_ft"] = df["france_travail"] / df["total"] * 100
    df["pct_wttj"] = df["wttj"] / df["total"] * 100
    fig = go.Figure()
    fig.add_trace(go.Bar(y=df["label"], x=df["pct_ft"], name="France Travail",
                         orientation="h", marker_color=PLATFORM_COLORS["france_travail"]))
    fig.add_trace(go.Bar(y=df["label"], x=df["pct_wttj"], name="Welcome to the Jungle",
                         orientation="h", marker_color=PLATFORM_COLORS["wttj"]))
    fig.update_layout(barmode="stack", title=title, xaxis_title="Part (%)")
    st.plotly_chart(apply_plotly_theme(fig, height=max(360, 30 * len(df))), use_container_width=True)


def salaire_bar(items, title, label_key="label", top_n=None):
    """Barres horizontales du salaire moyen ET median par categorie (region,
    secteur, competence...). Deux series cote a cote pour comparer moyenne/mediane.
    Attend des cles: <label_key>, salaire_moyen, salaire_median."""
    if not items:
        _empty(title)
        return
    df = pd.DataFrame(items)
    if label_key not in df.columns:
        st.error(f"Colonne « {label_key} » absente pour « {title} ».")
        return
    df = df[df["salaire_moyen"].notna()]
    if df.empty:
        _empty(title)
        return
    if top_n:
        df = df.nlargest(top_n, "salaire_moyen")
    df = df.sort_values("salaire_moyen", ascending=True)
    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=df[label_key], x=df["salaire_moyen"], name="Moyen",
        orientation="h", marker_color=PRIMARY,
        hovertemplate="%{y}<br>Moyen : %{x:,.0f} €<extra></extra>",
    ))
    if "salaire_median" in df.columns:
        fig.add_trace(go.Bar(
            y=df[label_key], x=df["salaire_median"], name="Médian",
            orientation="h", marker_color=ACCENT,
            hovertemplate="%{y}<br>Médian : %{x:,.0f} €<extra></extra>",
        ))
    fig.update_layout(barmode="group", title=title, xaxis_title="Salaire annuel brut (€)")
    st.plotly_chart(apply_plotly_theme(fig, height=max(360, 30 * len(df))), use_container_width=True)


def evolution_periode(points, title, color=PRIMARY):
    """Courbe d'evolution a partir d'une liste [{periode: 'YYYY-MM-DD', nb_offres}].
    'periode' est une date ISO (jour/mois/annee tronque cote API)."""
    if not points:
        _empty(title)
        return
    df = pd.DataFrame(points)
    df["periode"] = pd.to_datetime(df["periode"])
    df = df.sort_values("periode")
    fig = px.area(df, x="periode", y="nb_offres", title=title)
    fig.update_traces(line_color=color, fillcolor="rgba(37,99,235,0.12)")
    fig.update_xaxes(title=None)
    fig.update_yaxes(title="Offres")
    st.plotly_chart(apply_plotly_theme(fig, height=380), use_container_width=True)
