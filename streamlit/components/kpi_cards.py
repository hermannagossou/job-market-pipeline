"""Composants d'affichage réutilisés sur plusieurs pages : cartes KPI et
graphiques en barres à partir d'une liste de `{label, nb_offres}`."""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st


def render_kpi_row(kpis: dict) -> None:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Offres", f"{kpis['nb_offres']:,}".replace(",", " "))
    col2.metric("Entreprises", f"{kpis['nb_entreprises']:,}".replace(",", " "))
    col3.metric("Métiers", f"{kpis['nb_metiers']:,}".replace(",", " "))
    col4.metric("Régions", f"{kpis['nb_regions']:,}".replace(",", " "))

    if kpis.get("salaire_min_moyen") is not None or kpis.get("salaire_max_moyen") is not None:
        col5, col6, col7 = st.columns(3)
        salaire_min = kpis.get("salaire_min_moyen")
        salaire_max = kpis.get("salaire_max_moyen")
        col5.metric("Salaire min. moyen", f"{salaire_min:,.0f} €".replace(",", " ") if salaire_min else "—")
        col6.metric("Salaire max. moyen", f"{salaire_max:,.0f} €".replace(",", " ") if salaire_max else "—")
        declare = kpis.get("nb_offres_salaire_declare", 0)
        estime = kpis.get("nb_offres_salaire_estime", 0)
        col7.metric("Salaires déclarés / estimés", f"{declare} / {estime}")


def render_bar_chart(items: list[dict], title: str, horizontal: bool = True, top_n: int | None = None) -> None:
    """Affiche une liste `[{label, nb_offres}, ...]` en graphique en barres.
    Affiche un message dédié plutôt qu'un graphique vide si aucune donnée ne
    correspond aux filtres actifs."""
    if not items:
        st.info(f"Aucune donnée disponible pour « {title} » avec les filtres actuels.")
        return

    df = pd.DataFrame(items)
    if top_n:
        df = df.nlargest(top_n, "nb_offres")

    if horizontal:
        df = df.sort_values("nb_offres", ascending=True)
        fig = px.bar(df, x="nb_offres", y="label", orientation="h", title=title)
    else:
        df = df.sort_values("nb_offres", ascending=False)
        fig = px.bar(df, x="label", y="nb_offres", title=title)

    fig.update_layout(margin=dict(l=10, r=10, t=40, b=10), height=420)
    st.plotly_chart(fig, use_container_width=True)


def render_evolution_chart(points: list[dict], title: str) -> None:
    """Affiche une évolution mensuelle `[{annee, mois, nb_offres}, ...]`."""
    if not points:
        st.info(f"Aucune donnée disponible pour « {title} » avec les filtres actuels.")
        return

    df = pd.DataFrame(points)
    df["periode"] = pd.to_datetime(df["annee"].astype(str) + "-" + df["mois"].astype(str) + "-01")
    df = df.sort_values("periode")
    fig = px.line(df, x="periode", y="nb_offres", title=title, markers=True)
    fig.update_layout(margin=dict(l=10, r=10, t=40, b=10), height=380)
    st.plotly_chart(fig, use_container_width=True)
