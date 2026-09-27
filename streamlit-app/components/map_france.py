"""Cartes choroplèthes France (régions et départements) via Plotly.

La correspondance entre les noms de régions/départements de nos données (issus
du référentiel INSEE dans les seeds dbt) et ceux du GeoJSON (même référentiel
INSEE) est faite sur une clé NORMALISÉE : minuscules, sans accents, apostrophes
unifiées, tirets/espaces neutralisés. Cette prudence évite les écarts déjà
rencontrés dans le projet (apostrophe typographique, casse, accents) sans
dépendre d'une correspondance strictement exacte des chaînes.
"""
from __future__ import annotations

import json
import unicodedata
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from components.theme import apply_plotly_theme

_ASSETS_DIR = Path(__file__).resolve().parent.parent / "assets"
_REGIONS_FILE = _ASSETS_DIR / "regions.geojson"
_DEPARTEMENTS_FILE = _ASSETS_DIR / "departements.geojson"


def _normalize(name: str) -> str:
    """Clé de jointure robuste : minuscule, sans accents, apostrophes/tirets/
    espaces neutralisés."""
    if name is None:
        return ""
    text = unicodedata.normalize("NFD", str(name))
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")  # retire accents
    text = text.lower().strip()
    for ch in ("'", "’", "-", " "):
        text = text.replace(ch, "")
    return text


@st.cache_data(show_spinner=False)
def _load_geojson(path_str: str) -> dict | None:
    """Charge un GeoJSON depuis le disque. Mis en cache pour ne le lire qu'une
    fois par session. Retourne None si le fichier est absent."""
    path = Path(path_str)
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _build_key_index(geojson: dict) -> dict[str, str]:
    """Indexe chaque feature du GeoJSON par sa clé normalisée -> nom exact du
    GeoJSON (celui que Plotly attend pour featureidkey='properties.nom')."""
    index = {}
    for feature in geojson.get("features", []):
        nom = feature.get("properties", {}).get("nom")
        if nom:
            index[_normalize(nom)] = nom
    return index


def _missing_geojson_message(kind: str) -> None:
    st.warning(
        f"Fond de carte des {kind} introuvable. Lancez une fois :\n\n"
        "`python streamlit/scripts/download_geojson.py`\n\n"
        "En attendant, les données restent consultables via les graphiques en barres.",
        icon="🗺️",
    )


def choropleth(
    items: list[dict],
    kind: str = "regions",
    title: str = "Concentration des offres",
    label_key: str = "label",
    value_key: str = "nb_offres",
    value_label: str = "Offres",
    color_scale: list[str] | None = None,
    hover_format: str | None = None,
) -> bool:
    """Affiche une carte choroplèthe.

    `kind` : 'regions' ou 'departements'.
    `value_key` : la colonne qui pilote la couleur (nb_offres par défaut, mais on
       peut passer 'salaire_moyen', 'salaire_median'…).
    `value_label` / `hover_format` : libellé et format de la valeur à l'affichage.
    Retourne True si la carte a été rendue, False sinon (GeoJSON absent, pas de
    données, aucune correspondance) — l'appelant peut alors afficher un repli.
    """
    if not items:
        st.info(f"Aucune donnée pour « {title} » avec les filtres actuels.")
        return False

    path = _REGIONS_FILE if kind == "regions" else _DEPARTEMENTS_FILE
    geojson = _load_geojson(str(path))
    if geojson is None:
        _missing_geojson_message("régions" if kind == "regions" else "départements")
        return False

    key_index = _build_key_index(geojson)
    df = pd.DataFrame(items)

    if value_key not in df.columns:
        st.error(f"Colonne « {value_key} » absente pour « {title} ».")
        return False

    # Résout le nom exact du GeoJSON via la clé normalisée ; écarte les labels
    # sans correspondance (ex: localisations étrangères comme 'Barcelona') et
    # les valeurs manquantes (ex: pas de salaire connu pour ce département).
    df["geo_nom"] = df[label_key].map(lambda x: key_index.get(_normalize(x)))
    non_matches = df[df["geo_nom"].isna()][label_key].tolist()
    df = df[df["geo_nom"].notna() & df[value_key].notna()]

    if df.empty:
        st.info("Aucune zone géographique exploitable pour cette carte avec les filtres actuels.")
        return False

    scale = color_scale or ["#DBEAFE", "#60A5FA", "#2563EB", "#1E3A8A"]
    fig = px.choropleth(
        df,
        geojson=geojson,
        locations="geo_nom",
        featureidkey="properties.nom",
        color=value_key,
        color_continuous_scale=scale,
        labels={value_key: value_label},
        title=title,
    )
    if hover_format:
        fig.update_traces(hovertemplate="%{location}<br>" + value_label + " : %{z:" + hover_format + "}<extra></extra>")
    fig.update_geos(fitbounds="locations", visible=False)
    fig = apply_plotly_theme(fig, height=520)
    fig.update_layout(margin=dict(l=0, r=0, t=48, b=0))
    st.plotly_chart(fig, use_container_width=True)

    if non_matches:
        st.caption(
            "Non localisées sur la carte (hors référentiel métropolitain) : "
            + ", ".join(str(x) for x in non_matches[:8])
            + ("…" if len(non_matches) > 8 else "")
        )
    return True
