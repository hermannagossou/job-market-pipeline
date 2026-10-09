"""Thème central du dashboard : palette de couleurs cohérente pour tous les
graphiques Plotly, CSS pour les cartes KPI, et helpers de formatage (nombres,
euros) réutilisés partout. Centraliser ça garantit une identité visuelle
homogène entre les pages plutôt que des styles ad hoc dupliqués."""
from __future__ import annotations

import streamlit as st

# Palette principale — dégradé de bleus + accents, lisible et sobre.
PRIMARY = "#2563EB"
PRIMARY_DARK = "#1E3A8A"
ACCENT = "#F59E0B"
SUCCESS = "#10B981"
NEUTRAL = "#64748B"

# Séquence utilisée pour les graphiques catégoriels (barres, camemberts...).
COLOR_SEQUENCE = ["#2563EB", "#3B82F6", "#60A5FA", "#93C5FD", "#1E3A8A", "#F59E0B", "#10B981", "#64748B"]

# Couleurs dédiées aux deux plateformes, cohérentes sur tout le dashboard.
PLATFORM_COLORS = {"france_travail": "#2563EB", "wttj": "#F59E0B"}


def apply_plotly_theme(fig, height: int = 400):
    """Applique un style homogène à une figure Plotly : marges compactes, fond
    transparent (s'intègre au thème Streamlit), police lisible."""
    fig.update_layout(
        height=height,
        margin=dict(l=10, r=10, t=48, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family="sans-serif", size=13, color="#0F172A"),
        title_font=dict(size=16, color="#0F172A"),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig.update_xaxes(showgrid=True, gridcolor="#E2E8F0", zeroline=False)
    fig.update_yaxes(showgrid=True, gridcolor="#E2E8F0", zeroline=False)
    return fig


def inject_css() -> None:
    """Injecte le CSS des cartes KPI et quelques ajustements de mise en page.
    Appelé une fois par page (idempotent)."""
    st.markdown(
        """
        <style>
        .kpi-card {
            background: linear-gradient(135deg, #FFFFFF 0%, #F8FAFC 100%);
            border: 1px solid #E2E8F0;
            border-radius: 14px;
            padding: 18px 20px;
            box-shadow: 0 1px 3px rgba(15, 23, 42, 0.06);
            height: 100%;
        }
        .kpi-label {
            font-size: 0.80rem;
            font-weight: 600;
            color: #64748B;
            text-transform: uppercase;
            letter-spacing: 0.03em;
            margin-bottom: 6px;
        }
        .kpi-value {
            font-size: 1.9rem;
            font-weight: 700;
            color: #0F172A;
            line-height: 1.1;
        }
        .kpi-sub {
            font-size: 0.82rem;
            color: #64748B;
            margin-top: 6px;
        }
        .persona-hero {
            background: linear-gradient(135deg, #1E3A8A 0%, #2563EB 100%);
            border-radius: 16px;
            padding: 26px 30px;
            color: #FFFFFF;
            margin-bottom: 8px;
        }
        .persona-hero h2 { color: #FFFFFF; margin: 0 0 6px 0; }
        .persona-hero p { color: #DBEAFE; margin: 0; font-size: 0.95rem; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def fmt_int(value) -> str:
    """Formate un entier avec espaces comme séparateurs de milliers (fr)."""
    try:
        return f"{int(value):,}".replace(",", " ")
    except (TypeError, ValueError):
        return "—"


def fmt_euro(value) -> str:
    try:
        return f"{float(value):,.0f} €".replace(",", " ")
    except (TypeError, ValueError):
        return "—"


def kpi_card(label: str, value: str, sub: str | None = None) -> str:
    """Retourne le HTML d'une carte KPI stylée (à insérer dans st.markdown)."""
    sub_html = f'<div class="kpi-sub">{sub}</div>' if sub else ""
    return f"""
    <div class="kpi-card">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
        {sub_html}
    </div>
    """


def persona_hero(title: str, subtitle: str) -> None:
    st.markdown(
        f'<div class="persona-hero"><h2>{title}</h2><p>{subtitle}</p></div>',
        unsafe_allow_html=True,
    )
