"""Espace Recommandation — offres correspondant à un CV.

Le moteur de recommandation est développé séparément par un collègue et n'est
pas encore intégré à l'API. L'interface est prête : elle appelle
`POST /api/recommandation/cv` et gère proprement le cas où la route n'existe
pas encore (404 → message explicite).
"""
import streamlit as st

from components.sidebar import render_sidebar
from components.theme import inject_css, persona_hero
from services.api_client import ApiError, post_recommandation_cv

st.set_page_config(page_title="Recommandation", page_icon="🎯", layout="wide")
inject_css()
persona_hero(
    "🎯 Recommandation d'offres",
    "Déposez votre CV pour obtenir les offres les plus proches de votre profil.",
)

filters = render_sidebar()

col1, col2 = st.columns(2)
with col1:
    uploaded_file = st.file_uploader("Déposer un CV (PDF)", type=["pdf"])
with col2:
    cv_text = st.text_area(
        "…ou collez le texte de votre CV",
        height=200,
        placeholder="Copiez-collez le contenu de votre CV ici.",
    )

if st.button("Rechercher des offres correspondantes", type="primary"):
    if uploaded_file is None and not cv_text.strip():
        st.warning("Déposez un fichier PDF ou collez le texte de votre CV avant de lancer la recherche.")
    else:
        with st.spinner("Recherche des offres les plus pertinentes…"):
            try:
                payload_text = cv_text.strip() or "[CV PDF non encore extrait — extraction à implémenter]"
                resultats = post_recommandation_cv(payload_text, filters=filters)
            except ApiError as exc:
                if exc.status_code == 404:
                    st.info(
                        "Le moteur de recommandation n'est pas encore branché sur "
                        "l'API — cette page fonctionnera automatiquement dès que la "
                        "route `/api/recommandation/cv` sera disponible.",
                        icon="🔧",
                    )
                else:
                    st.error(f"Erreur lors de la recherche : {exc}")
                resultats = None

        if resultats:
            offres = resultats.get("resultats", resultats if isinstance(resultats, list) else [])
            if not offres:
                st.info("Aucune offre correspondante trouvée avec les filtres actuels.")
            for offre in offres:
                with st.container(border=True):
                    col_a, col_b = st.columns([3, 1])
                    with col_a:
                        st.markdown(f"**{offre.get('nom_metier', 'Poste')}** — {offre.get('nom_entreprise', '—')}")
                        st.caption(f"{offre.get('ville', '—')} · {offre.get('type_contrat', '—')}")
                        if offre.get("competences_communes"):
                            st.markdown(f"✅ Compétences communes : {', '.join(offre['competences_communes'])}")
                        if offre.get("competences_manquantes"):
                            st.markdown(f"⚠️ Compétences manquantes : {', '.join(offre['competences_manquantes'])}")
                        if offre.get("url"):
                            st.markdown(f"[Voir l'offre]({offre['url']})")
                    with col_b:
                        score = offre.get("score_matching")
                        if score is not None:
                            st.metric("Score", f"{score:.0%}" if score <= 1 else f"{score:.0f}")
