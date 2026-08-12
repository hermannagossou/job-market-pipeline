"""
Job Market — Page "Mes recommandations"
==========================================
Page accessible depuis app.py après soumission réussie du formulaire (bouton
"Voir mes offres recommandées"), ou directement si le client revient plus
tard dans la même session.

Lit l'id_client depuis st.session_state (rempli par app.py à la soumission) —
rien n'est recalculé pour un autre client que celui qui vient de s'inscrire
dans cette session.
"""

import streamlit as st

from shared import get_client_profile, get_recommendations

st.set_page_config(page_title="Job Market — Mes recommandations", page_icon="🎯", layout="centered")
st.title("🎯 Tes offres recommandées")

id_client = st.session_state.get("last_client_id")

if not id_client:
    st.info("Aucun profil trouvé dans cette session — remplis d'abord le formulaire.")
    st.page_link("app.py", label="Remplir mon profil", icon="📋")
    st.stop()

# =============================================================================
# TON PROFIL — pour vérifier la pertinence des recommandations, et corriger si besoin
# =============================================================================
try:
    profil = get_client_profile(id_client)
except Exception as e:
    profil = None
    st.warning(f"Impossible de récupérer ton profil pour l'instant ({e}).")

if profil:
    with st.expander("👤 Ton profil", expanded=False):
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**{profil['prenom']} {profil['nom']}**")
            st.caption(profil["email"])
            st.markdown(f"🎓 {profil['formation_label']}")
            st.markdown(f"💼 {profil['experience_label']}")
            st.markdown(f"📄 {profil['contrat_label']}")
        with col2:
            st.markdown(f"💰 {profil['salaire_min']:.0f}€ - {profil['salaire_max']:.0f}€")
            st.caption(f"Profil soumis le {profil['date_soumission']}")
            st.caption("📎 CV enregistré" if profil.get("cv_storage_path") else "Pas de CV enregistré")

        st.divider()
        st.markdown("**🛠️ Compétences**")
        st.write(", ".join(c["label"] for c in profil["competences"]) or "Aucune")
        st.markdown("**🎯 Métier(s) recherché(s)**")
        st.write(", ".join(m["label"] for m in profil["metiers"]) or "Aucun")
        st.markdown("**📍 Ville(s) recherchée(s)**")
        st.write(", ".join(l["label"] for l in profil["localisations"]) or "Aucune")

        st.divider()
        if st.button("✏️ Modifier mon profil", use_container_width=True):
            st.session_state["profile_prefill"] = profil
            st.switch_page("app.py")

st.divider()

# =============================================================================
# RECOMMANDATIONS
# =============================================================================

try:
    with st.spinner("Calcul de tes recommandations..."):
        recommendations = get_recommendations(id_client)
except Exception as e:
    st.error(f"Impossible de calculer tes recommandations pour l'instant ({e}).")
    st.stop()

if not recommendations:
    st.info("Aucune offre ne correspond à ton profil pour le moment — reviens plus tard !")
else:
    st.caption(f"{len(recommendations)} offre(s) trouvée(s), triées par pertinence.")
    for offre in recommendations:
        with st.container(border=True):
            col_a, col_b = st.columns([3, 1])
            with col_a:
                st.markdown(
                    f"**{offre['metier'] or 'Poste non renseigné'}** — "
                    f"{offre['entreprise'] or 'Entreprise non renseignée'}"
                )
                st.caption(
                    f"📍 {offre['ville'] or 'Ville non renseignée'} · "
                    f"💰 {offre['offre_salaire_min']:.0f}€ - {offre['offre_salaire_max']:.0f}€"
                )
            with col_b:
                st.metric("Score", f"{offre['score_final']*100:.0f}%")

st.divider()
st.page_link("app.py", label="Remplir un nouveau profil", icon="📋")