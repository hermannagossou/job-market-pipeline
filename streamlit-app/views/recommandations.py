"""
Job Market — Vue "Mes recommandations"
==========================================
Déclarée comme page via st.Page() dans app.py — ce fichier n'est pas exécuté
directement (streamlit run app.py reste le seul point d'entrée). Accessible
depuis la vue "Mon profil" après soumission réussie du formulaire (bouton
"Voir mes offres recommandées"), ou directement si le client revient plus
tard dans la même session.

Lit l'id_client depuis st.session_state (rempli par la vue "Mon profil" à la
soumission) ; profil et recommandations viennent de l'API (api_client.py).
"""

import streamlit as st

import api_client
from api_client import ApiError

st.set_page_config(page_title="Mes recommandations — Job Market", layout="centered")

st.title("Tes offres recommandées")

id_client = st.session_state.get("last_client_id")

if not id_client:
    st.info("Aucun profil trouvé dans cette session — remplis d'abord le formulaire.")
    st.page_link("views/mon_profil.py", label="Remplir mon profil", icon=":material/assignment:")
    st.stop()

# =============================================================================
# TON PROFIL — pour vérifier la pertinence des recommandations, et corriger si besoin
# =============================================================================
try:
    profil = api_client.get_client_profile(id_client)
except ApiError as e:
    profil = None
    st.warning(f"Impossible de récupérer ton profil pour l'instant ({e}).")

if profil:
    with st.expander("Ton profil", icon=":material/person:"):
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**{profil['prenom']} {profil['nom']}**")
            st.caption(profil["email"])
            st.markdown(f":material/school: {profil['formation_label']}")
            st.markdown(f":material/work_history: {profil['experience_label']}")
            st.markdown(f":material/description: {profil['contrat_label']}")
        with col2:
            st.markdown(f":material/euro: {profil['salaire_min']:.0f}€ - {profil['salaire_max']:.0f}€")
            st.caption(f"Profil soumis le {profil['date_soumission']}")
            st.caption("CV enregistré" if profil.get("cv_storage_path") else "Pas de CV enregistré")

        st.divider()
        st.markdown("**Compétences**")
        st.write(", ".join(c["label"] for c in profil["competences"]) or "Aucune")
        st.markdown("**Métier(s) recherché(s)**")
        st.write(", ".join(m["label"] for m in profil["metiers"]) or "Aucun")
        st.markdown("**Ville(s) recherchée(s)**")
        st.write(", ".join(l["label"] for l in profil["localisations"]) or "Aucune")

        st.divider()
        if st.button("Modifier mon profil", icon=":material/edit:", width="stretch"):
            st.session_state["profile_prefill"] = profil
            st.switch_page("views/mon_profil.py")

st.divider()

# =============================================================================
# RECOMMANDATIONS
# =============================================================================

try:
    with st.spinner("Calcul de tes recommandations..."):
        recommendations = api_client.get_recommendations(id_client)
except ApiError as e:
    st.error(f"Impossible de calculer tes recommandations pour l'instant ({e}).", icon=":material/error:")
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
                    f":material/location_on: {offre['ville'] or 'Ville non renseignée'} · "
                    f":material/euro: {offre['offre_salaire_min']:.0f}€ - {offre['offre_salaire_max']:.0f}€"
                )
                if offre.get("lien_offre"):
                    st.link_button("Voir l'offre", offre["lien_offre"], icon=":material/open_in_new:")
            with col_b:
                st.metric("Score", f"{offre['score_final']*100:.0f}%")

st.divider()
st.page_link("views/mon_profil.py", label="Remplir un nouveau profil", icon=":material/assignment:")
