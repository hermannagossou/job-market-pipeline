"""
Job Market — Vue "Mon profil"
=================================
Formulaire + upload CV. Déclarée comme page via st.Page() dans app.py — ce
fichier n'est pas exécuté directement (streamlit run app.py reste le seul
point d'entrée).

Toutes les données passent par l'API (api_client.py) : analyse du CV,
référentiels, enregistrement du profil et du CV.
"""

import streamlit as st

import api_client
from api_client import ApiError

MAX_CV_SIZE_MB = 5

st.title("Créer mon profil candidat")
st.caption("Dépose ton CV pour un pré-remplissage automatique, puis complète et vérifie avant de valider.")

try:
    formations = api_client.get_referentiel("formations")
    experiences = api_client.get_referentiel("experiences")
    metiers = api_client.get_referentiel("metiers")
    contrats = api_client.get_referentiel("contrats")
    competences_ref = api_client.get_referentiel("competences")
    localisations_ref = api_client.get_referentiel("localisations")
except ApiError as e:
    st.error(f"Impossible de charger les référentiels : {e}", icon=":material/error:")
    st.stop()

st.session_state.setdefault("cv_analysis", None)
st.session_state.setdefault("cv_error", None)

# =============================================================================
# DÉJÀ INSCRIT ? — retrouver ses recommandations sans ressaisir son profil
# =============================================================================
with st.container(border=True):
    st.markdown("**:material/key: Déjà inscrit ?**")
    st.caption(
        "Retrouve tes recommandations sans repasser par le formulaire. "
        "Simple recherche par email, pas un vrai compte sécurisé — "
        "n'importe qui connaissant ton email pourrait accéder à ce que tu as renseigné."
    )
    with st.container(horizontal=True, vertical_alignment="bottom"):
        email_lookup = st.text_input(
            "Ton email", key="email_lookup", label_visibility="collapsed", placeholder="ton.email@exemple.com"
        )
        lookup_clicked = st.button("Me connecter", icon=":material/login:")

    if lookup_clicked:
        if not email_lookup.strip():
            st.error("Entre ton email d'abord.")
        else:
            try:
                found_id_client = api_client.find_client_by_email(email_lookup.strip())
            except ApiError as e:
                st.error(f"Recherche indisponible pour l'instant ({e}).")
            else:
                if found_id_client:
                    st.session_state["last_client_id"] = found_id_client
                    st.switch_page("views/recommandations.py")
                else:
                    st.warning("Aucun profil trouvé avec cet email — remplis le formulaire ci-dessous pour t'inscrire.")

st.divider()

# =============================================================================
# NOUVEAU PROFIL — formulaire + CV
# =============================================================================
st.subheader("1. Ton CV")
cv_file = st.file_uploader("Format PDF uniquement", type=["pdf"])

if cv_file is not None and cv_file.size > MAX_CV_SIZE_MB * 1024 * 1024:
    st.error(f"Le fichier fait {cv_file.size / (1024 * 1024):.1f} Mo, la limite est {MAX_CV_SIZE_MB} Mo.")
    cv_file = None

if cv_file is not None and st.button("Analyser mon CV", type="primary", icon=":material/document_search:"):
    cv_bytes = cv_file.getvalue()
    # Stockés en session_state (et pas affichés directement ici) : ce bloc ne
    # s'exécute que sur le rerun du clic, un message affiché ici disparaîtrait
    # à la première interaction suivante.
    try:
        with st.spinner("Analyse du CV..."):
            analyse = api_client.analyser_cv(cv_bytes, cv_file.name)
    except ApiError as e:
        st.session_state.cv_analysis = None
        st.session_state.cv_error = f"{e} Tu peux quand même remplir les champs manuellement plus bas."
    else:
        st.session_state.cv_analysis = {"analyse": analyse, "cv_bytes": cv_bytes, "cv_filename": cv_file.name}
        st.session_state.cv_error = None
    st.rerun()

if st.session_state.cv_error:
    st.error(st.session_state.cv_error, icon=":material/error:")

cv_analysis = st.session_state.cv_analysis
analysis = cv_analysis["analyse"] if cv_analysis else None
profile_prefill = st.session_state.get("profile_prefill")
suggested_competence_ids: list = []
suggested_metier_ids: list = []
suggested_localisation_ids: list = []

if profile_prefill:
    st.info(
        "Tu modifies ton profil existant — les champs ci-dessous sont pré-remplis "
        "avec tes informations actuelles. Corrige ce qui a changé, puis valide.",
        icon=":material/edit:",
    )
    suggested_competence_ids = [c["id"] for c in profile_prefill["competences"]]
    suggested_metier_ids = [m["id"] for m in profile_prefill["metiers"]]
    suggested_localisation_ids = [l["id"] for l in profile_prefill["localisations"]]

elif analysis:
    if analysis.get("erreur_competences"):
        st.warning(f"Compétences : {analysis['erreur_competences']}")
    if analysis.get("erreur_metiers"):
        st.warning(f"Métiers : {analysis['erreur_metiers']}")
    if not analysis.get("erreur_competences") and not analysis.get("erreur_metiers"):
        st.success(
            "CV analysé — les champs ci-dessous ont été pré-remplis. Vérifie et corrige-les directement si besoin.",
            icon=":material/check_circle:",
        )

    suggested_competence_ids = list(dict.fromkeys(r["id"] for r in analysis["competences"]))
    suggested_metier_ids = list(dict.fromkeys(r["id"] for r in analysis["metiers"]))

st.divider()

# =============================================================================
# ÉTAPE 2 — Formulaire (pré-rempli si CV analysé, sinon vide)
# =============================================================================
st.subheader("2. Ton profil")

if profile_prefill:
    source_prefill = profile_prefill
elif analysis:
    source_prefill = analysis
else:
    source_prefill = {}


def _index(options: list, valeur) -> int:
    return options.index(valeur) if valeur in options else 0


with st.form("profil_client_form"):
    col1, col2 = st.columns(2)
    with col1:
        nom = st.text_input("Nom *", value=source_prefill.get("nom") or "")
    with col2:
        prenom = st.text_input("Prénom *", value=source_prefill.get("prenom") or "")
    email = st.text_input("Email *", value=source_prefill.get("email") or "")

    st.markdown("**Formation et expérience** — suggérées si disponibles, modifiables")
    formation_ids = list(formations.keys())
    id_formation = st.selectbox(
        "Niveau de formation *",
        options=formation_ids,
        format_func=lambda x: formations[x],
        index=_index(formation_ids, source_prefill.get("id_formation")),
    )
    experience_ids = list(experiences.keys())
    id_experience = st.selectbox(
        "Niveau d'expérience *",
        options=experience_ids,
        format_func=lambda x: experiences[x],
        index=_index(experience_ids, source_prefill.get("id_experience")),
    )

    st.markdown("**Compétences** — pré-sélectionnées si disponibles")
    ids_competences = st.multiselect(
        "Compétences *",
        options=list(competences_ref.keys()),
        format_func=lambda x: competences_ref[x],
        default=[cid for cid in suggested_competence_ids if cid in competences_ref],
    )

    st.markdown("**Recherche**")
    ids_metiers = st.multiselect(
        "Métier(s) recherché(s) *",
        options=list(metiers.keys()),
        format_func=lambda x: metiers[x],
        default=[mid for mid in suggested_metier_ids if mid in metiers],
        help="Suggérés depuis le titre du CV si trouvé (plusieurs si le CV en cite plusieurs, ex. \"Data Engineer / Analytics Engineer\") — vérifie que ce sont bien des postes visés, pas juste le poste actuel.",
    )

    contrat_ids = list(contrats.keys())
    id_contrat = st.selectbox(
        "Type de contrat recherché *",
        options=contrat_ids,
        format_func=lambda x: contrats[x],
        index=_index(contrat_ids, profile_prefill.get("id_contrat") if profile_prefill else None),
        help="Rarement présent sur un CV — critère strict : seules les offres de ce type te seront proposées.",
    )

    ids_localisations = st.multiselect(
        "Villes où tu cherches un poste *",
        options=list(localisations_ref.keys()),
        format_func=lambda x: localisations_ref[x],
        default=[lid for lid in suggested_localisation_ids if lid in localisations_ref],
    )

    default_salaire_min = int(profile_prefill["salaire_min"]) if profile_prefill else 30000
    default_salaire_max = int(profile_prefill["salaire_max"]) if profile_prefill else 45000
    col_sal1, col_sal2 = st.columns(2)
    with col_sal1:
        salaire_min = st.number_input("Salaire min souhaité (€ brut/an) *", min_value=0, step=1000, value=default_salaire_min)
    with col_sal2:
        salaire_max = st.number_input("Salaire max souhaité (€ brut/an) *", min_value=0, step=1000, value=default_salaire_max)

    submitted = st.form_submit_button("Valider mon profil", type="primary", icon=":material/check:", width="stretch")

# ---------------------------------------------------------------------------
# Traitement de la soumission
# ---------------------------------------------------------------------------
if submitted:
    # Contrôles aussi faits par l'API (422) — dupliqués ici pour des messages
    # immédiats et en français, sans aller-retour réseau.
    errors = []
    if not nom.strip():
        errors.append("Le nom est requis.")
    if not prenom.strip():
        errors.append("Le prénom est requis.")
    if not email.strip() or "@" not in email:
        errors.append("Un email valide est requis.")
    if not ids_competences:
        errors.append("Sélectionne au moins une compétence.")
    if not ids_metiers:
        errors.append("Sélectionne au moins un métier recherché.")
    if not ids_localisations:
        errors.append("Sélectionne au moins une ville.")
    if salaire_min > salaire_max:
        errors.append("Le salaire minimum ne peut pas dépasser le salaire maximum.")

    if errors:
        for err in errors:
            st.error(err)
        st.stop()

    try:
        with st.spinner("Enregistrement du profil..."):
            resultat = api_client.upsert_client(
                {
                    "nom": nom.strip(),
                    "prenom": prenom.strip(),
                    "email": email.strip(),
                    "id_formation": id_formation,
                    "id_experience": id_experience,
                    "id_contrat": id_contrat,
                    "salaire_min": salaire_min,
                    "salaire_max": salaire_max,
                    "ids_competences": ids_competences,
                    "ids_metiers": ids_metiers,
                    "ids_localisations": ids_localisations,
                }
            )
    except ApiError as e:
        st.error(f"Échec de l'enregistrement du profil : {e}", icon=":material/error:")
        st.stop()

    id_client = resultat["id_client"]
    st.session_state["last_client_id"] = id_client
    st.success(
        "Ton profil a bien été enregistré !" if resultat["est_nouveau"] else "Ton profil existant a été mis à jour !",
        icon=":material/check_circle:",
    )

    # CV déposé séparément, une fois le profil créé. Sans nouveau CV analysé,
    # l'API conserve le CV déjà enregistré pour ce client.
    if cv_analysis:
        try:
            cv_storage_path = api_client.upload_cv(id_client, cv_analysis["cv_bytes"], cv_analysis["cv_filename"])
            st.caption(f"CV stocké : `{cv_storage_path}`")
        except ApiError as e:
            st.warning(f"Profil enregistré, mais le CV n'a pas pu être déposé ({e}).")

    st.balloons()
    st.session_state.cv_analysis = None
    st.session_state.cv_error = None
    st.session_state["profile_prefill"] = None

# --- Bouton vers la page recommandations ---
# Volontairement HORS du bloc "if submitted:" : ce bloc ne s'exécute que sur
# le rerun exact du clic sur "Valider mon profil". Sur le rerun suivant
# (celui déclenché par le clic sur CE bouton), "submitted" redevient False —
# un bouton placé à l'intérieur de "if submitted:" ne serait donc plus jamais
# ré-instancié, et son clic ne pourrait jamais être traité. En s'appuyant sur
# st.session_state["last_client_id"] (qui persiste, lui) plutôt que sur
# "submitted", ce bloc reste actif sur tous les reruns suivants.
if st.session_state.get("last_client_id"):
    st.divider()
    if st.button("Voir mes offres recommandées", type="primary", icon=":material/target:", width="stretch"):
        st.switch_page("views/recommandations.py")
