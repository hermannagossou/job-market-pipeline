"""
Job Market — Page "Mon profil"
=================================
Page d'accueil de l'app (point d'entrée streamlit run app.py).
Formulaire + upload CV, comme avant — la seule différence : les
recommandations ne s'affichent plus ici, elles vivent sur une page dédiée
(pages/1_Recommandations.py), accessible via un bouton après soumission.

Toute la config, les connexions, et la logique CV/résolution/scoring sont
dans shared.py, importé ci-dessous — jamais dupliqué entre les pages.
"""

import streamlit as st

from shared import (
    extract_cv_info,
    extract_text_from_pdf,
    get_bq_client,
    get_gcs_client,
    load_dimension,
    resolve_competences_via_embedding,
    resolve_experience,
    resolve_formation,
    resolve_id_client,
    resolve_metiers_via_embedding,
    upsert_client_profile,
    BUCKET_NAME,
    MAX_CV_SIZE_MB,
)
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Interface
# ---------------------------------------------------------------------------
st.set_page_config(page_title="Job Market — Mon profil", page_icon="🧭", layout="centered")
st.title("🧭 Créer mon profil candidat")
st.caption("Dépose ton CV pour un pré-remplissage automatique, puis complète et vérifie avant de valider.")

try:
    formations = load_dimension("dim_formations", "id_formation", "niveau")
    experiences = load_dimension("dim_experiences", "id_experience", "niveau")
    metiers = load_dimension("dim_metiers", "id_metier", "nom")
    contrats = load_dimension("dim_contrats", "id_contrat", "contrat")
    competences_ref = load_dimension("dim_competences", "id_competence", "competence")
    localisations_ref = load_dimension("dim_localisations", "id_localisation", "ville")
except Exception as e:
    st.error(f"Impossible de charger les référentiels depuis BigQuery : {e}")
    st.stop()

if "cv_analysis" not in st.session_state:
    st.session_state.cv_analysis = None

st.divider()

# =============================================================================
# ÉTAPE 1 — Upload + analyse du CV
# =============================================================================
st.subheader("1️⃣ Ton CV")
cv_file = st.file_uploader("Format PDF uniquement", type=["pdf"])

if cv_file is not None:
    size_mb = cv_file.size / (1024 * 1024)
    if size_mb > MAX_CV_SIZE_MB:
        st.error(f"Le fichier fait {size_mb:.1f} Mo, la limite est {MAX_CV_SIZE_MB} Mo.")
        cv_file = None

if cv_file is not None and st.button("🔍 Analyser mon CV", type="primary"):
    cv_bytes = cv_file.getvalue()

    with st.spinner("Extraction du texte du PDF..."):
        texte_cv = extract_text_from_pdf(cv_bytes)

    if not texte_cv.strip():
        st.error(
            "Aucun texte n'a pu être extrait — le PDF est peut-être scanné (image). "
            "Tu peux quand même continuer et remplir les champs manuellement plus bas."
        )
    else:
        with st.spinner("Analyse du CV par IA..."):
            extraction = extract_cv_info(texte_cv)

        if extraction is None:
            st.error(
                "L'analyse automatique a échoué (réponse IA mal formée). "
                "Remplis les champs manuellement plus bas."
            )
        else:
            with st.spinner("Résolution des compétences et des métiers vers le référentiel..."):
                resolutions, resolution_error = resolve_competences_via_embedding(
                    extraction.get("competences", [])
                )
                resolutions_metiers, metier_resolution_error = resolve_metiers_via_embedding(
                    extraction.get("metiers", [])
                )

            st.session_state.cv_analysis = {
                "resolutions_competences": resolutions,
                "resolution_error": resolution_error,
                "resolutions_metiers": resolutions_metiers,
                "metier_resolution_error": metier_resolution_error,
                "nom_suggere": extraction.get("nom"),
                "prenom_suggere": extraction.get("prenom"),
                "email_suggere": extraction.get("email"),
                "id_formation_suggere": resolve_formation(
                    extraction.get("niveau_formation", ""), formations
                ),
                "id_experience_suggere": resolve_experience(
                    extraction.get("annees_experience"), experiences
                ),
                "cv_bytes": cv_bytes,
            }
            st.rerun()

analysis = st.session_state.cv_analysis
suggested_competence_ids: list[int] = []
suggested_metier_ids: list[int] = []

if analysis:
    if analysis.get("resolution_error"):
        st.warning(
            f"Résolution automatique des compétences indisponible ({analysis['resolution_error']}) "
            "— sélectionne-les manuellement ci-dessous."
        )
    if analysis.get("metier_resolution_error"):
        st.warning(
            f"Résolution automatique des métiers indisponible ({analysis['metier_resolution_error']}) "
            "— sélectionne-les manuellement ci-dessous."
        )
    if not analysis.get("resolution_error") and not analysis.get("metier_resolution_error"):
        st.success("CV analysé — les champs ci-dessous ont été pré-remplis. Vérifie et corrige-les directement si besoin.")

    suggested_competence_ids = list(
        dict.fromkeys(r["id_competence"] for r in analysis["resolutions_competences"])
    )
    suggested_metier_ids = list(
        dict.fromkeys(r["id_metier"] for r in analysis["resolutions_metiers"])
    )

st.divider()

# =============================================================================
# ÉTAPE 2 — Formulaire (pré-rempli si CV analysé, sinon vide)
# =============================================================================
st.subheader("2️⃣ Ton profil")

with st.form("profil_client_form"):
    col1, col2 = st.columns(2)
    with col1:
        nom = st.text_input("Nom *", value=(analysis.get("nom_suggere") or "") if analysis else "")
    with col2:
        prenom = st.text_input("Prénom *", value=(analysis.get("prenom_suggere") or "") if analysis else "")
    email = st.text_input("Email *", value=(analysis.get("email_suggere") or "") if analysis else "")

    st.markdown("**🎓 Formation & expérience** — suggérées par le CV si disponible, modifiables")
    formation_ids = list(formations.keys())
    default_formation_idx = (
        formation_ids.index(analysis["id_formation_suggere"])
        if analysis and analysis["id_formation_suggere"] in formation_ids
        else 0
    )
    id_formation = st.selectbox(
        "Niveau de formation *",
        options=formation_ids,
        format_func=lambda x: formations[x],
        index=default_formation_idx,
    )

    experience_ids = list(experiences.keys())
    default_experience_idx = (
        experience_ids.index(analysis["id_experience_suggere"])
        if analysis and analysis["id_experience_suggere"] in experience_ids
        else 0
    )
    id_experience = st.selectbox(
        "Niveau d'expérience *",
        options=experience_ids,
        format_func=lambda x: experiences[x],
        index=default_experience_idx,
    )

    st.markdown("**🛠️ Compétences** — pré-sélectionnées depuis le CV si disponible")
    ids_competences = st.multiselect(
        "Compétences *",
        options=list(competences_ref.keys()),
        format_func=lambda x: competences_ref[x],
        default=[cid for cid in suggested_competence_ids if cid in competences_ref],
    )

    st.markdown("**🎯 Recherche**")
    ids_metiers = st.multiselect(
        "Métier(s) recherché(s) *",
        options=list(metiers.keys()),
        format_func=lambda x: metiers[x],
        default=[mid for mid in suggested_metier_ids if mid in metiers],
        help="Suggérés depuis le titre du CV si trouvé (plusieurs si le CV en cite plusieurs, ex. \"Data Engineer / Analytics Engineer\") — vérifie que ce sont bien des postes visés, pas juste le poste actuel.",
    )
    id_contrat = st.selectbox(
        "Type de contrat recherché *",
        options=list(contrats.keys()),
        format_func=lambda x: contrats[x],
        help="Rarement présent sur un CV — critère strict : seules les offres de ce type te seront proposées.",
    )
    ids_localisations = st.multiselect(
        "Villes où tu cherches un poste *",
        options=list(localisations_ref.keys()),
        format_func=lambda x: localisations_ref[x],
    )

    col_sal1, col_sal2 = st.columns(2)
    with col_sal1:
        salaire_min = st.number_input("Salaire min souhaité (€ brut/an) *", min_value=0, step=1000, value=30000)
    with col_sal2:
        salaire_max = st.number_input("Salaire max souhaité (€ brut/an) *", min_value=0, step=1000, value=45000)

    submitted = st.form_submit_button("✅ Valider mon profil", type="primary", use_container_width=True)

# ---------------------------------------------------------------------------
# Traitement de la soumission
# ---------------------------------------------------------------------------
if submitted:
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

    id_client, is_new_client = resolve_id_client(email.strip())
    date_soumission = datetime.now(timezone.utc).date().isoformat()

    # --- Upload du CV vers GCS (si un CV a été analysé) ---
    # Réutilise le même id_client → si le client remplace son profil, son
    # ancien CV est écrasé par le nouveau, pas dupliqué.
    cv_storage_path = None
    if analysis and analysis.get("cv_bytes"):
        try:
            bucket = get_gcs_client().bucket(BUCKET_NAME)
            blob = bucket.blob(f"{id_client}.pdf")
            blob.upload_from_string(analysis["cv_bytes"], content_type="application/pdf")
            cv_storage_path = f"gs://{BUCKET_NAME}/{id_client}.pdf"
        except Exception as e:
            st.error(f"Échec de l'upload du CV vers GCS : {e}")
            st.stop()

    # --- Upsert complet (dim_clients + 3 bridges), en une seule requête ---
    try:
        upsert_client_profile(
            id_client=id_client,
            nom=nom.strip(),
            prenom=prenom.strip(),
            email=email.strip(),
            id_formation=id_formation,
            id_experience=id_experience,
            id_contrat=id_contrat,
            salaire_min=salaire_min,
            salaire_max=salaire_max,
            cv_storage_path=cv_storage_path,
            date_soumission=date_soumission,
            ids_competences=ids_competences,
            ids_metiers=ids_metiers,
            ids_localisations=ids_localisations,
        )
    except Exception as e:
        st.error(f"Échec de l'enregistrement du profil : {e}")
        st.stop()

    st.session_state["last_client_id"] = id_client
    if is_new_client:
        st.success("✅ Ton profil a bien été enregistré !")
    else:
        st.success("✅ Ton profil existant a été mis à jour !")
    st.balloons()
    if cv_storage_path:
        st.caption(f"CV stocké : `{cv_storage_path}`")
    st.session_state.cv_analysis = None

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
    if st.button("🎯 Voir mes offres recommandées", type="primary", use_container_width=True):
        st.switch_page("pages/1_Recommandations.py")