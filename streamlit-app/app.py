"""
Job Market — Profil client (CV + formulaire complémentaire)
=============================================================
Flux en 2 temps :
  1. Upload CV (PDF) -> extraction texte (pdfplumber) -> extraction structurée
     (Gemini/Vertex AI) -> résolution compétences + métier (embedding +
     VECTOR_SEARCH) -> résolution formation/expérience (règles) -> nom/prénom/
     email pré-remplis en texte libre -> l'utilisateur VALIDE/corrige tout
     (jamais d'insertion silencieuse).
  2. Formulaire complémentaire pour ce qu'un CV ne contient presque jamais :
     type de contrat recherché, fourchette de salaire, villes visées.

Si l'extraction échoue ou si le client n'a pas de CV, tous les champs
restent modifiables manuellement — le formulaire fonctionne dans tous les
cas, avec ou sans CV.

Insertion BigQuery à la soumission finale :
  - dim_clients                    (1 ligne, cv_storage_path rempli ou NULL)
  - bridge_clients_competences     (N lignes)
  - bridge_clients_metiers         (N lignes — un client peut viser plusieurs métiers)
  - bridge_clients_localisations   (N lignes)

--------------------------------------------------------------------------
PRÉREQUIS — à faire une seule fois, en dehors de cette app (BigQuery / SQL) :
--------------------------------------------------------------------------
1. Modèle d'embedding créé :
     CREATE OR REPLACE MODEL `PROJECT.DATASET.embedding_model`
     REMOTE WITH CONNECTION `US.vertex-ai`
     OPTIONS (ENDPOINT = 'text-multilingual-embedding-002');

2. Embeddings de dim_competences calculés en batch et indexés :
     CREATE OR REPLACE TABLE `PROJECT.DATASET.dim_competences_embeddings` AS
     SELECT * FROM ML.GENERATE_EMBEDDING(
       MODEL `PROJECT.DATASET.embedding_model`,
       (SELECT id_competence, competence AS content FROM `PROJECT.DATASET.dim_competences`)
     ) WHERE LENGTH(ml_generate_embedding_status) = 0;

     CREATE OR REPLACE VECTOR INDEX competences_index
     ON `PROJECT.DATASET.dim_competences_embeddings`(ml_generate_embedding_result)
     OPTIONS(index_type='IVF', distance_type='COSINE', ivf_options='{"num_lists":10}');

3. Embeddings de dim_metiers calculés en batch (table plus petite, pas
   besoin d'index vectoriel pour ce volume) :
     CREATE OR REPLACE TABLE `PROJECT.DATASET.dim_metiers_embeddings` AS
     SELECT * FROM ML.GENERATE_EMBEDDING(
       MODEL `PROJECT.DATASET.embedding_model`,
       (SELECT id_metier, nom AS content FROM `PROJECT.DATASET.dim_metiers`)
     ) WHERE LENGTH(ml_generate_embedding_status) = 0;

Sans ces étapes, la résolution des compétences et du métier extraits du CV
échouera (avec un message d'erreur explicite affiché dans l'app — le
formulaire reste utilisable en mode manuel dans ce cas).

--------------------------------------------------------------------------
Setup Python / secrets :
--------------------------------------------------------------------------
pip install streamlit google-cloud-bigquery google-cloud-storage pdfplumber \
            google-cloud-aiplatform --break-system-packages

.streamlit/secrets.toml :

    [gcp_service_account]
    # Copie ICI TOUS les champs de ton fichier credentials.json téléchargé
    # depuis GCP (IAM > Comptes de service > clé JSON) — pas un sous-ensemble,
    # tous les champs sont requis par google-auth.
    type = "service_account"
    project_id = "..."
    private_key_id = "..."
    private_key = "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"
    client_email = "..."
    client_id = "..."
    auth_uri = "https://accounts.google.com/o/oauth2/auth"
    token_uri = "https://oauth2.googleapis.com/token"
    auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
    client_x509_cert_url = "..."
    universe_domain = "googleapis.com"

    [gcp]
    bigquery_dataset = "ton_dataset"        # <-- À ADAPTER
    gcs_bucket = "job-market-cv-uploads"    # <-- À ADAPTER, bucket déjà créé
    vertex_location = "global"              # les modèles en preview (dont gemini-3.1-flash-lite)
                                             # ne sont souvent accessibles que via "global",
                                             # pas un endpoint régional comme us-central1

Rôles IAM requis sur le compte de service : BigQuery Data Editor, BigQuery
Connections Admin (si tu recrées la connexion), Storage Object Creator,
Vertex AI User.

Lancer avec : streamlit run app.py
"""

import io
import json
import uuid
from datetime import datetime, timezone

import pdfplumber
import streamlit as st
import vertexai
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from vertexai.generative_models import GenerationConfig, GenerativeModel

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
PROJECT_ID = st.secrets["gcp_service_account"]["project_id"]
DATASET = st.secrets["gcp"]["bigquery_dataset"]
BUCKET_NAME = st.secrets["gcp"]["gcs_bucket"]
VERTEX_LOCATION = st.secrets["gcp"].get("vertex_location", "us-central1")

TABLE_CLIENTS = f"{PROJECT_ID}.{DATASET}.dim_clients"
TABLE_BRIDGE_COMPETENCES = f"{PROJECT_ID}.{DATASET}.bridge_clients_competences"
TABLE_BRIDGE_LOCALISATIONS = f"{PROJECT_ID}.{DATASET}.bridge_clients_localisations"
TABLE_BRIDGE_METIERS = f"{PROJECT_ID}.{DATASET}.bridge_clients_metiers"

MAX_CV_SIZE_MB = 5
# Modèle Gemini pour l'extraction JSON — vérifier le nom courant sur
# docs.cloud.google.com/vertex-ai (les noms de modèles évoluent régulièrement).
GEMINI_MODEL = "gemini-3.1-flash-lite"

# Règles de résolution formation/expérience — HEURISTIQUES SIMPLES, à ajuster
# toi-même après avoir observé des résultats réels sur tes CV de test.
FORMATION_KEYWORDS = {
    "Doctorat": ["doctorat", "phd", "thèse", "these"],
    "Bac+5": ["bac+5", "master 2", "m2", "ingénieur", "ingenieur", "mastère", "mba", "master"],
    "Bac+4": ["bac+4", "master 1", "m1"],
    "Bac+3": ["bac+3", "licence", "bachelor"],
    "Bac+2": ["bac+2", "bts", "dut"],
}
EXPERIENCE_SEUILS = [(1, "Junior"), (4, "Confirmé"), (8, "Senior")]  # au-delà -> "Expert"


# ---------------------------------------------------------------------------
# Connexions (mises en cache pour ne pas les recréer à chaque interaction)
# ---------------------------------------------------------------------------
@st.cache_resource
def get_credentials() -> service_account.Credentials:
    return service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )


@st.cache_resource
def get_bq_client() -> bigquery.Client:
    creds = get_credentials()
    return bigquery.Client(credentials=creds, project=creds.project_id)


@st.cache_resource
def get_gcs_client() -> storage.Client:
    creds = get_credentials()
    return storage.Client(credentials=creds, project=creds.project_id)


@st.cache_resource
def get_gemini_model() -> GenerativeModel:
    vertexai.init(project=PROJECT_ID, location=VERTEX_LOCATION, credentials=get_credentials())
    return GenerativeModel(GEMINI_MODEL)


# ---------------------------------------------------------------------------
# Référentiels (dimensions) — mis en cache 1h
# ---------------------------------------------------------------------------
@st.cache_data(ttl=3600)
def load_dimension(table: str, id_col: str, label_col: str) -> dict:
    """Retourne {id: label}, trié par libellé."""
    client = get_bq_client()
    query = f"""
        SELECT {id_col} AS id, {label_col} AS label
        FROM `{PROJECT_ID}.{DATASET}.{table}`
        ORDER BY {label_col}
    """
    rows = client.query(query).result()
    return {row.id: row.label for row in rows}


# ---------------------------------------------------------------------------
# Étape 1 — Extraction texte du PDF
# ---------------------------------------------------------------------------
def extract_text_from_pdf(file_bytes: bytes) -> str:
    text = ""
    with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
    return text


# ---------------------------------------------------------------------------
# Étape 2 — Extraction structurée via Gemini
# ---------------------------------------------------------------------------
def extract_cv_info(texte_cv: str) -> dict | None:
    prompt = f"""Analyse ce CV et extrait UNIQUEMENT les informations suivantes.
Réponds STRICTEMENT en JSON avec ce format exact, sans texte autour :
{{
  "nom": "nom de famille si identifiable, sinon null",
  "prenom": "prénom si identifiable, sinon null",
  "email": "adresse email si présente dans le texte, sinon null",
  "metiers": ["liste des intitulés de poste ou titres professionnels mis en avant en haut du CV (poste occupé ou visé) — plusieurs si le CV en mentionne plusieurs séparés par '/' ou ','"],
  "competences": ["liste des compétences techniques mentionnées, telles qu'écrites"],
  "niveau_formation": "niveau de formation le plus élevé mentionné, en texte libre",
  "annees_experience": nombre entier d'années d'expérience professionnelle estimées
}}

IMPORTANT : si une information n'est pas clairement présente dans le texte, renvoie
null (ou une liste vide pour metiers/competences) — n'invente JAMAIS une valeur plausible
(surtout pour l'email et les métiers).

Texte du CV :
{texte_cv[:8000]}
"""
    model = get_gemini_model()
    config = GenerationConfig(response_mime_type="application/json", temperature=0.1)
    try:
        response = model.generate_content(prompt, generation_config=config)
        return json.loads(response.text)
    except (json.JSONDecodeError, AttributeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Étape 3 — Résolutions vers les référentiels
# ---------------------------------------------------------------------------
def resolve_competences_via_embedding(competences_extraites: list[str]) -> tuple[list[dict], str | None]:
    """Résout chaque compétence texte vers l'id_competence le plus proche
    (VECTOR_SEARCH). Retourne (resolutions, message_erreur_ou_None)."""
    if not competences_extraites:
        return [], None
    query = f"""
        SELECT
            query.content AS competence_extraite,
            base.id_competence AS id_competence,
            base.content AS competence_resolue,
            distance
        FROM VECTOR_SEARCH(
            TABLE `{PROJECT_ID}.{DATASET}.dim_competences_embeddings`,
            'ml_generate_embedding_result',
            (
                SELECT ml_generate_embedding_result, content
                FROM ML.GENERATE_EMBEDDING(
                    MODEL `{PROJECT_ID}.{DATASET}.embedding_model`,
                    (SELECT competence AS content FROM UNNEST(@competences) AS competence)
                )
            ),
            top_k => 1
        )
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("competences", "STRING", competences_extraites)
        ]
    )
    try:
        rows = get_bq_client().query(query, job_config=job_config).result()
        return [dict(row.items()) for row in rows], None
    except Exception as e:
        return [], str(e)


def resolve_metiers_via_embedding(metiers_extraits: list[str]) -> tuple[list[dict], str | None]:
    """Résout chaque intitulé de poste extrait vers l'id_metier le plus proche
    (VECTOR_SEARCH, top_k=1 par intitulé). Retourne (resolutions, message_erreur_ou_None).
    Nécessite dim_metiers_embeddings déjà calculée (cf. prérequis en tête de fichier)."""
    if not metiers_extraits:
        return [], None
    query = f"""
        SELECT
            query.content AS metier_extrait,
            base.id_metier AS id_metier,
            base.content AS metier_resolu,
            distance
        FROM VECTOR_SEARCH(
            TABLE `{PROJECT_ID}.{DATASET}.dim_metiers_embeddings`,
            'ml_generate_embedding_result',
            (
                SELECT ml_generate_embedding_result, content
                FROM ML.GENERATE_EMBEDDING(
                    MODEL `{PROJECT_ID}.{DATASET}.embedding_model`,
                    (SELECT metier AS content FROM UNNEST(@metiers) AS metier)
                )
            ),
            top_k => 1
        )
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("metiers", "STRING", metiers_extraits)]
    )
    try:
        rows = get_bq_client().query(query, job_config=job_config).result()
        return [dict(row.items()) for row in rows], None
    except Exception as e:
        return [], str(e)


def resolve_formation(texte_niveau: str, formations_ref: dict) -> int | None:
    """Heuristique par mots-clés sur les 6 valeurs connues de dim_formations."""
    texte = (texte_niveau or "").lower()
    for label, keywords in FORMATION_KEYWORDS.items():
        if any(kw in texte for kw in keywords):
            for fid, flabel in formations_ref.items():
                if flabel == label:
                    return fid
    return None


def resolve_experience(annees, experiences_ref: dict) -> int | None:
    """Mapping par seuils sur les années d'expérience extraites."""
    if annees is None:
        return None
    try:
        annees = int(annees)
    except (TypeError, ValueError):
        return None
    label = "Expert"
    for seuil, seuil_label in EXPERIENCE_SEUILS:
        if annees <= seuil:
            label = seuil_label
            break
    for eid, elabel in experiences_ref.items():
        if elabel == label:
            return eid
    return None


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

    id_client = str(uuid.uuid4())
    date_soumission = datetime.now(timezone.utc).isoformat()
    bq_client = get_bq_client()

    # --- Upload du CV vers GCS (si un CV a été analysé) ---
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

    # --- Insertion dim_clients ---
    client_row = {
        "id_client": id_client,
        "nom": nom.strip(),
        "prenom": prenom.strip(),
        "email": email.strip(),
        "id_formation": id_formation,
        "id_experience": id_experience,
        "id_contrat": id_contrat,
        "salaire_min": salaire_min,
        "salaire_max": salaire_max,
        "cv_storage_path": cv_storage_path,
        "date_soumission": date_soumission,
    }
    err = bq_client.insert_rows_json(TABLE_CLIENTS, [client_row])
    if err:
        st.error(f"Échec de l'insertion dans dim_clients : {err}")
        st.stop()

    # --- Insertion bridge_clients_competences ---
    rows = [{"id_client": id_client, "id_competence": cid} for cid in ids_competences]
    err = bq_client.insert_rows_json(TABLE_BRIDGE_COMPETENCES, rows)
    if err:
        st.error(f"Échec de l'insertion des compétences : {err}")
        st.stop()

    # --- Insertion bridge_clients_metiers ---
    rows = [{"id_client": id_client, "id_metier": mid} for mid in ids_metiers]
    err = bq_client.insert_rows_json(TABLE_BRIDGE_METIERS, rows)
    if err:
        st.error(f"Échec de l'insertion des métiers : {err}")
        st.stop()

    # --- Insertion bridge_clients_localisations ---
    rows = [{"id_client": id_client, "id_localisation": lid} for lid in ids_localisations]
    err = bq_client.insert_rows_json(TABLE_BRIDGE_LOCALISATIONS, rows)
    if err:
        st.error(f"Échec de l'insertion des localisations : {err}")
        st.stop()

    st.success("✅ Ton profil a bien été enregistré !")
    st.balloons()
    if cv_storage_path:
        st.caption(f"CV stocké : `{cv_storage_path}`")
    st.session_state.cv_analysis = None