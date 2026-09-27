"""
Job Market — Code partagé entre les pages Streamlit
======================================================
Ce module regroupe tout ce qui est commun aux différentes pages de l'app
(config, connexions mises en cache, extraction/résolution du CV, calcul des
recommandations) — importé par views/mon_profil.py (page "Mon profil") et par
views/recommandations.py (page "Mes recommandations"), routées depuis app.py.

Ne pas exécuter ce fichier directement avec `streamlit run` — c'est un module
d'import, pas une page.

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
1. Modèle d'embedding créé dans le dataset cible :
     CREATE OR REPLACE MODEL `PROJECT.DATASET.embedding_model`
     REMOTE WITH CONNECTION `US.job-market-conn`
     OPTIONS (ENDPOINT = 'text-multilingual-embedding-002');

2. Tables alimentées par cette app créées dans le dataset cible (dim_clients
   + 3 bridges, cf. source app_streamlit dans _sources_streamlit.yml).

3. dbt build lancé au moins une fois sur ce dataset : il construit les
   tables d'embeddings (dim_competences_embeddings, dim_metiers_embeddings,
   offres_embeddings) et leurs index vectoriels.

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
EXPERIENCE_SEUILS = [(2, "Junior"), (5, "Confirmé"), (10, "Senior")]  # au-delà -> "Expert"
# Aligné sur la classification côté offres (int_france_travail_niveau_experience.sql,
# ajustée le 23 août) : 0-2 ans Junior, 3-5 Confirmé, 6-10 Senior, >10 Expert.
# Avant : (1, "Junior"), (4, "Confirmé"), (8, "Senior") — désynchronisé du côté offres,
# le filtre dur client_rang_experience >= offre_rang_experience comparait deux
# définitions différentes de chaque niveau.


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


def find_client_by_email(email: str) -> str | None:
    """Recherche pure : retourne l'id_client si un profil existe déjà pour cet
    email, sinon None. Contrairement à resolve_id_client (utilisée à la
    soumission), celle-ci ne génère jamais de nouvel id — elle sert
    uniquement à vérifier si un client peut se reconnecter à son profil
    existant, sans rien créer ni modifier."""
    query = f"SELECT id_client FROM `{TABLE_CLIENTS}` WHERE email = @email LIMIT 1"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("email", "STRING", email)]
    )
    rows = list(get_bq_client().query(query, job_config=job_config).result())
    return rows[0]["id_client"] if rows else None


def resolve_id_client(email: str) -> tuple[str, bool]:
    """Réutilise l'id_client existant si l'email est déjà dans dim_clients
    (upsert par email), sinon en génère un nouveau. Retourne (id_client, est_nouveau)."""
    query = f"SELECT id_client FROM `{TABLE_CLIENTS}` WHERE email = @email LIMIT 1"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("email", "STRING", email)]
    )
    rows = list(get_bq_client().query(query, job_config=job_config).result())
    if rows:
        return rows[0]["id_client"], False
    return str(uuid.uuid4()), True


def upsert_client_profile(
    id_client: str,
    nom: str,
    prenom: str,
    email: str,
    id_formation,
    id_experience,
    id_contrat,
    salaire_min: float,
    salaire_max: float,
    cv_storage_path: str | None,
    date_soumission: str,
    ids_competences: list,
    ids_metiers: list,
    ids_localisations: list,
) -> None:
    """Upsert complet du profil client, en une seule requête script (multi-
    instructions) : MERGE sur dim_clients (par id_client, déjà résolu par
    email via resolve_id_client), puis remplacement complet des 3 bridges
    (DELETE + INSERT).

    Tout en DML — jamais insert_rows_json (streaming) — pour éviter la
    restriction BigQuery qui interdit UPDATE/DELETE/MERGE sur des lignes
    tout juste écrites en streaming (jusqu'à ~30-90 min de délai). Sans ça,
    un client qui teste plusieurs fois de suite (comme pendant le dev)
    tomberait en erreur en tentant de mettre à jour son propre profil
    récemment soumis.
    """
    script = f"""
    MERGE `{TABLE_CLIENTS}` AS target
    USING (SELECT
        @id_client AS id_client, @nom AS nom, @prenom AS prenom, @email AS email,
        @id_formation AS id_formation, @id_experience AS id_experience, @id_contrat AS id_contrat,
        @salaire_min AS salaire_min, @salaire_max AS salaire_max,
        @cv_storage_path AS cv_storage_path, @date_soumission AS date_soumission
    ) AS source
    ON target.id_client = source.id_client
    WHEN MATCHED THEN UPDATE SET
        nom = source.nom, prenom = source.prenom, email = source.email,
        id_formation = source.id_formation, id_experience = source.id_experience,
        id_contrat = source.id_contrat, salaire_min = source.salaire_min, salaire_max = source.salaire_max,
        cv_storage_path = source.cv_storage_path, date_soumission = source.date_soumission
    WHEN NOT MATCHED THEN INSERT (
        id_client, nom, prenom, email, id_formation, id_experience, id_contrat,
        salaire_min, salaire_max, cv_storage_path, date_soumission
    )
    VALUES (
        source.id_client, source.nom, source.prenom, source.email, source.id_formation,
        source.id_experience, source.id_contrat, source.salaire_min, source.salaire_max,
        source.cv_storage_path, source.date_soumission
    );

    DELETE FROM `{TABLE_BRIDGE_COMPETENCES}` WHERE id_client = @id_client;
    INSERT INTO `{TABLE_BRIDGE_COMPETENCES}` (id_client, id_competence)
    SELECT @id_client, id_competence FROM UNNEST(@competences) AS id_competence;

    DELETE FROM `{TABLE_BRIDGE_METIERS}` WHERE id_client = @id_client;
    INSERT INTO `{TABLE_BRIDGE_METIERS}` (id_client, id_metier)
    SELECT @id_client, id_metier FROM UNNEST(@metiers) AS id_metier;

    DELETE FROM `{TABLE_BRIDGE_LOCALISATIONS}` WHERE id_client = @id_client;
    INSERT INTO `{TABLE_BRIDGE_LOCALISATIONS}` (id_client, id_localisation)
    SELECT @id_client, id_localisation FROM UNNEST(@localisations) AS id_localisation;
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("id_client", "STRING", id_client),
            bigquery.ScalarQueryParameter("nom", "STRING", nom),
            bigquery.ScalarQueryParameter("prenom", "STRING", prenom),
            bigquery.ScalarQueryParameter("email", "STRING", email),
            bigquery.ScalarQueryParameter("id_formation", "STRING", id_formation),
            bigquery.ScalarQueryParameter("id_experience", "STRING", id_experience),
            bigquery.ScalarQueryParameter("id_contrat", "STRING", id_contrat),
            bigquery.ScalarQueryParameter("salaire_min", "FLOAT64", salaire_min),
            bigquery.ScalarQueryParameter("salaire_max", "FLOAT64", salaire_max),
            bigquery.ScalarQueryParameter("cv_storage_path", "STRING", cv_storage_path),
            bigquery.ScalarQueryParameter("date_soumission", "DATE", date_soumission),
            bigquery.ArrayQueryParameter("competences", "STRING", ids_competences),
            bigquery.ArrayQueryParameter("metiers", "STRING", ids_metiers),
            bigquery.ArrayQueryParameter("localisations", "STRING", ids_localisations),
        ]
    )
    get_bq_client().query(script, job_config=job_config).result()


def get_client_profile(id_client: str) -> dict | None:
    """Récupère le profil complet d'un client : infos de base + libellés
    résolus (formation, expérience, contrat) + listes détaillées (id + label)
    des compétences, métiers, localisations. Sert à la fois à l'affichage sur
    la page recommandations et au pré-remplissage du formulaire en cas de
    modification. Retourne None si l'id_client n'existe pas."""
    client = get_bq_client()

    query_profil = f"""
        SELECT
            dc.nom, dc.prenom, dc.email,
            dc.id_formation, df.niveau AS formation_label,
            dc.id_experience, de.niveau AS experience_label,
            dc.id_contrat, dcon.contrat AS contrat_label,
            dc.salaire_min, dc.salaire_max,
            dc.cv_storage_path, dc.date_soumission
        FROM `{TABLE_CLIENTS}` dc
        JOIN `{PROJECT_ID}.{DATASET}.dim_formations` df ON df.id_formation = dc.id_formation
        JOIN `{PROJECT_ID}.{DATASET}.dim_experiences` de ON de.id_experience = dc.id_experience
        JOIN `{PROJECT_ID}.{DATASET}.dim_contrats` dcon ON dcon.id_contrat = dc.id_contrat
        WHERE dc.id_client = @id_client
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("id_client", "STRING", id_client)]
    )
    rows = list(client.query(query_profil, job_config=job_config).result())
    if not rows:
        return None
    profil = dict(rows[0].items())

    def _fetch_liste(table_bridge: str, table_dim: str, col_id: str, col_label: str) -> list[dict]:
        q = f"""
            SELECT b.{col_id} AS id, d.{col_label} AS label
            FROM `{PROJECT_ID}.{DATASET}.{table_bridge}` b
            JOIN `{PROJECT_ID}.{DATASET}.{table_dim}` d ON d.{col_id} = b.{col_id}
            WHERE b.id_client = @id_client
            ORDER BY d.{col_label}
        """
        jc = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("id_client", "STRING", id_client)]
        )
        return [dict(r.items()) for r in client.query(q, job_config=jc).result()]

    profil["competences"] = _fetch_liste("bridge_clients_competences", "dim_competences", "id_competence", "competence")
    profil["metiers"] = _fetch_liste("bridge_clients_metiers", "dim_metiers", "id_metier", "nom")
    profil["localisations"] = _fetch_liste("bridge_clients_localisations", "dim_localisations", "id_localisation", "ville")

    return profil


def get_recommendations(id_client: str, top_n: int = 10) -> list[dict]:
    """Calcule les recommandations d'offres pour un client, en temps réel.

    Filtre dur sur 6 critères : formation, expérience, contrat (exigences
    objectives de l'offre) + localisation, salaire, métier (préférences
    client sans ambiguïté possible).

    Métier passé en filtre dur le 23 août (option retenue : "C", sans repli
    automatique) — remplace le score gradué par embedding utilisé jusque-là.
    Historique complet de la décision : filtre dur (trop restrictif au
    départ, écarté) -> score plat 1.0/0.3 (trop permissif, écarté) -> score
    gradué par embedding (retenu un temps) -> filtre dur de nouveau,
    définitivement cette fois. Le déclencheur : sur le profil "Hermann"
    (Analytics Engineer/Data Analyst), la seule offre disponible à Toulouse
    était un Data Scientist à 35% — comportement jugé plus déroutant que
    rigoureux une fois montré concrètement. Assumé : ça peut désormais
    renvoyer 0 résultat si aucune offre du métier exact n'existe dans la
    zone/le budget du client (cas réel : Toulouse, Analytics Engineer/Data
    Analyst -> 0 résultat, alors qu'un Data Scientist existait).

    Exclut aussi les offres sans aucune compétence renseignée dans
    bridge_offres_competences (score_exact/score_embedding y seraient à 0
    par construction, faussant le classement).

    Le filtre salaire est un chevauchement (overlap), pas une inclusion
    stricte : l'offre est retenue dès que sa fourchette recoupe celle du
    client, même partiellement (ex. offre 24-36k acceptée pour un client
    30-60k). Avant, `salaire_min >= client_min AND salaire_max <= client_max`
    excluait à tort des offres qui chevauchaient réellement le budget
    (diagnostiqué sur le profil test "Camille Dubois" : 4 offres Data
    Engineer à 24-36k perdues sur un budget 30-60k, malgré 6k€ de
    recoupement réel).

    score_final combine 2 scores, renormalisés après le retrait de
    score_metier (le métier étant maintenant un filtre dur à correspondance
    exacte garantie, un score métier gradué n'apporterait plus rien à
    distinguer) :
        score_final = 0.625 × score_exact + 0.375 × score_embedding

    Dédoublonnage par contenu (métier + entreprise + salaire) en toute fin de
    requête : un même recruteur republie parfois la même annonce à quelques
    jours d'écart sous un id différent (cas confirmé : REXEL FRANCE, deux
    annonces identiques du 05/08 et du 07/08 pour le même poste). Ce n'est
    pas un doublon d'ingestion — chaque id_offre est légitime et unique côté
    fact_offres — mais afficher deux fois la même offre à l'utilisateur
    n'apporte rien. On garde la ligne au score le plus haut par groupe.
    """
    query = f"""
        WITH
        client_profile AS (
            SELECT
                dc.id_client,
                df.rang_formation AS client_rang_formation,
                de.rang_experience AS client_rang_experience,
                dc.id_contrat AS client_id_contrat,
                dc.salaire_min AS client_salaire_min,
                dc.salaire_max AS client_salaire_max
            FROM `{PROJECT_ID}.{DATASET}.dim_clients` dc
            JOIN `{PROJECT_ID}.{DATASET}.dim_formations` df ON df.id_formation = dc.id_formation
            JOIN `{PROJECT_ID}.{DATASET}.dim_experiences` de ON de.id_experience = dc.id_experience
            WHERE dc.id_client = @id_client
        ),
        offres_eligibles AS (
            SELECT
                fo.id_offre, fo.id_metier, fo.id_entreprise, fo.id_localisation,
                fo.salaire_min AS offre_salaire_min, fo.salaire_max AS offre_salaire_max,
                fo.lien_offre
            FROM `{PROJECT_ID}.{DATASET}.fact_offres` fo
            JOIN `{PROJECT_ID}.{DATASET}.dim_formations` df ON df.id_formation = fo.id_formation
            JOIN `{PROJECT_ID}.{DATASET}.dim_experiences` de ON de.id_experience = fo.id_experience
            CROSS JOIN client_profile cp
            WHERE cp.client_rang_formation >= df.rang_formation
              AND cp.client_rang_experience >= de.rang_experience
              AND fo.id_contrat = cp.client_id_contrat
              AND fo.id_localisation IN (
                  SELECT id_localisation FROM `{PROJECT_ID}.{DATASET}.bridge_clients_localisations`
                  WHERE id_client = @id_client
              )
              AND fo.salaire_min <= cp.client_salaire_max
              AND fo.salaire_max >= cp.client_salaire_min
              AND fo.id_metier IN (
                  SELECT id_metier FROM `{PROJECT_ID}.{DATASET}.bridge_clients_metiers`
                  WHERE id_client = @id_client
              )
              AND fo.id_offre IN (
                  SELECT DISTINCT id_offre FROM `{PROJECT_ID}.{DATASET}.bridge_offres_competences`
              )
        ),
        competences_client AS (
            SELECT id_competence FROM `{PROJECT_ID}.{DATASET}.bridge_clients_competences`
            WHERE id_client = @id_client
        ),
        score_exact_calc AS (
            SELECT
                boc.id_offre,
                COUNT(DISTINCT CASE WHEN cc.id_competence IS NOT NULL THEN boc.id_competence END)
                    / NULLIF(COUNT(DISTINCT boc.id_competence), 0) AS score_exact
            FROM `{PROJECT_ID}.{DATASET}.bridge_offres_competences` boc
            LEFT JOIN competences_client cc ON cc.id_competence = boc.id_competence
            WHERE boc.id_offre IN (SELECT id_offre FROM offres_eligibles)
            GROUP BY boc.id_offre
        ),
        texte_competences_client AS (
            SELECT STRING_AGG(dc.competence, ', ') AS content
            FROM competences_client cc
            JOIN `{PROJECT_ID}.{DATASET}.dim_competences` dc ON dc.id_competence = cc.id_competence
        ),
        -- Aligné sur bridge_offres_clients.sql : même top_k, et normalisation
        -- min-max calculée sur les seules offres éligibles (jointure ci-dessous),
        -- pas sur tout le catalogue — sinon le score dépendrait d'offres que le
        -- client ne verra jamais, et différerait du score persisté.
        score_embedding_raw AS (
            SELECT base.id_offre, distance
            FROM VECTOR_SEARCH(
                TABLE `{PROJECT_ID}.{DATASET}.offres_embeddings`,
                'ml_generate_embedding_result',
                (
                    SELECT ml_generate_embedding_result
                    FROM ML.GENERATE_EMBEDDING(
                        MODEL `{PROJECT_ID}.{DATASET}.embedding_model`,
                        (SELECT content FROM texte_competences_client)
                    )
                ),
                top_k => 5000
            )
            JOIN offres_eligibles oe ON oe.id_offre = base.id_offre
        ),
        score_embedding_calc AS (
            SELECT
                id_offre,
                SAFE_DIVIDE(
                    MAX(distance) OVER () - distance,
                    NULLIF(MAX(distance) OVER () - MIN(distance) OVER (), 0)
                ) AS score_embedding
            FROM score_embedding_raw
        )
        SELECT
            oe.id_offre,
            dm.nom AS metier,
            dent.nom AS entreprise,
            dl.ville AS ville,
            oe.offre_salaire_min,
            oe.offre_salaire_max,
            oe.lien_offre,
            COALESCE(sec.score_exact, 0) AS score_exact,
            COALESCE(sem.score_embedding, 0) AS score_embedding,
            ROUND(
                0.625 * COALESCE(sec.score_exact, 0)
                + 0.375 * COALESCE(sem.score_embedding, 0)
            , 3) AS score_final
        FROM offres_eligibles oe
        LEFT JOIN score_exact_calc sec ON sec.id_offre = oe.id_offre
        LEFT JOIN score_embedding_calc sem ON sem.id_offre = oe.id_offre
        LEFT JOIN `{PROJECT_ID}.{DATASET}.dim_metiers` dm ON dm.id_metier = oe.id_metier
        LEFT JOIN `{PROJECT_ID}.{DATASET}.dim_entreprises` dent ON dent.id_entreprise = oe.id_entreprise
        LEFT JOIN `{PROJECT_ID}.{DATASET}.dim_localisations` dl ON dl.id_localisation = oe.id_localisation
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY oe.id_metier, oe.id_entreprise,
                CAST(oe.offre_salaire_min AS INT64), CAST(oe.offre_salaire_max AS INT64)
            ORDER BY score_final DESC
        ) = 1
        ORDER BY score_final DESC
        LIMIT {top_n}
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("id_client", "STRING", id_client)]
    )
    rows = get_bq_client().query(query, job_config=job_config).result()
    return [dict(row.items()) for row in rows]