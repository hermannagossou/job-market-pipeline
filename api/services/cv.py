"""Analyse d'un CV : texte (pdfplumber) -> JSON structuré (Gemini) -> résolution
vers les référentiels (embedding + VECTOR_SEARCH pour compétences/métiers,
règles pour formation/expérience).

Le résultat n'est qu'une suggestion : rien n'est enregistré ici, le client
valide ou corrige chaque valeur dans le formulaire avant de soumettre.
"""
import io
import json
import logging
from functools import lru_cache

import pdfplumber
import vertexai
from google.cloud import bigquery
from vertexai.generative_models import GenerationConfig, GenerativeModel

from api.core.config import get_settings
from api.db.bigquery import BigQueryQueryError, run_query
from api.schemas.cv import AnalyseCV, ResolutionReferentiel
from api.services import referentiels
from api.services.base import table

logger = logging.getLogger(__name__)

# Règles de résolution formation/expérience — heuristiques simples.
FORMATION_KEYWORDS = {
    "Doctorat": ["doctorat", "phd", "thèse", "these"],
    "Bac+5": ["bac+5", "master 2", "m2", "ingénieur", "ingenieur", "mastère", "mba", "master"],
    "Bac+4": ["bac+4", "master 1", "m1"],
    "Bac+3": ["bac+3", "licence", "bachelor"],
    "Bac+2": ["bac+2", "bts", "dut"],
}
# Aligné sur la classification côté offres (int_france_travail_niveau_experience.sql) :
# 0-2 ans Junior, 3-5 Confirmé, 6-10 Senior, >10 Expert.
EXPERIENCE_SEUILS = [(2, "Junior"), (5, "Confirmé"), (10, "Senior")]  # au-delà -> "Expert"

_MESSAGE_RESOLUTION_KO = "Résolution automatique indisponible — sélectionne les valeurs manuellement."


class CvIllisibleError(ValueError):
    """Le PDF ne contient aucun texte exploitable (PDF scanné, fichier corrompu...)."""


class AnalyseIAError(RuntimeError):
    """Gemini n'a pas renvoyé de JSON exploitable, ou n'a pas pu être appelé."""


@lru_cache
def _get_gemini_model() -> GenerativeModel:
    settings = get_settings()
    vertexai.init(project=settings.bq_project_id, location=settings.vertex_location)
    return GenerativeModel(settings.gemini_model)


def extract_text_from_pdf(file_bytes: bytes) -> str:
    try:
        with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
            text = "\n".join(page.extract_text() or "" for page in pdf.pages)
    except Exception as exc:  # noqa: BLE001 - pdfplumber lève des erreurs variées sur un PDF invalide
        raise CvIllisibleError("Le fichier n'a pas pu être lu comme un PDF.") from exc
    if not text.strip():
        raise CvIllisibleError("Aucun texte n'a pu être extrait — le PDF est peut-être scanné (image).")
    return text


def extract_cv_info(texte_cv: str) -> dict:
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
    config = GenerationConfig(response_mime_type="application/json", temperature=0.1)
    try:
        response = _get_gemini_model().generate_content(prompt, generation_config=config)
        return json.loads(response.text)
    except Exception as exc:  # noqa: BLE001 - réseau, quota, JSON mal formé : même issue pour le client
        logger.exception("Échec de l'extraction structurée du CV par Gemini")
        raise AnalyseIAError("L'analyse automatique du CV a échoué.") from exc


def _resolve_via_embedding(
    termes: list[str], table_embeddings: str, id_col: str
) -> tuple[list[ResolutionReferentiel], str | None]:
    """Rapproche chaque terme extrait de la valeur la plus proche du référentiel
    (VECTOR_SEARCH, top_k=1). Retourne (résolutions, message d'erreur ou None)."""
    if not termes:
        return [], None
    # Ne jamais aliaser manuellement le résultat de VECTOR_SEARCH : ses colonnes
    # sont exposées via les alias internes `query` et `base`.
    sql = f"""
    SELECT
        query.content AS extrait,
        base.{id_col} AS id,
        base.content AS label,
        distance
    FROM VECTOR_SEARCH(
        TABLE {table(table_embeddings)},
        'ml_generate_embedding_result',
        (
            SELECT ml_generate_embedding_result, content
            FROM ML.GENERATE_EMBEDDING(
                MODEL {table('embedding_model')},
                (SELECT terme AS content FROM UNNEST(@termes) AS terme)
            )
        ),
        top_k => 1
    )
    """
    try:
        rows = run_query(sql, [bigquery.ArrayQueryParameter("termes", "STRING", termes)])
    except BigQueryQueryError:
        return [], _MESSAGE_RESOLUTION_KO
    return [ResolutionReferentiel(**row) for row in rows], None


def resolve_formation(texte_niveau: str | None) -> str | None:
    texte = (texte_niveau or "").lower()
    ids_par_label = {label: fid for fid, label in referentiels.as_dict("formations").items()}
    for label, keywords in FORMATION_KEYWORDS.items():
        if any(kw in texte for kw in keywords):
            return ids_par_label.get(label)
    return None


def resolve_experience(annees) -> str | None:
    try:
        annees = int(annees)
    except (TypeError, ValueError):
        return None
    label = next((seuil_label for seuil, seuil_label in EXPERIENCE_SEUILS if annees <= seuil), "Expert")
    ids_par_label = {lbl: eid for eid, lbl in referentiels.as_dict("experiences").items()}
    return ids_par_label.get(label)


def analyser_cv(file_bytes: bytes) -> AnalyseCV:
    extraction = extract_cv_info(extract_text_from_pdf(file_bytes))

    competences, erreur_competences = _resolve_via_embedding(
        extraction.get("competences") or [], "dim_competences_embeddings", "id_competence"
    )
    metiers, erreur_metiers = _resolve_via_embedding(
        extraction.get("metiers") or [], "dim_metiers_embeddings", "id_metier"
    )
    return AnalyseCV(
        nom=extraction.get("nom"),
        prenom=extraction.get("prenom"),
        email=extraction.get("email"),
        id_formation=resolve_formation(extraction.get("niveau_formation")),
        id_experience=resolve_experience(extraction.get("annees_experience")),
        competences=competences,
        metiers=metiers,
        erreur_competences=erreur_competences,
        erreur_metiers=erreur_metiers,
    )
