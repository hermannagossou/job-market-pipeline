"""Profils clients : recherche par email, enregistrement (upsert), CV, lecture.

Tout est écrit en DML (MERGE / DELETE / INSERT / UPDATE), jamais en streaming
(`insert_rows_json`) : BigQuery interdit de modifier des lignes écrites en
streaming pendant ~30-90 min, ce qui empêcherait un client de corriger un
profil qu'il vient de soumettre.
"""
import uuid
from datetime import datetime, timezone

from google.cloud import bigquery, storage

from api.core.config import get_settings
from api.db.bigquery import run_query
from api.schemas.clients import ClientEnregistre, ProfilClient, ProfilClientIn
from api.schemas.referentiels import ReferentielItem
from api.services import referentiels
from api.services.base import table


class ReferenceInconnueError(ValueError):
    """Un id soumis n'existe pas dans le référentiel correspondant."""


def _param_id_client(id_client: str) -> list[bigquery.ScalarQueryParameter]:
    return [bigquery.ScalarQueryParameter("id_client", "STRING", id_client)]


def find_client_by_email(email: str) -> str | None:
    """Recherche pure : ne crée jamais d'id. Ce n'est pas une authentification —
    quiconque connaît l'email accède au profil."""
    sql = f"SELECT id_client FROM {table('dim_clients')} WHERE email = @email LIMIT 1"
    rows = run_query(sql, [bigquery.ScalarQueryParameter("email", "STRING", email.strip().lower())])
    return rows[0]["id_client"] if rows else None


def client_exists(id_client: str) -> bool:
    sql = f"SELECT 1 FROM {table('dim_clients')} WHERE id_client = @id_client LIMIT 1"
    return bool(run_query(sql, _param_id_client(id_client)))


def _verifier_references(profil: ProfilClientIn) -> None:
    """Refuse tout id absent des référentiels — l'API ne fait pas confiance au
    formulaire, n'importe quel client HTTP peut l'appeler."""
    a_verifier = [
        ("formations", [profil.id_formation]),
        ("experiences", [profil.id_experience]),
        ("contrats", [profil.id_contrat]),
        ("competences", profil.ids_competences),
        ("metiers", profil.ids_metiers),
        ("localisations", profil.ids_localisations),
    ]
    for nom, ids in a_verifier:
        inconnus = set(ids) - referentiels.as_dict(nom).keys()
        if inconnus:
            raise ReferenceInconnueError(f"{nom} : id(s) inconnu(s) {sorted(inconnus)}")


def upsert_client_profile(profil: ProfilClientIn) -> ClientEnregistre:
    """Crée ou met à jour le profil identifié par son email : MERGE sur
    dim_clients puis remplacement complet des 3 bridges, en un seul script.

    cv_storage_path n'est jamais touché ici (seulement par `save_cv`) : modifier
    son profil sans redéposer de CV conserve le CV existant.
    """
    _verifier_references(profil)

    id_client = find_client_by_email(profil.email)
    est_nouveau = id_client is None
    if est_nouveau:
        id_client = str(uuid.uuid4())

    script = f"""
    MERGE {table('dim_clients')} AS target
    USING (SELECT
        @id_client AS id_client, @nom AS nom, @prenom AS prenom, @email AS email,
        @id_formation AS id_formation, @id_experience AS id_experience, @id_contrat AS id_contrat,
        @salaire_min AS salaire_min, @salaire_max AS salaire_max,
        @date_soumission AS date_soumission
    ) AS source
    ON target.id_client = source.id_client
    WHEN MATCHED THEN UPDATE SET
        nom = source.nom, prenom = source.prenom, email = source.email,
        id_formation = source.id_formation, id_experience = source.id_experience,
        id_contrat = source.id_contrat, salaire_min = source.salaire_min, salaire_max = source.salaire_max,
        date_soumission = source.date_soumission
    WHEN NOT MATCHED THEN INSERT (
        id_client, nom, prenom, email, id_formation, id_experience, id_contrat,
        salaire_min, salaire_max, cv_storage_path, date_soumission
    )
    VALUES (
        source.id_client, source.nom, source.prenom, source.email, source.id_formation,
        source.id_experience, source.id_contrat, source.salaire_min, source.salaire_max,
        NULL, source.date_soumission
    );

    DELETE FROM {table('bridge_clients_competences')} WHERE id_client = @id_client;
    INSERT INTO {table('bridge_clients_competences')} (id_client, id_competence)
    SELECT @id_client, id_competence FROM UNNEST(@competences) AS id_competence;

    DELETE FROM {table('bridge_clients_metiers')} WHERE id_client = @id_client;
    INSERT INTO {table('bridge_clients_metiers')} (id_client, id_metier)
    SELECT @id_client, id_metier FROM UNNEST(@metiers) AS id_metier;

    DELETE FROM {table('bridge_clients_localisations')} WHERE id_client = @id_client;
    INSERT INTO {table('bridge_clients_localisations')} (id_client, id_localisation)
    SELECT @id_client, id_localisation FROM UNNEST(@localisations) AS id_localisation;
    """
    params = [
        bigquery.ScalarQueryParameter("id_client", "STRING", id_client),
        bigquery.ScalarQueryParameter("nom", "STRING", profil.nom),
        bigquery.ScalarQueryParameter("prenom", "STRING", profil.prenom),
        bigquery.ScalarQueryParameter("email", "STRING", profil.email),
        bigquery.ScalarQueryParameter("id_formation", "STRING", profil.id_formation),
        bigquery.ScalarQueryParameter("id_experience", "STRING", profil.id_experience),
        bigquery.ScalarQueryParameter("id_contrat", "STRING", profil.id_contrat),
        bigquery.ScalarQueryParameter("salaire_min", "FLOAT64", profil.salaire_min),
        bigquery.ScalarQueryParameter("salaire_max", "FLOAT64", profil.salaire_max),
        bigquery.ScalarQueryParameter("date_soumission", "DATE", datetime.now(timezone.utc).date()),
        # dict.fromkeys : dédoublonne en gardant l'ordre (PK des bridges)
        bigquery.ArrayQueryParameter("competences", "STRING", list(dict.fromkeys(profil.ids_competences))),
        bigquery.ArrayQueryParameter("metiers", "STRING", list(dict.fromkeys(profil.ids_metiers))),
        bigquery.ArrayQueryParameter("localisations", "STRING", list(dict.fromkeys(profil.ids_localisations))),
    ]
    run_query(script, params)
    return ClientEnregistre(id_client=id_client, est_nouveau=est_nouveau)


def save_cv(id_client: str, file_bytes: bytes) -> str:
    """Dépose le CV sur GCS (un fichier par client, écrasé en cas de nouveau
    dépôt) et enregistre son chemin dans dim_clients."""
    settings = get_settings()
    blob = storage.Client(project=settings.bq_project_id).bucket(settings.cv_bucket_name).blob(f"{id_client}.pdf")
    blob.upload_from_string(file_bytes, content_type="application/pdf")
    cv_storage_path = f"gs://{settings.cv_bucket_name}/{id_client}.pdf"

    sql = f"UPDATE {table('dim_clients')} SET cv_storage_path = @cv_storage_path WHERE id_client = @id_client"
    run_query(sql, _param_id_client(id_client) + [
        bigquery.ScalarQueryParameter("cv_storage_path", "STRING", cv_storage_path),
    ])
    return cv_storage_path


def get_client_profile(id_client: str) -> ProfilClient | None:
    sql_profil = f"""
    SELECT
        dc.id_client, dc.nom, dc.prenom, dc.email,
        dc.id_formation, df.niveau AS formation_label,
        dc.id_experience, de.niveau AS experience_label,
        dc.id_contrat, dcon.contrat AS contrat_label,
        dc.salaire_min, dc.salaire_max,
        dc.cv_storage_path, dc.date_soumission
    FROM {table('dim_clients')} dc
    JOIN {table('dim_formations')} df ON df.id_formation = dc.id_formation
    JOIN {table('dim_experiences')} de ON de.id_experience = dc.id_experience
    JOIN {table('dim_contrats')} dcon ON dcon.id_contrat = dc.id_contrat
    WHERE dc.id_client = @id_client
    """
    rows = run_query(sql_profil, _param_id_client(id_client))
    if not rows:
        return None

    def _liste(table_bridge: str, table_dim: str, col_id: str, col_label: str) -> list[ReferentielItem]:
        sql = f"""
        SELECT b.{col_id} AS id, d.{col_label} AS label
        FROM {table(table_bridge)} b
        JOIN {table(table_dim)} d ON d.{col_id} = b.{col_id}
        WHERE b.id_client = @id_client
        ORDER BY d.{col_label}
        """
        return [ReferentielItem(**r) for r in run_query(sql, _param_id_client(id_client))]

    return ProfilClient(
        **rows[0],
        competences=_liste("bridge_clients_competences", "dim_competences", "id_competence", "competence"),
        metiers=_liste("bridge_clients_metiers", "dim_metiers", "id_metier", "nom"),
        localisations=_liste("bridge_clients_localisations", "dim_localisations", "id_localisation", "ville"),
    )
