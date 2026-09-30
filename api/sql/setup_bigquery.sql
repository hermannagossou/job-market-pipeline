-- Objets BigQuery dont l'API a besoin et que dbt ne crée pas.
-- À exécuter une fois par dataset (idempotent : IF NOT EXISTS partout) :
--     bq query --use_legacy_sql=false < api/sql/setup_bigquery.sql
-- Dataset cible : prod (projet job-market-de-492514).

-- Modèle d'embedding distant (Vertex AI), utilisé par les modèles dbt
-- *_embeddings et par l'API (résolution du CV, score embedding).
CREATE MODEL IF NOT EXISTS `job-market-de-492514.prod.embedding_model`
REMOTE WITH CONNECTION `US.job-market-conn`
OPTIONS (ENDPOINT = 'text-multilingual-embedding-002');

-- Tables alimentées par l'API (profils clients), déclarées côté dbt comme
-- source app_streamlit (models/marts/_sources_streamlit.yml).
CREATE TABLE IF NOT EXISTS `job-market-de-492514.prod.dim_clients` (
  id_client STRING NOT NULL,
  nom STRING NOT NULL,
  prenom STRING NOT NULL,
  email STRING NOT NULL,
  id_formation STRING NOT NULL,
  id_experience STRING NOT NULL,
  id_contrat STRING NOT NULL,
  salaire_min FLOAT64 NOT NULL,
  salaire_max FLOAT64 NOT NULL,
  cv_storage_path STRING,
  date_soumission DATE,
  PRIMARY KEY (id_client) NOT ENFORCED
);

CREATE TABLE IF NOT EXISTS `job-market-de-492514.prod.bridge_clients_competences` (
  id_client STRING NOT NULL,
  id_competence STRING NOT NULL,
  PRIMARY KEY (id_client, id_competence) NOT ENFORCED
);

CREATE TABLE IF NOT EXISTS `job-market-de-492514.prod.bridge_clients_metiers` (
  id_client STRING NOT NULL,
  id_metier STRING NOT NULL,
  PRIMARY KEY (id_client, id_metier) NOT ENFORCED
);

CREATE TABLE IF NOT EXISTS `job-market-de-492514.prod.bridge_clients_localisations` (
  id_client STRING NOT NULL,
  id_localisation STRING NOT NULL,
  PRIMARY KEY (id_client, id_localisation) NOT ENFORCED
);
