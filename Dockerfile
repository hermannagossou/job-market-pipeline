FROM astrocrpublic.azurecr.io/runtime:3.3-8

# dbt Fusion : binaire compilé (pas de package pip), installé dans /home/astro/.local/bin.
# Version figée sur celle utilisée en local.
ARG DBT_FUSION_VERSION=2.0.0-preview.218
RUN curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --version ${DBT_FUSION_VERSION}
ENV PATH="/home/astro/.local/bin:${PATH}"

# Le projet complet (dags/, ingestion/, job_market_dbt/, include/) est déjà copié dans
# /usr/local/airflow par les instructions ONBUILD de l'image de base.
