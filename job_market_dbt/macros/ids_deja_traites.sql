-- Sous-requête des offres déjà traitées par un modèle IA (Gemini), pour les files
-- d'attente *_a_traiter : `id not in {{ ids_deja_traites('int_..._ai_x', 'x is not null') }}`.
--
-- Pourquoi une file d'attente matérialisée plutôt qu'un filtre incrémental dans le
-- modèle IA lui-même : BigQuery évalue ai.generate_text dans l'étape de jointure,
-- AVANT d'appliquer un `id not in (select id from {{ this }})` placé dans la même
-- requête. Mesuré le 20/10/2026 : un run sans aucune offre nouvelle relisait 369 208
-- lignes dans l'étape Gemini, et le projet a reçu ~150 000 appels réussis (et ~650 000
-- refusés pour quota) en une journée. Le modèle IA lit donc une table déjà filtrée, sur
-- une simple colonne : seules les offres réellement à traiter atteignent Gemini.
--
-- La table IA est référencée par son nom (pas par ref) : c'est son état AVANT le run
-- qui compte, et un ref créerait un cycle (le modèle IA dépend de la file d'attente).
-- Si elle n'existe pas encore, ou en --full-refresh, tout est à traiter.
-- `condition_reponse` : ce qui fait qu'une ligne compte comme traitée (en général
-- une réponse non NULL ; NULL = appel en erreur, quota ou délai, à retenter).
{% macro ids_deja_traites(nom_modele, condition_reponse='true') %}
    {%- set relation = none -%}
    {%- if execute and not flags.FULL_REFRESH -%}
        {%- set relation = adapter.get_relation(
            database=this.database, schema=this.schema, identifier=nom_modele
        ) -%}
    {%- endif -%}
    {%- if relation is not none -%}
        (select id from {{ relation }} where {{ condition_reponse }})
    {%- else -%}
        (select cast(null as string) as id from unnest([1]) where false)
    {%- endif -%}
{% endmacro %}
