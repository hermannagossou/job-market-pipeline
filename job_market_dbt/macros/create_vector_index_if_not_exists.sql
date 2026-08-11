{% macro create_vector_index_if_not_exists(index_name, column_name, index_type='IVF', distance_type='COSINE', num_lists=10, min_rows=5000) %}
  {% if execute %}
    {% set count_query %}
      SELECT COUNT(*) AS cnt FROM {{ this }}
    {% endset %}
    {% set count_results = run_query(count_query) %}
    {% set row_count = count_results.columns[0].values()[0] %}

    {% if row_count < min_rows %}
      {{ log("Table " ~ this ~ " a " ~ row_count ~ " lignes (< " ~ min_rows ~ " requis pour IVF) — index ignoré pour l'instant, VECTOR_SEARCH fonctionne sans (recherche exhaustive).", info=True) }}
    {% else %}
      {% set check_query %}
        SELECT COUNT(*) AS cnt
        FROM `{{ this.database }}.{{ this.schema }}.INFORMATION_SCHEMA.VECTOR_INDEXES`
        WHERE table_name = '{{ this.identifier }}' AND index_name = '{{ index_name }}'
      {% endset %}
      {% set results = run_query(check_query) %}
      {% set idx_count = results.columns[0].values()[0] %}
      {% if idx_count == 0 %}
        {% set create_stmt %}
          CREATE VECTOR INDEX `{{ index_name }}`
          ON {{ this }}({{ column_name }})
          OPTIONS(index_type='{{ index_type }}', distance_type='{{ distance_type }}', ivf_options='{"num_lists":{{ num_lists }}}')
        {% endset %}
        {% do run_query(create_stmt) %}
        {{ log("Index vectoriel créé sur " ~ this, info=True) }}
      {% else %}
        {{ log("Index vectoriel déjà présent sur " ~ this ~ ", rien à faire", info=True) }}
      {% endif %}
    {% endif %}
  {% endif %}
{% endmacro %}