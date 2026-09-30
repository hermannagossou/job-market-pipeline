{% macro normalize_ville(ville_column) %}
    TRIM(REGEXP_REPLACE({{ ville_column }}, r'\s+(\d+e|1er)\s+Arrondissement$', ''))
{% endmacro %}