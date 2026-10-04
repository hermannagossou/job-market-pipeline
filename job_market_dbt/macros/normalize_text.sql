-- macros/normalize_text.sql
{% macro normalize_text(expr) %}
    upper(trim(
        regexp_replace(
            regexp_replace(
                normalize({{ expr }}, NFD),
                r'\pM',
                ''
            ),
            r"['\-]",
            ' '
        )
    ))
{% endmacro %}