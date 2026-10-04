-- Clé de comparaison pour les libellés géographiques en texte libre : insensible
-- à la casse, aux accents et aux variantes d'apostrophe.
-- Ex. "Ile-De-France", "Île-de-France" -> "ile-de-france" ;
--     "Provence-Alpes-Cote D’azur" -> "provence-alpes-cote d'azur".
-- Ne sert qu'aux jointures : le libellé exposé reste celui du référentiel INSEE.
{% macro cle_geo(expr) %}
    lower(trim(regexp_replace(
        regexp_replace(normalize({{ expr }}, NFD), r"\p{M}", ""),
        r"[\x{2018}\x{2019}]", "'"
    )))
{% endmacro %}
