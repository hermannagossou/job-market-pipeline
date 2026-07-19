{% docs int_offres %}
Point d'union multi-sources pour les offres d'emploi normalisées. **Une ligne = une offre.**

Actuellement alimenté par France Travail uniquement. Les futures plateformes (WTTJ, etc.)
seront intégrées via `UNION ALL` dans ce modèle.
{% enddocs %}


{% docs int_offres_competences %}
Point d'union multi-sources pour les compétences techniques extraites. **Une ligne = un couple (offre, compétence).**

Actuellement alimenté par France Travail uniquement. Les futures plateformes seront intégrées
via `UNION ALL` dans ce modèle.
{% enddocs %}


{% docs int_offres_langues %}
Point d'union multi-sources pour les langues extraites. **Une ligne = un couple (offre, langue).**

Actuellement alimenté par France Travail uniquement. Les futures plateformes seront intégrées
via `UNION ALL` dans ce modèle.
{% enddocs %}
