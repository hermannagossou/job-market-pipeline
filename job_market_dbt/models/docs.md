{% docs src_raw_offres %}
Source brute des offres d'emploi collectées par les scripts d'ingestion (France Travail, Welcome to the Jungle),
stockées telles quelles avant toute transformation dbt.
{% enddocs %}

{% docs src_raw_france_travail_offres %}
Offres d'emploi brutes issues de l'API France Travail (ex Pôle Emploi), une ligne par offre,
payload JSON natif de l'API sans transformation.
{% enddocs %}

{% docs src_raw_wttj_offres %}
Offres d'emploi brutes issues de l'API Welcome to the Jungle (via Algolia), une ligne par offre,
champs internes Algolia (_highlightResult, _snippetResult, _rankingInfo) déjà exclus à l'ingestion.
{% enddocs %}