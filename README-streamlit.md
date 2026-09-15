# Job Market Dashboard — Streamlit

Dashboard d'analyse du marché de l'emploi Data en France. Communique
exclusivement avec l'API FastAPI (`../api`) — aucun accès direct à BigQuery ou
aux modèles dbt depuis ce code.

## Structure

```
streamlit/
├── app.py                       # page d'accueil
├── config.py                    # configuration via variables d'environnement
├── services/api_client.py       # client HTTP vers l'API (seul point d'accès aux données)
├── components/
│   ├── sidebar.py                # filtres partagés par toutes les pages
│   └── kpi_cards.py               # cartes KPI + graphiques réutilisables
└── pages/
    ├── 1_Vue_d_ensemble.py
    ├── 2_Analyse_geographique.py
    ├── 3_Profils_recherches.py
    └── 4_Recommandation.py
```

## Installation

```bash
pip install -r requirements-streamlit.txt
```

## Fonds de carte (à faire une fois)

Les cartes choroplèthes de France (régions et départements) ont besoin de
fichiers GeoJSON, à télécharger une seule fois après le clone :

```bash
python streamlit/scripts/download_geojson.py
```

Cela crée `streamlit/assets/regions.geojson` et `departements.geojson`. Sans
ces fichiers, le dashboard reste fonctionnel : les cartes sont automatiquement
remplacées par des graphiques en barres, avec un message indiquant la commande
à lancer.

## Configuration

```bash
export API_BASE_URL=http://localhost:8000   # adresse de l'API FastAPI
```

(Windows PowerShell : `$env:API_BASE_URL = "http://localhost:8000"`)

## Lancement

L'API doit être démarrée au préalable (voir `README-api.md`), puis :

```bash
cd streamlit
streamlit run app.py
```

## Pages

1. **Vue d'ensemble** — KPIs globaux, évolution du volume d'offres, top
   régions/secteurs/métiers/compétences.
2. **Analyse géographique** — comparaison entre régions, puis détail complet
   (secteurs, métiers, compétences, contrats, évolution) une fois une région
   sélectionnée dans la sidebar.
3. **Profils recherchés** — répond aux questions "quels métiers recrutent le
   plus", "quelles compétences sont demandées", en croisant avec les filtres
   secteur/région de la sidebar.
4. **Recommandation** — dépôt de CV et affichage des offres correspondantes.
   **Non fonctionnelle tant que le moteur de recommandation du collègue n'est
   pas branché sur l'API** (route `POST /api/recommandation/cv` à créer) —
   l'interface gère ce cas proprement (message explicite plutôt qu'une erreur brute).

## Choix techniques à connaître pour la soutenance

- **Cache** : `st.cache_data(ttl=300)` sur tous les appels API en lecture —
  5 minutes de cache, cohérent avec une ingestion quotidienne des données.
  Évite de re-solliciter l'API à chaque interaction (changement de filtre,
  changement de page).
- **Filtres communs** : un seul composant `render_sidebar()` réutilisé sur
  toutes les pages, pour garantir des filtres strictement identiques partout
  (même libellés, même comportement) plutôt que de dupliquer la logique.
- **Gestion d'erreurs** : toute erreur réseau/HTTP est convertie en `ApiError`
  avec un message utilisateur clair (`services/api_client.py`), jamais une
  trace Python brute affichée à l'utilisateur.
- **États vides** : chaque graphique affiche un message dédié
  (`st.info(...)`) plutôt qu'un graphique vide quand aucune donnée ne
  correspond aux filtres actifs (`components/kpi_cards.render_bar_chart`).

## Limitation connue

Pas de carte de France choroplèthe interactive (Page 2) : nécessiterait un
fichier GeoJSON des contours de régions françaises, indisponible dans
l'environnement où ce code a été écrit (pas d'accès réseau pour le
télécharger). Le graphique en barres "Offres par région" fournit la même
information de concentration géographique en attendant. Pour l'ajouter :
récupérer un GeoJSON des régions françaises (ex. sur data.gouv.fr) et
utiliser `plotly.express.choropleth`.

## Non testé

Comme pour l'API, ce code n'a pas pu être exécuté dans cet environnement
(pas de Streamlit installé, pas d'API en cours d'exécution à interroger).
Seule la syntaxe Python a été vérifiée. À tester en priorité :
1. `streamlit run app.py` démarre et affiche la page d'accueil sans erreur.
2. La sidebar charge bien les listes de filtres depuis l'API.
3. Chaque page affiche ses graphiques une fois l'API et BigQuery accessibles.
