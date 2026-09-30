# Job Market API

API FastAPI servant de couche intermédiaire entre le dashboard Streamlit et les
données BigQuery (modèles `marts` du projet dbt : `fact_offres` + dimensions).

## Architecture

```
api/
├── main.py            # instanciation FastAPI, montage des routers, gestion d'erreurs
├── core/config.py      # configuration via variables d'environnement (aucun secret en dur)
├── db/bigquery.py      # client BigQuery partagé + exécution de requêtes paramétrées
├── dependencies.py     # filtres communs à tous les endpoints (Depends())
├── schemas/             # modèles Pydantic de réponse
├── services/             # construction des requêtes SQL (agrégation faite côté BigQuery)
└── routes/               # endpoints FastAPI, un fichier par domaine
```

**Principe** : toute agrégation (comptages, moyennes, groupements) est faite côté
BigQuery, jamais en récupérant des lignes brutes puis en les agrégeant en Python.
Chaque service construit une requête SQL paramétrée (`@nom_param`, jamais de
valeur utilisateur concaténée directement) à partir des filtres actifs.

## Installation

```bash
pip install -r requirements-api.txt
```

## Configuration

```bash
cp .env.example .env
# Éditez .env avec votre BQ_PROJECT_ID et BQ_DATASET réels
```

Si vous êtes en local avec des credentials par défaut (`gcloud auth application-default login`),
laissez `GOOGLE_APPLICATION_CREDENTIALS` vide.

## Lancement

```bash
uvicorn api.main:app --reload --port 8000
```

Documentation interactive : http://localhost:8000/docs

## Endpoints disponibles

Tous les endpoints (sauf `/`) acceptent le même jeu de filtres en query params :
`region`, `departement`, `secteur`, `metier`, `type_contrat`, `niveau_experience`,
`niveau_formation`, `date_debut`, `date_fin`.

| Méthode | Route | Description |
|---|---|---|
| GET | `/api/kpis/overview` | KPIs globaux (nb offres, entreprises, métiers, régions, salaires moyens) |
| GET | `/api/geo/regions` | Nombre d'offres par région |
| GET | `/api/geo/departements` | Nombre d'offres par département |
| GET | `/api/metiers/repartition` | Top métiers (paramètre `limit`) |
| GET | `/api/metiers/evolution` | Évolution mensuelle du volume (filtrer par `metier` pour une courbe unique) |
| GET | `/api/competences/repartition` | Top compétences (paramètres `group_by`, `limit`) |
| GET | `/api/competences/evolution` | Évolution mensuelle pour une compétence (paramètre requis `competence`) |
| GET | `/api/contrats/repartition` | Répartition par type de contrat |
| GET | `/api/profil/repartition` | Répartition formation + expérience en un seul appel |
| GET | `/api/offres` | Liste paginée d'offres détaillées (`page`, `page_size`) |

## Points d'attention connus (hérités du modèle dbt)

- **Comptage d'offres** : `fact_offres` a pour grain 1 ligne = 1 offre × 1 plateforme.
  Les endpoints utilisent `COUNT(DISTINCT id_offre)`. Un modèle séparé
  (`int_offres_doublons`, pas encore dans les marts) détecte les republications
  — pas encore exposé par l'API, à voir si le dashboard doit distinguer
  "annonces publiées" de "postes distincts estimés".
- **Salaires** : `statut_salaire` (Déclaré / Incomplet / Estimé) doit être affiché
  ou filtrable dans le dashboard pour ne pas mélanger du réel et de l'imputé
  sans le signaler. `/api/kpis/overview` expose déjà la ventilation.
- **Recommandation CV** : pas encore intégrée (développée séparément par un
  collègue). Aucune route de ce type pour l'instant.

## Non testé

Le code n'a pas pu être exécuté ni testé contre une vraie instance BigQuery
dans cet environnement (pas d'accès réseau ni de credentials). Seule la
syntaxe Python a été vérifiée (`py_compile`). À tester en priorité une fois
en local :
1. `uvicorn api.main:app --reload` démarre sans erreur d'import.
2. `/api/kpis/overview` sans filtre retourne bien un JSON cohérent.
3. Un filtre invalide (valeur inexistante) retourne une liste/valeur vide
   plutôt qu'une erreur.
