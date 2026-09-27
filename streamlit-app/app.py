"""
Job Market — Point d'entrée
============================
Point d'entrée unique et fixe (streamlit run app.py). Déclare la navigation
via st.navigation()/st.Page(), en deux sections :

- Observatoire : analyse du marché de l'emploi Data (views/observatoire/)
- Recommandation : profil candidat et offres recommandées (views/)

Aucune logique métier ni accès aux données ici : les vues passent toutes par
l'API FastAPI (api_client.py) — l'app n'a besoin d'aucun identifiant GCP.
Chaque page fixe sa propre mise en page et son titre d'onglet
(st.set_page_config, appels additifs).
"""

import streamlit as st

st.set_page_config(page_title="Job Market", page_icon=":material/explore:")

pg = st.navigation(
    {
        "Observatoire": [
            st.Page("views/observatoire/accueil.py", title="Vue d'ensemble", icon=":material/insights:", default=True),
            st.Page("views/observatoire/chercheur_emploi.py", title="Chercheur d'emploi", icon=":material/person_search:"),
            st.Page("views/observatoire/rh_recruteur.py", title="RH / Recruteur", icon=":material/badge:"),
            st.Page("views/observatoire/analyste_marche.py", title="Analyste marché", icon=":material/monitoring:"),
            st.Page("views/observatoire/analyse_geographique.py", title="Analyse géographique", icon=":material/map:"),
        ],
        "Recommandation": [
            st.Page("views/mon_profil.py", title="Mon profil", icon=":material/person:"),
            st.Page("views/recommandations.py", title="Mes recommandations", icon=":material/target:"),
        ],
    }
)
pg.run()
