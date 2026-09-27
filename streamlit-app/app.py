"""
Job Market — Point d'entrée
============================
Point d'entrée unique et fixe (streamlit run app.py, ne change plus jamais).
Déclare la navigation et les titres de menu explicitement via
st.navigation()/st.Page() — indépendants des noms de fichiers, contrairement
à l'ancien système multipage automatique (dossier pages/, où le libellé du
script racine dans le menu était dérivé de son nom de fichier : "app").

Le contenu réel des deux pages vit dans views/ (mon_profil.py,
recommandations.py) — jamais dans ce fichier, qui ne fait que router.

Aucune logique métier ni accès aux données ici : les vues passent toutes par
l'API FastAPI (api_client.py) — l'app n'a besoin d'aucun identifiant GCP.
"""

import streamlit as st

st.set_page_config(page_title="Job Market", page_icon=":material/explore:", layout="centered")

mon_profil = st.Page("views/mon_profil.py", title="Mon profil", icon=":material/person:", default=True)
recommandations = st.Page("views/recommandations.py", title="Recommandations", icon=":material/target:")

pg = st.navigation([mon_profil, recommandations])
pg.run()
