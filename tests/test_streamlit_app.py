"""Tests des vues Streamlit (AppTest) — l'API est simulée, aucun appel réseau."""
import sys
from pathlib import Path

import pytest
import streamlit as st
from streamlit.testing.v1 import AppTest

APP_DIR = Path(__file__).resolve().parent.parent / "streamlit-app"
sys.path.insert(0, str(APP_DIR))

import api_client  # noqa: E402

REFERENTIELS = {
    "formations": {"f5": "Bac+5", "f3": "Bac+3"},
    "experiences": {"e1": "Junior", "e2": "Confirmé"},
    "metiers": {"m1": "Data Engineer"},
    "contrats": {"c1": "CDI"},
    "competences": {"k1": "SQL", "k2": "Python"},
    "localisations": {"l1": "Paris"},
}

PROFIL = {
    "id_client": "id-123",
    "nom": "Dubois",
    "prenom": "Camille",
    "email": "camille@exemple.fr",
    "id_formation": "f5",
    "formation_label": "Bac+5",
    "id_experience": "e2",
    "experience_label": "Confirmé",
    "id_contrat": "c1",
    "contrat_label": "CDI",
    "salaire_min": 30000.0,
    "salaire_max": 60000.0,
    "cv_storage_path": None,
    "date_soumission": "2026-09-27",
    "competences": [{"id": "k1", "label": "SQL"}],
    "metiers": [{"id": "m1", "label": "Data Engineer"}],
    "localisations": [{"id": "l1", "label": "Paris"}],
}


@pytest.fixture
def api(monkeypatch):
    """Remplace chaque appel HTTP par une réponse simulée, et garde la trace des appels."""
    appels = []
    monkeypatch.setattr(api_client, "get_referentiel", lambda nom: REFERENTIELS[nom])

    def upsert(profil):
        appels.append(("upsert", profil))
        return {"id_client": "id-123", "est_nouveau": True}

    monkeypatch.setattr(api_client, "upsert_client", upsert)
    monkeypatch.setattr(api_client, "get_client_profile", lambda id_client: PROFIL)
    monkeypatch.setattr(
        api_client,
        "get_recommendations",
        lambda id_client, top_n=10: [
            {
                "id_offre": "o1",
                "metier": "Data Engineer",
                "entreprise": "REXEL FRANCE",
                "ville": "Paris",
                "offre_salaire_min": 40000.0,
                "offre_salaire_max": 50000.0,
                "lien_offre": "https://exemple.fr/offre",
                "score_exact": 0.5,
                "score_embedding": 1.0,
                "score_final": 0.688,
            }
        ],
    )
    return appels


def _page(nom: str) -> AppTest:
    return AppTest.from_file(str(APP_DIR / "views" / nom), default_timeout=10)


def test_mon_profil_affiche_les_referentiels(api):
    at = _page("mon_profil.py").run()
    assert not at.exception
    assert at.selectbox[0].options == ["Bac+5", "Bac+3"]


def test_mon_profil_soumission_envoie_le_profil(api):
    at = _page("mon_profil.py").run()
    at.text_input[1].input("Dubois")
    at.text_input[2].input("Camille")
    at.text_input[3].input("camille@exemple.fr")
    at.multiselect[0].select("k1")
    at.multiselect[1].select("m1")
    at.multiselect[2].select("l1")
    next(b for b in at.button if b.label == "Valider mon profil").click().run()

    assert not at.exception
    assert [a[0] for a in api] == ["upsert"]
    envoye = api[0][1]
    assert envoye["email"] == "camille@exemple.fr"
    assert envoye["ids_competences"] == ["k1"]
    assert at.session_state["last_client_id"] == "id-123"


def test_mon_profil_validation_locale(api):
    at = _page("mon_profil.py").run()
    next(b for b in at.button if b.label == "Valider mon profil").click().run()
    assert api == []  # rien envoyé à l'API
    assert any("nom est requis" in e.value for e in at.error)


def test_mon_profil_api_indisponible(monkeypatch):
    def ko(nom):
        raise api_client.ApiError("Impossible de joindre l'API")

    monkeypatch.setattr(api_client, "get_referentiel", ko)
    at = _page("mon_profil.py").run()
    assert not at.exception
    assert "Impossible de charger les référentiels" in at.error[0].value


def _recommandations(last_client_id: str | None = None) -> AppTest:
    """La vue utilise st.page_link : elle doit tourner sous le routeur app.py."""
    at = AppTest.from_file(str(APP_DIR / "app.py"), default_timeout=10)
    if last_client_id:
        at.session_state["last_client_id"] = last_client_id
    return at.switch_page("views/recommandations.py").run()


def test_recommandations_sans_client(api):
    at = _recommandations()
    assert not at.exception
    assert "Aucun profil trouvé" in at.info[0].value


def test_recommandations_affiche_les_offres(api):
    at = _recommandations("id-123")
    assert not at.exception
    assert at.metric[0].value == "69%"


def test_recherche_email_api_indisponible(api, monkeypatch):
    def ko(email):
        raise api_client.ApiError("Le service de données est momentanément indisponible.", status_code=503)

    monkeypatch.setattr(api_client, "find_client_by_email", ko)
    at = _page("mon_profil.py").run()
    at.text_input[0].input("camille@exemple.fr")
    next(b for b in at.button if b.label == "Me connecter").click().run()
    assert any("Recherche indisponible" in e.value for e in at.error)
    assert not at.warning  # pas de "aucun profil trouvé" trompeur en plus de l'erreur


@pytest.mark.parametrize(
    "page",
    ["accueil", "chercheur_emploi", "rh_recruteur", "analyste_marche", "analyse_geographique"],
)
def test_observatoire_api_injoignable(monkeypatch, page):
    """Chaque page de l'observatoire s'affiche avec un message d'erreur, sans
    planter, quand l'API ne répond pas."""

    def ko(*args, **kwargs):
        raise api_client.ApiError("Impossible de joindre l'API")

    monkeypatch.setattr(api_client, "_request", ko)
    st.cache_data.clear()  # sinon une réponse mise en cache par un autre test masquerait la panne
    at = AppTest.from_file(str(APP_DIR / "app.py"), default_timeout=10)
    at.switch_page(f"views/observatoire/{page}.py").run()
    assert not at.exception
    assert at.error
