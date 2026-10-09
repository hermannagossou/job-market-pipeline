"""Tests des routes de recommandation de l'API — BigQuery simulé, aucune
requête réelle n'est envoyée."""
import pytest
from fastapi.testclient import TestClient

from api.main import app
from api.services import clients as clients_service
from api.services import cv as cv_service
from api.services import referentiels as referentiels_service

REFERENTIELS = {
    "dim_formations": [
        {"id": "f0", "label": "Non Renseigné"},
        {"id": "f5", "label": "Bac+5"},
    ],
    "dim_experiences": [
        {"id": "e0", "label": "Non Renseigné"},
        {"id": "e1", "label": "Junior"},
        {"id": "e2", "label": "Confirmé"},
        {"id": "e3", "label": "Senior"},
        {"id": "e4", "label": "Expert"},
    ],
    "dim_contrats": [{"id": "c1", "label": "CDI"}],
    "dim_competences": [{"id": "k1", "label": "SQL"}, {"id": "k2", "label": "Python"}],
    "dim_metiers": [{"id": "m1", "label": "Data Engineer"}],
    "dim_localisations": [{"id": "l1", "label": "Paris"}],
}

PROFIL_VALIDE = {
    "nom": "Dubois",
    "prenom": "Camille",
    "email": "  Camille.Dubois@Exemple.fr ",
    "id_formation": "f5",
    "id_experience": "e2",
    "id_contrat": "c1",
    "salaire_min": 30000,
    "salaire_max": 60000,
    "ids_competences": ["k1", "k2", "k1"],
    "ids_metiers": ["m1"],
    "ids_localisations": ["l1"],
}


def _fake_referentiels(sql, params=None):
    for nom_table, rows in REFERENTIELS.items():
        if f".{nom_table}`" in sql:
            return rows
    raise AssertionError(f"requête inattendue : {sql}")


@pytest.fixture
def client(monkeypatch):
    referentiels_service._cache.clear()
    monkeypatch.setattr(referentiels_service, "run_query", _fake_referentiels)
    return TestClient(app)


@pytest.fixture
def requetes_clients(monkeypatch):
    """Simule dim_clients vide et enregistre chaque requête envoyée."""
    envoyees = []

    def fake(sql, params=None):
        envoyees.append((sql, {p.name: getattr(p, "value", None) or getattr(p, "values", None) for p in params or []}))
        return []

    monkeypatch.setattr(clients_service, "run_query", fake)
    return envoyees


def test_referentiel_exclut_non_renseigne(client):
    response = client.get("/api/referentiels/experiences")
    assert response.status_code == 200
    assert [item["label"] for item in response.json()] == ["Junior", "Confirmé", "Senior", "Expert"]


def test_referentiel_inconnu_refuse(client):
    assert client.get("/api/referentiels/dim_clients").status_code == 422


def test_upsert_nouveau_client(client, requetes_clients):
    response = client.put("/api/clients", json=PROFIL_VALIDE)
    assert response.status_code == 200, response.text
    assert response.json()["est_nouveau"] is True

    lookup, (script, params) = requetes_clients
    assert lookup[1]["email"] == "camille.dubois@exemple.fr"
    assert "MERGE" in script
    assert params["email"] == "camille.dubois@exemple.fr"
    assert params["competences"] == ["k1", "k2"]  # dédoublonné pour la PK du bridge
    assert "cv_storage_path = " not in script  # le CV existant n'est jamais écrasé ici


@pytest.mark.parametrize(
    "modification",
    [
        {"salaire_min": 70000},
        {"ids_competences": []},
        {"email": "pas-un-email"},
        {"nom": "   "},
    ],
)
def test_upsert_profil_invalide(client, requetes_clients, modification):
    response = client.put("/api/clients", json={**PROFIL_VALIDE, **modification})
    assert response.status_code == 422
    assert requetes_clients == []


def test_upsert_id_inconnu_refuse(client, requetes_clients):
    response = client.put("/api/clients", json={**PROFIL_VALIDE, "ids_metiers": ["m1", "m999"]})
    assert response.status_code == 422
    assert "m999" in response.json()["detail"]
    assert requetes_clients == []


def test_upsert_niveau_non_renseigne_refuse(client, requetes_clients):
    response = client.put("/api/clients", json={**PROFIL_VALIDE, "id_experience": "e0"})
    assert response.status_code == 422


def test_recherche_email_inconnu(client, requetes_clients):
    assert client.get("/api/clients", params={"email": "x@y.fr"}).status_code == 404


def test_recommandations_client_inconnu(client, requetes_clients):
    assert client.get("/api/clients/inconnu/recommandations").status_code == 404


def test_analyse_cv_refuse_non_pdf(client):
    response = client.post("/api/cv/analyse", files={"fichier": ("cv.txt", b"texte", "text/plain")})
    assert response.status_code == 415


def test_analyse_cv_refuse_fichier_trop_gros(client):
    gros = b"0" * (5 * 1024 * 1024 + 10)
    response = client.post("/api/cv/analyse", files={"fichier": ("cv.pdf", gros, "application/pdf")})
    assert response.status_code == 413


def test_analyse_cv_pdf_illisible(client):
    response = client.post("/api/cv/analyse", files={"fichier": ("cv.pdf", b"pas un pdf", "application/pdf")})
    assert response.status_code == 422


@pytest.mark.parametrize(
    ("annees", "label"),
    [(0, "Junior"), (2, "Junior"), (3, "Confirmé"), (5, "Confirmé"), (6, "Senior"), (10, "Senior"), (11, "Expert")],
)
def test_resolve_experience_seuils(client, annees, label):
    ids = {v: k for k, v in referentiels_service.as_dict("experiences").items()}
    assert cv_service.resolve_experience(annees) == ids[label]


def test_resolve_experience_valeur_absente(client):
    assert cv_service.resolve_experience(None) is None
    assert cv_service.resolve_experience("beaucoup") is None
