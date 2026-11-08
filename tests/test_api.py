
from fastapi.testclient import TestClient

from app.main import app


client = TestClient(app)


def test_root_endpoint():

    response = client.get("/")

    assert response.status_code == 200

    data = response.json()

    assert data["message"] == "Welcome to Mini Task Queue"


def test_health_endpoint():

    response = client.get("/health")

    assert response.status_code == 200

    data = response.json()

    assert data["status"] == "healthy"
    assert data["application"] == "mini-task-queue"
    assert data["redis"] == "connected"


def test_swagger_documentation():

    response = client.get("/docs")

    assert response.status_code == 200
