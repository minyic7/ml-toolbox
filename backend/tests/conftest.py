import pytest
from fastapi.testclient import TestClient

from ml_toolbox.main import app


@pytest.fixture(autouse=True)
def _isolate_data_dir(tmp_path, monkeypatch):
    """Point both stores at a temporary directory for every test."""
    projects = tmp_path / "projects"
    monkeypatch.setattr("ml_toolbox.services.store.PROJECTS_DIR", projects)
    monkeypatch.setattr("ml_toolbox.services.file_store.PROJECTS_DIR", projects)


@pytest.fixture()
def client():
    """Shared FastAPI TestClient."""
    return TestClient(app)


@pytest.fixture()
def create_pipeline(client):
    """Factory fixture: creates a pipeline and returns its id."""

    def _create(name: str = "Test Pipeline") -> str:
        resp = client.post("/api/pipelines", json={"name": name})
        assert resp.status_code == 201
        return resp.json()["id"]

    return _create


@pytest.fixture()
def two_node_pipeline(client, create_pipeline):
    """Create a pipeline with run → clean_data nodes. Returns (pid, n1_id, n2_id)."""
    pid = create_pipeline("Two Node Test")

    r1 = client.post(
        f"/api/pipelines/{pid}/nodes",
        json={"type": "ml_toolbox.nodes.demo.run", "position": {"x": 0, "y": 0}},
    )
    n1 = r1.json()["id"]

    r2 = client.post(
        f"/api/pipelines/{pid}/nodes",
        json={
            "type": "ml_toolbox.nodes.transform.clean",
            "position": {"x": 200, "y": 0},
        },
    )
    n2 = r2.json()["id"]

    return pid, n1, n2
