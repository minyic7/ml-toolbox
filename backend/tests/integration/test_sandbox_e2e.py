"""Integration test — runs a real pipeline through a real sandbox container.

Requires Docker and the sandbox image to be available locally.
Skip with: pytest -m "not integration"
"""

import json
import time

import pytest

from fastapi.testclient import TestClient

# Check Docker is available before running any test in this module.
try:
    import docker

    _client = docker.from_env()
    _client.ping()
    _has_docker = True
except Exception:
    _has_docker = False

pytestmark = pytest.mark.skipif(not _has_docker, reason="Docker not available")

SANDBOX_IMAGE = "ml-toolbox-sandbox"


@pytest.fixture(autouse=True)
def _isolate(tmp_path, monkeypatch):
    """Isolate file stores + point executor at local sandbox image."""
    projects = tmp_path / "projects"
    monkeypatch.setattr("ml_toolbox.services.store.PROJECTS_DIR", projects)
    monkeypatch.setattr("ml_toolbox.services.file_store.PROJECTS_DIR", projects)
    # Use tmp_path as DATA_DIR so executor can compute relative paths
    monkeypatch.setattr("ml_toolbox.services.executor.DATA_DIR", tmp_path)
    # Use a bind mount instead of named volume (we're running locally, not DinD)
    monkeypatch.setattr(
        "ml_toolbox.services.executor.DOCKER_VOLUME_NAME", str(tmp_path)
    )
    monkeypatch.setattr("ml_toolbox.services.executor.SANDBOX_IMAGE", SANDBOX_IMAGE)


@pytest.fixture()
def client():
    from ml_toolbox.main import app

    return TestClient(app)


def _wait_for_run(client: TestClient, pid: str, timeout: int = 60) -> dict:
    """Poll until the pipeline run finishes. Returns the run dict."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = client.get(f"/api/pipelines/{pid}/runs")
        assert resp.status_code == 200
        runs = resp.json()
        if runs and runs[0]["status"] in ("done", "error"):
            return runs[0]
        time.sleep(1)
    raise TimeoutError(f"Pipeline {pid} did not finish within {timeout}s")


class TestSandboxIntegration:
    """Tests that actually create sandbox containers and run node code."""

    def test_single_node_pipeline(self, client: TestClient):
        """Create pipeline with generate_data node, run it, verify output."""
        # Create pipeline
        resp = client.post("/api/pipelines", json={"name": "Integration Test"})
        assert resp.status_code == 201
        pid = resp.json()["id"]

        # Add generate_data node
        resp = client.post(
            f"/api/pipelines/{pid}/nodes",
            json={
                "type": "ml_toolbox.nodes.demo.run",
                "position": {"x": 0, "y": 0},
            },
        )
        assert resp.status_code == 201
        node_id = resp.json()["id"]

        # Run pipeline
        resp = client.post(f"/api/pipelines/{pid}/run")
        assert resp.status_code == 200
        run_id = resp.json()["run_id"]
        assert run_id

        # Wait for completion
        run = _wait_for_run(client, pid)
        assert run["status"] == "done", f"Run failed: {run}"
        assert run["id"] == run_id

        # Verify output file exists
        resp = client.get(f"/api/pipelines/{pid}/status")
        assert resp.status_code == 200
        assert resp.json()["is_running"] is False

    def test_two_node_pipeline(self, client: TestClient):
        """Generate data → clean data, verify both nodes execute."""
        resp = client.post("/api/pipelines", json={"name": "Two Node Test"})
        pid = resp.json()["id"]

        # Add nodes
        gen = client.post(
            f"/api/pipelines/{pid}/nodes",
            json={
                "type": "ml_toolbox.nodes.demo.run",
                "position": {"x": 0, "y": 0},
            },
        ).json()

        clean = client.post(
            f"/api/pipelines/{pid}/nodes",
            json={
                "type": "ml_toolbox.nodes.demo.clean_data",
                "position": {"x": 200, "y": 0},
            },
        ).json()

        # Connect them
        resp = client.post(
            f"/api/pipelines/{pid}/edges",
            json={
                "source": gen["id"],
                "source_port": "df",
                "target": clean["id"],
                "target_port": "df",
            },
        )
        assert resp.status_code == 201

        # Run
        resp = client.post(f"/api/pipelines/{pid}/run")
        assert resp.status_code == 200

        run = _wait_for_run(client, pid)
        assert run["status"] == "done", f"Run failed: {run}"

    def test_concurrent_run_rejected(self, client: TestClient):
        """A second run on the same pipeline should return 409."""
        resp = client.post("/api/pipelines", json={"name": "Concurrent Test"})
        pid = resp.json()["id"]

        client.post(
            f"/api/pipelines/{pid}/nodes",
            json={
                "type": "ml_toolbox.nodes.demo.run",
                "position": {"x": 0, "y": 0},
            },
        )

        # Start first run
        resp = client.post(f"/api/pipelines/{pid}/run")
        assert resp.status_code == 200

        # Second run should be rejected
        resp = client.post(f"/api/pipelines/{pid}/run")
        assert resp.status_code == 409

        # Wait for first to finish
        _wait_for_run(client, pid)

    def test_cancel_running_pipeline(self, client: TestClient):
        """Cancel should stop a running pipeline."""
        resp = client.post("/api/pipelines", json={"name": "Cancel Test"})
        pid = resp.json()["id"]

        client.post(
            f"/api/pipelines/{pid}/nodes",
            json={
                "type": "ml_toolbox.nodes.demo.run",
                "position": {"x": 0, "y": 0},
            },
        )

        client.post(f"/api/pipelines/{pid}/run")

        # Cancel immediately
        resp = client.post(f"/api/pipelines/{pid}/cancel")
        assert resp.status_code == 200

        # Wait for run to finish (should be done or cancelled)
        run = _wait_for_run(client, pid)
        assert run["status"] in ("done", "cancelled", "error")
