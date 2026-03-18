"""Tests for the node library and pipeline CRUD API endpoints."""

import pytest
from fastapi.testclient import TestClient

from ml_toolbox.main import app

client = TestClient(app)


# ── Node Library API ──────────────────────────────────────────────


class TestNodesAPI:
    def test_list_nodes_returns_demo_nodes(self):
        resp = client.get("/api/nodes")
        assert resp.status_code == 200
        nodes = resp.json()
        assert len(nodes) >= 3
        ids = {n["id"] for n in nodes}
        assert "ml_toolbox.nodes.demo.run" in ids
        assert "ml_toolbox.nodes.demo.clean_data" in ids
        assert "ml_toolbox.nodes.demo.summarize_data" in ids

    def test_list_nodes_entry_shape(self):
        resp = client.get("/api/nodes")
        node = resp.json()[0]
        assert "id" in node
        assert "name" in node
        assert "inputs" in node
        assert "outputs" in node
        assert "params" in node
        assert "code" in node

    def test_get_node_by_type(self):
        resp = client.get("/api/nodes/ml_toolbox.nodes.demo.run")
        assert resp.status_code == 200
        node = resp.json()
        assert node["id"] == "ml_toolbox.nodes.demo.run"
        assert node["name"] == "run"

    def test_get_node_not_found(self):
        resp = client.get("/api/nodes/nonexistent.node")
        assert resp.status_code == 404


# ── Pipeline CRUD API ─────────────────────────────────────────────


class TestPipelineLifecycle:
    """Test create → get → update → list → delete lifecycle."""

    def test_full_lifecycle(self):
        # Create
        resp = client.post("/api/pipelines", json={"name": "My Pipeline"})
        assert resp.status_code == 201
        created = resp.json()
        pid = created["id"]
        assert created["name"] == "My Pipeline"
        assert len(pid) == 32  # uuid hex

        # Get
        resp = client.get(f"/api/pipelines/{pid}")
        assert resp.status_code == 200
        pipeline = resp.json()
        assert pipeline["id"] == pid
        assert pipeline["name"] == "My Pipeline"
        assert pipeline["settings"] == {"keep_outputs": True}
        assert pipeline["nodes"] == []
        assert pipeline["edges"] == []

        # Update
        updated_data = {
            "id": pid,
            "name": "My Pipeline",
            "settings": {"keep_outputs": False},
            "nodes": [{"id": "n1"}],
            "edges": [],
        }
        resp = client.put(f"/api/pipelines/{pid}", json=updated_data)
        assert resp.status_code == 200

        # Verify update
        resp = client.get(f"/api/pipelines/{pid}")
        assert resp.json()["nodes"] == [{"id": "n1"}]
        assert resp.json()["settings"]["keep_outputs"] is False

        # List
        resp = client.get("/api/pipelines")
        assert resp.status_code == 200
        items = resp.json()
        assert any(p["id"] == pid for p in items)
        match = next(p for p in items if p["id"] == pid)
        assert match["node_count"] == 1

        # Delete
        resp = client.delete(f"/api/pipelines/{pid}")
        assert resp.status_code == 204

        # Verify gone
        resp = client.get(f"/api/pipelines/{pid}")
        assert resp.status_code == 404


class TestPipelineDuplicate:
    def test_duplicate_creates_independent_copy(self):
        # Create original
        resp = client.post("/api/pipelines", json={"name": "Original"})
        original_id = resp.json()["id"]

        # Add some data
        client.put(
            f"/api/pipelines/{original_id}",
            json={
                "id": original_id,
                "name": "Original",
                "settings": {"keep_outputs": True},
                "nodes": [{"id": "n1"}, {"id": "n2"}],
                "edges": [{"source": "n1", "target": "n2"}],
            },
        )

        # Duplicate
        resp = client.post(f"/api/pipelines/{original_id}/duplicate")
        assert resp.status_code == 201
        dup = resp.json()
        assert dup["name"] == "Original (copy)"
        assert dup["id"] != original_id

        # Verify independent copy
        resp = client.get(f"/api/pipelines/{dup['id']}")
        clone = resp.json()
        assert clone["nodes"] == [{"id": "n1"}, {"id": "n2"}]
        assert clone["edges"] == [{"source": "n1", "target": "n2"}]

        # Modify original, clone unchanged
        client.put(
            f"/api/pipelines/{original_id}",
            json={
                "id": original_id,
                "name": "Original",
                "settings": {},
                "nodes": [],
                "edges": [],
            },
        )
        resp = client.get(f"/api/pipelines/{dup['id']}")
        assert len(resp.json()["nodes"]) == 2

        # Cleanup
        client.delete(f"/api/pipelines/{original_id}")
        client.delete(f"/api/pipelines/{dup['id']}")

    def test_duplicate_not_found(self):
        resp = client.post("/api/pipelines/nonexistent/duplicate")
        assert resp.status_code == 404


class TestPipelineSettings:
    def test_patch_settings_partial_merge(self):
        resp = client.post("/api/pipelines", json={"name": "Settings Test"})
        pid = resp.json()["id"]

        # Initial settings should have keep_outputs: true
        resp = client.get(f"/api/pipelines/{pid}")
        assert resp.json()["settings"]["keep_outputs"] is True

        # Patch with new setting
        resp = client.patch(
            f"/api/pipelines/{pid}/settings",
            json={"keep_outputs": False},
        )
        assert resp.status_code == 200
        assert resp.json()["keep_outputs"] is False

        # Verify merged
        resp = client.get(f"/api/pipelines/{pid}")
        assert resp.json()["settings"]["keep_outputs"] is False

        # Cleanup
        client.delete(f"/api/pipelines/{pid}")

    def test_patch_settings_not_found(self):
        resp = client.patch(
            "/api/pipelines/nonexistent/settings",
            json={"keep_outputs": False},
        )
        assert resp.status_code == 404


class TestPipeline404:
    def test_get_missing(self):
        resp = client.get("/api/pipelines/does_not_exist")
        assert resp.status_code == 404

    def test_put_missing(self):
        resp = client.put("/api/pipelines/does_not_exist", json={"id": "x"})
        assert resp.status_code == 404

    def test_delete_missing(self):
        resp = client.delete("/api/pipelines/does_not_exist")
        assert resp.status_code == 404
