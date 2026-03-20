"""Tests for the node library, pipeline CRUD, and edge-case API endpoints."""

import pytest
from fastapi.testclient import TestClient

from ml_toolbox.main import app

client = TestClient(app)


# ── Helpers ──────────────────────────────────────────────────────


def _add_node(pid: str, node_type: str, x: float = 0, y: float = 0) -> dict:
    resp = client.post(
        f"/api/pipelines/{pid}/nodes",
        json={"type": node_type, "position": {"x": x, "y": y}},
    )
    assert resp.status_code == 201
    return resp.json()


# ── Node Library API ──────────────────────────────────────────────


class TestNodesAPI:
    def test_list_nodes_returns_demo_nodes(self):
        resp = client.get("/api/nodes")
        assert resp.status_code == 200
        nodes = resp.json()
        assert len(nodes) >= 3
        types = {n["type"] for n in nodes}
        assert "ml_toolbox.nodes.demo.run" in types
        assert "ml_toolbox.nodes.demo.clean_data" in types
        assert "ml_toolbox.nodes.demo.summarize_data" in types

    def test_list_nodes_entry_shape(self):
        resp = client.get("/api/nodes")
        node = resp.json()[0]
        assert "type" in node
        assert "label" in node
        assert "category" in node
        assert "description" in node
        assert "inputs" in node
        assert "outputs" in node
        assert "params" in node
        assert "default_code" in node

    def test_get_node_by_type(self):
        resp = client.get("/api/nodes/ml_toolbox.nodes.demo.run")
        assert resp.status_code == 200
        node = resp.json()
        assert node["type"] == "ml_toolbox.nodes.demo.run"
        assert node["label"] == "Generate Data"

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
        assert resp.json()["nodes"][0]["id"] == "n1"
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
        assert [n["id"] for n in clone["nodes"]] == ["n1", "n2"]
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


# ── Node Operations API ──────────────────────────────────────────


class TestAddNode:
    def test_add_node_from_registry(self):
        resp = client.post("/api/pipelines", json={"name": "Node Test"})
        pid = resp.json()["id"]

        resp = client.post(
            f"/api/pipelines/{pid}/nodes",
            json={"type": "ml_toolbox.nodes.demo.run", "position": {"x": 10, "y": 20}},
        )
        assert resp.status_code == 201
        node = resp.json()
        assert node["type"] == "ml_toolbox.nodes.demo.run"
        assert node["position"] == {"x": 10.0, "y": 20.0}
        assert "id" in node
        assert "code" in node

        # Verify it appears in the pipeline
        resp = client.get(f"/api/pipelines/{pid}")
        assert len(resp.json()["nodes"]) == 1
        assert resp.json()["nodes"][0]["id"] == node["id"]

        client.delete(f"/api/pipelines/{pid}")

    def test_add_node_unknown_type(self):
        resp = client.post("/api/pipelines", json={"name": "Bad Node"})
        pid = resp.json()["id"]

        resp = client.post(
            f"/api/pipelines/{pid}/nodes",
            json={"type": "nonexistent.node", "position": {"x": 0, "y": 0}},
        )
        assert resp.status_code == 400

        client.delete(f"/api/pipelines/{pid}")


class TestDeleteNode:
    def test_delete_node_removes_connected_edges(self):
        resp = client.post("/api/pipelines", json={"name": "Delete Node"})
        pid = resp.json()["id"]

        # Add two nodes
        r1 = client.post(
            f"/api/pipelines/{pid}/nodes",
            json={"type": "ml_toolbox.nodes.demo.run", "position": {"x": 0, "y": 0}},
        )
        n1 = r1.json()["id"]

        r2 = client.post(
            f"/api/pipelines/{pid}/nodes",
            json={
                "type": "ml_toolbox.nodes.demo.clean_data",
                "position": {"x": 100, "y": 0},
            },
        )
        n2 = r2.json()["id"]

        # Connect them
        resp = client.post(
            f"/api/pipelines/{pid}/edges",
            json={
                "source": n1,
                "source_port": "df",
                "target": n2,
                "target_port": "df",
            },
        )
        assert resp.status_code == 201

        # Delete source node
        resp = client.delete(f"/api/pipelines/{pid}/nodes/{n1}")
        assert resp.status_code == 204

        # Verify node and edge gone
        pipeline = client.get(f"/api/pipelines/{pid}").json()
        assert len(pipeline["nodes"]) == 1
        assert len(pipeline["edges"]) == 0

        client.delete(f"/api/pipelines/{pid}")

    def test_delete_node_not_found(self):
        resp = client.post("/api/pipelines", json={"name": "No Node"})
        pid = resp.json()["id"]

        resp = client.delete(f"/api/pipelines/{pid}/nodes/nonexistent")
        assert resp.status_code == 404

        client.delete(f"/api/pipelines/{pid}")


class TestPutPreservesNodeFields:
    """Simulate real frontend flow: add node → auto-save PUT with partial fields → verify code survives."""

    def test_put_with_partial_node_preserves_code(self):
        """Frontend auto-save only sends id/type/position/params — code must not be lost."""
        resp = client.post("/api/pipelines", json={"name": "Roundtrip Test"})
        pid = resp.json()["id"]

        # Add node — backend returns full node including code
        resp = client.post(
            f"/api/pipelines/{pid}/nodes",
            json={"type": "ml_toolbox.nodes.demo.run", "position": {"x": 0, "y": 0}},
        )
        full_node = resp.json()
        assert full_node["code"]  # code should be non-empty
        assert full_node["inputs"] is not None
        assert full_node["outputs"]

        # Simulate frontend auto-save: PUT with only the fields React Flow tracks
        client.put(
            f"/api/pipelines/{pid}",
            json={
                "id": pid,
                "name": "Roundtrip Test",
                "nodes": [
                    {
                        "id": full_node["id"],
                        "type": full_node["type"],
                        "position": {"x": 100, "y": 50},
                        "params": {"rows": 200},
                    }
                ],
                "edges": [],
            },
        )

        # Read back — code, inputs, outputs must still be present
        pipeline = client.get(f"/api/pipelines/{pid}").json()
        saved_node = pipeline["nodes"][0]
        assert saved_node["code"], "Node code was lost after PUT!"
        assert saved_node["inputs"] is not None, "Node inputs were lost after PUT!"
        assert saved_node["outputs"], "Node outputs were lost after PUT!"

        client.delete(f"/api/pipelines/{pid}")


class TestUpdateNode:
    def test_update_node_params_code_position(self):
        resp = client.post("/api/pipelines", json={"name": "Update Node"})
        pid = resp.json()["id"]

        r = client.post(
            f"/api/pipelines/{pid}/nodes",
            json={"type": "ml_toolbox.nodes.demo.run", "position": {"x": 0, "y": 0}},
        )
        node_id = r.json()["id"]

        # Update params
        resp = client.patch(
            f"/api/pipelines/{pid}/nodes/{node_id}",
            json={"params": {"rows": 500}},
        )
        assert resp.status_code == 200
        # Backend merges values into the ParamDefinition array
        params = resp.json()["params"]
        assert isinstance(params, list)
        rows_param = next(p for p in params if p["name"] == "rows")
        assert rows_param["default"] == 500

        # Update code
        resp = client.patch(
            f"/api/pipelines/{pid}/nodes/{node_id}",
            json={"code": "print('hello')"},
        )
        assert resp.status_code == 200
        assert resp.json()["code"] == "print('hello')"

        # Update position
        resp = client.patch(
            f"/api/pipelines/{pid}/nodes/{node_id}",
            json={"position": {"x": 99, "y": 88}},
        )
        assert resp.status_code == 200
        assert resp.json()["position"] == {"x": 99.0, "y": 88.0}

        # Verify persisted
        pipeline = client.get(f"/api/pipelines/{pid}").json()
        node = pipeline["nodes"][0]
        assert isinstance(node["params"], list)
        assert next(p for p in node["params"] if p["name"] == "rows")["default"] == 500
        assert node["code"] == "print('hello')"
        assert node["position"] == {"x": 99.0, "y": 88.0}

        client.delete(f"/api/pipelines/{pid}")

    def test_update_node_not_found(self):
        resp = client.post("/api/pipelines", json={"name": "No Node"})
        pid = resp.json()["id"]

        resp = client.patch(
            f"/api/pipelines/{pid}/nodes/nonexistent",
            json={"code": "x"},
        )
        assert resp.status_code == 404

        client.delete(f"/api/pipelines/{pid}")


# ── Edge Operations API ──────────────────────────────────────────


class TestAddEdge:
    def _make_two_node_pipeline(self):
        """Helper: create pipeline with run → clean_data nodes, return (pid, n1, n2)."""
        resp = client.post("/api/pipelines", json={"name": "Edge Test"})
        pid = resp.json()["id"]

        r1 = client.post(
            f"/api/pipelines/{pid}/nodes",
            json={"type": "ml_toolbox.nodes.demo.run", "position": {"x": 0, "y": 0}},
        )
        n1 = r1.json()["id"]

        r2 = client.post(
            f"/api/pipelines/{pid}/nodes",
            json={
                "type": "ml_toolbox.nodes.demo.clean_data",
                "position": {"x": 100, "y": 0},
            },
        )
        n2 = r2.json()["id"]
        return pid, n1, n2

    def test_add_valid_edge(self):
        pid, n1, n2 = self._make_two_node_pipeline()

        resp = client.post(
            f"/api/pipelines/{pid}/edges",
            json={
                "source": n1,
                "source_port": "df",
                "target": n2,
                "target_port": "df",
            },
        )
        assert resp.status_code == 201
        edge = resp.json()
        assert edge["source"] == n1
        assert edge["target"] == n2
        assert edge["condition"] is None
        assert "id" in edge

        client.delete(f"/api/pipelines/{pid}")

    def test_add_edge_type_mismatch(self):
        """run outputs TABLE(df), summarize_data outputs METRICS(summary) — mismatch."""
        resp = client.post("/api/pipelines", json={"name": "Mismatch"})
        pid = resp.json()["id"]

        # run: outputs TABLE(df)
        r1 = client.post(
            f"/api/pipelines/{pid}/nodes",
            json={"type": "ml_toolbox.nodes.demo.run", "position": {"x": 0, "y": 0}},
        )
        n1 = r1.json()["id"]

        # summarize_data: outputs METRICS(summary), inputs TABLE(df)
        r2 = client.post(
            f"/api/pipelines/{pid}/nodes",
            json={
                "type": "ml_toolbox.nodes.demo.summarize_data",
                "position": {"x": 100, "y": 0},
            },
        )
        n2 = r2.json()["id"]

        # Try to connect summary(METRICS) output → df(TABLE) input — type mismatch
        resp = client.post(
            f"/api/pipelines/{pid}/edges",
            json={
                "source": n2,
                "source_port": "summary",
                "target": r2.json()["id"],
                "target_port": "df",
            },
        )
        assert resp.status_code == 400
        assert "type mismatch" in resp.json()["detail"].lower()

        client.delete(f"/api/pipelines/{pid}")

    def test_add_edge_creates_cycle(self):
        """A→B→C, then C→A should fail."""
        resp = client.post("/api/pipelines", json={"name": "Cycle"})
        pid = resp.json()["id"]

        # Create A (run) → B (clean) → C (clean) chain
        ra = client.post(
            f"/api/pipelines/{pid}/nodes",
            json={"type": "ml_toolbox.nodes.demo.run", "position": {"x": 0, "y": 0}},
        )
        a = ra.json()["id"]

        rb = client.post(
            f"/api/pipelines/{pid}/nodes",
            json={
                "type": "ml_toolbox.nodes.demo.clean_data",
                "position": {"x": 100, "y": 0},
            },
        )
        b = rb.json()["id"]

        rc = client.post(
            f"/api/pipelines/{pid}/nodes",
            json={
                "type": "ml_toolbox.nodes.demo.clean_data",
                "position": {"x": 200, "y": 0},
            },
        )
        c = rc.json()["id"]

        # A→B
        resp = client.post(
            f"/api/pipelines/{pid}/edges",
            json={"source": a, "source_port": "df", "target": b, "target_port": "df"},
        )
        assert resp.status_code == 201

        # B→C
        resp = client.post(
            f"/api/pipelines/{pid}/edges",
            json={"source": b, "source_port": "df", "target": c, "target_port": "df"},
        )
        assert resp.status_code == 201

        # C→A would create cycle (but C outputs TABLE, A has no inputs — so
        # we need a different setup). Instead, try C→B which creates B→C→B.
        # C outputs TABLE(df), B accepts TABLE(df).
        resp = client.post(
            f"/api/pipelines/{pid}/edges",
            json={"source": c, "source_port": "df", "target": b, "target_port": "df"},
        )
        assert resp.status_code == 400
        assert "cycle" in resp.json()["detail"].lower()

        client.delete(f"/api/pipelines/{pid}")

    def test_add_edge_nonexistent_node(self):
        resp = client.post("/api/pipelines", json={"name": "Bad Edge"})
        pid = resp.json()["id"]

        resp = client.post(
            f"/api/pipelines/{pid}/edges",
            json={
                "source": "no_such_node",
                "source_port": "df",
                "target": "also_missing",
                "target_port": "df",
            },
        )
        assert resp.status_code == 400

        client.delete(f"/api/pipelines/{pid}")

    def test_add_edge_nonexistent_port(self):
        pid, n1, n2 = self._make_two_node_pipeline()

        resp = client.post(
            f"/api/pipelines/{pid}/edges",
            json={
                "source": n1,
                "source_port": "no_such_port",
                "target": n2,
                "target_port": "df",
            },
        )
        assert resp.status_code == 400

        client.delete(f"/api/pipelines/{pid}")


class TestDeleteEdge:
    def test_delete_edge(self):
        resp = client.post("/api/pipelines", json={"name": "Del Edge"})
        pid = resp.json()["id"]

        r1 = client.post(
            f"/api/pipelines/{pid}/nodes",
            json={"type": "ml_toolbox.nodes.demo.run", "position": {"x": 0, "y": 0}},
        )
        n1 = r1.json()["id"]

        r2 = client.post(
            f"/api/pipelines/{pid}/nodes",
            json={
                "type": "ml_toolbox.nodes.demo.clean_data",
                "position": {"x": 100, "y": 0},
            },
        )
        n2 = r2.json()["id"]

        edge_resp = client.post(
            f"/api/pipelines/{pid}/edges",
            json={"source": n1, "source_port": "df", "target": n2, "target_port": "df"},
        )
        edge_id = edge_resp.json()["id"]

        resp = client.delete(f"/api/pipelines/{pid}/edges/{edge_id}")
        assert resp.status_code == 204

        # Verify removed
        pipeline = client.get(f"/api/pipelines/{pid}").json()
        assert len(pipeline["edges"]) == 0

        client.delete(f"/api/pipelines/{pid}")

    def test_delete_edge_not_found(self):
        resp = client.post("/api/pipelines", json={"name": "No Edge"})
        pid = resp.json()["id"]

        resp = client.delete(f"/api/pipelines/{pid}/edges/nonexistent")
        assert resp.status_code == 404

        client.delete(f"/api/pipelines/{pid}")


class TestUpdateEdge:
    def test_update_edge_condition(self):
        resp = client.post("/api/pipelines", json={"name": "Cond Edge"})
        pid = resp.json()["id"]

        r1 = client.post(
            f"/api/pipelines/{pid}/nodes",
            json={"type": "ml_toolbox.nodes.demo.run", "position": {"x": 0, "y": 0}},
        )
        n1 = r1.json()["id"]

        r2 = client.post(
            f"/api/pipelines/{pid}/nodes",
            json={
                "type": "ml_toolbox.nodes.demo.clean_data",
                "position": {"x": 100, "y": 0},
            },
        )
        n2 = r2.json()["id"]

        edge_resp = client.post(
            f"/api/pipelines/{pid}/edges",
            json={"source": n1, "source_port": "df", "target": n2, "target_port": "df"},
        )
        edge_id = edge_resp.json()["id"]

        # Set condition
        resp = client.patch(
            f"/api/pipelines/{pid}/edges/{edge_id}",
            json={"condition": "len(df) > 0"},
        )
        assert resp.status_code == 200
        assert resp.json()["condition"] == "len(df) > 0"

        # Clear condition
        resp = client.patch(
            f"/api/pipelines/{pid}/edges/{edge_id}",
            json={"condition": None},
        )
        assert resp.status_code == 200
        assert resp.json()["condition"] is None

        client.delete(f"/api/pipelines/{pid}")

    def test_update_edge_not_found(self):
        resp = client.post("/api/pipelines", json={"name": "No Edge"})
        pid = resp.json()["id"]

        resp = client.patch(
            f"/api/pipelines/{pid}/edges/nonexistent",
            json={"condition": "x"},
        )
        assert resp.status_code == 404

        client.delete(f"/api/pipelines/{pid}")


# ── Cycle Detection Unit Test ────────────────────────────────────


class TestCycleDetection:
    def test_would_create_cycle_self_loop(self):
        from ml_toolbox.routers.pipelines import would_create_cycle

        data = {"edges": []}
        assert would_create_cycle(data, "a", "a") is True

    def test_would_create_cycle_linear(self):
        from ml_toolbox.routers.pipelines import would_create_cycle

        data = {"edges": [{"source": "a", "target": "b"}]}
        # b→a would create a→b→a cycle
        assert would_create_cycle(data, "b", "a") is True
        # a→c would not create a cycle
        assert would_create_cycle(data, "a", "c") is False

    def test_would_create_cycle_diamond(self):
        from ml_toolbox.routers.pipelines import would_create_cycle

        # a→b, a→c, b→d, c→d — no cycle
        data = {
            "edges": [
                {"source": "a", "target": "b"},
                {"source": "a", "target": "c"},
                {"source": "b", "target": "d"},
                {"source": "c", "target": "d"},
            ]
        }
        # d→a would create cycle
        assert would_create_cycle(data, "d", "a") is True
        # d→b would create cycle
        assert would_create_cycle(data, "d", "b") is True
        # b→c would not create cycle
        assert would_create_cycle(data, "b", "c") is False


# ── Pipeline 404s ────────────────────────────────────────────────


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

    def test_run_missing(self):
        resp = client.post("/api/pipelines/nonexistent/run")
        assert resp.status_code == 404

    def test_status_missing(self):
        resp = client.get("/api/pipelines/nonexistent/status")
        assert resp.status_code == 404

    def test_run_from_nonexistent_node(self):
        resp = client.post("/api/pipelines", json={"name": "Run From Bad Node"})
        pid = resp.json()["id"]
        resp = client.post(f"/api/pipelines/{pid}/run/nonexistent-node")
        assert resp.status_code == 404
        client.delete(f"/api/pipelines/{pid}")


# ── WebSocket Integration ────────────────────────────────────────


class TestWebSocket:
    def test_websocket_connects(self):
        """Verify WebSocket endpoint is functional."""
        with client.websocket_connect("/ws/pipelines/e2e-test") as ws:
            pass

    def test_websocket_receives_broadcast(self):
        """Verify WebSocket receives broadcast messages."""
        from ml_toolbox.routers.ws import broadcast_sync

        with client.websocket_connect("/ws/pipelines/e2e-ws-test") as ws:
            broadcast_sync("e2e-ws-test", {"status": "running", "node_id": "n1"})
            msg = ws.receive_json()
            assert msg["status"] == "running"
            assert msg["node_id"] == "n1"


# ── Concurrent Run Prevention ────────────────────────────────────


class TestConcurrentRunAPI:
    def test_concurrent_run_returns_409(self):
        """Starting a second run while one is active returns 409."""
        from ml_toolbox.services.executor import (
            PipelineExecutor,
            remove_active_executor,
            try_set_active_executor,
        )

        resp = client.post("/api/pipelines", json={"name": "Concurrency Test"})
        pid = resp.json()["id"]

        executor = PipelineExecutor()
        try_set_active_executor(pid, executor)

        try:
            resp = client.post(f"/api/pipelines/{pid}/run")
            assert resp.status_code == 409
            assert "already running" in resp.json()["detail"].lower()
        finally:
            remove_active_executor(pid)

        client.delete(f"/api/pipelines/{pid}")
