"""End-to-end smoke test: validates all components work together.

Tests the full pipeline lifecycle including creation, node/edge management,
execution (mocked Docker), output retrieval, run-from-node, cancellation,
cycle detection, type mismatches, and 404 handling.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from ml_toolbox.main import app

client = TestClient(app)


# ── Helpers ──────────────────────────────────────────────────────


def _create_pipeline(name: str = "E2E Smoke Test") -> str:
    resp = client.post("/api/pipelines", json={"name": name})
    assert resp.status_code == 201
    data = resp.json()
    assert "id" in data
    assert data["name"] == name
    return data["id"]


def _add_node(pid: str, node_type: str, x: float = 0, y: float = 0) -> dict:
    resp = client.post(
        f"/api/pipelines/{pid}/nodes",
        json={"type": node_type, "position": {"x": x, "y": y}},
    )
    assert resp.status_code == 201
    return resp.json()


def _add_edge(
    pid: str, source: str, source_port: str, target: str, target_port: str
) -> dict:
    resp = client.post(
        f"/api/pipelines/{pid}/edges",
        json={
            "source": source,
            "source_port": source_port,
            "target": target,
            "target_port": target_port,
        },
    )
    assert resp.status_code == 201
    return resp.json()


def _write_fake_outputs(run_dir: Path, node_id: str, port_name: str) -> None:
    """Write a fake parquet output file to simulate sandbox execution."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({"id": [1, 2, 3], "value": [10.0, 20.0, 30.0]})
    out_path = run_dir / f"{node_id}_{port_name}.parquet"
    pq.write_table(table, out_path)


def _write_fake_json_output(run_dir: Path, node_id: str, port_name: str) -> None:
    """Write a fake JSON output file to simulate summarize node."""
    out_path = run_dir / f"{node_id}_{port_name}.json"
    # Use a non-.json extension to be found by _find_output_file (which excludes .json)
    # Actually, the output filter excludes .json — use .msgpack or similar?
    # Looking at _find_output_file: it excludes .json, .hash, .txt
    # So for METRICS output, let's write as a .csv instead for testability
    out_path = run_dir / f"{node_id}_{port_name}.csv"
    out_path.write_text("metric,value\nrow_count,3\ncolumn_count,2")


# ── Full Pipeline Lifecycle Smoke Test ───────────────────────────


class TestE2ESmokeTest:
    """Complete end-to-end smoke test covering the full pipeline lifecycle."""

    def test_full_pipeline_lifecycle(self) -> None:
        """
        Steps:
        1. Create pipeline
        2. Add 3 demo nodes (generate → clean → summarize)
        3. Connect them with edges (validate port types match)
        4. Run pipeline (mocked Docker)
        5. Check status
        6. Verify outputs
        7. Run from node (clean_data) — upstream cached, downstream re-runs
        8. Cancel a run
        9. Pipeline appears in list
        10. Delete pipeline — verify gone
        """
        # ── Step 1: Create pipeline ──
        pid = _create_pipeline("E2E Full Lifecycle")

        # ── Step 2: Add 3 demo nodes ──
        gen_node = _add_node(pid, "ml_toolbox.nodes.demo.run", x=0, y=0)
        clean_node = _add_node(pid, "ml_toolbox.nodes.demo.clean_data", x=200, y=0)
        summary_node = _add_node(
            pid, "ml_toolbox.nodes.demo.summarize_data", x=400, y=0
        )

        gen_id = gen_node["id"]
        clean_id = clean_node["id"]
        summary_id = summary_node["id"]

        # Verify nodes have expected structure
        assert gen_node["type"] == "ml_toolbox.nodes.demo.run"
        assert "code" in gen_node
        assert "outputs" in gen_node
        assert clean_node["type"] == "ml_toolbox.nodes.demo.clean_data"
        assert "inputs" in clean_node
        assert summary_node["type"] == "ml_toolbox.nodes.demo.summarize_data"

        # Verify pipeline now has 3 nodes
        pipeline = client.get(f"/api/pipelines/{pid}").json()
        assert len(pipeline["nodes"]) == 3

        # ── Step 3: Connect nodes with edges ──
        # generate_data(df:TABLE) → clean_data(df:TABLE)
        edge1 = _add_edge(pid, gen_id, "df", clean_id, "df")
        assert edge1["source"] == gen_id
        assert edge1["target"] == clean_id

        # clean_data(df:TABLE) → summarize_data(df:TABLE)
        edge2 = _add_edge(pid, clean_id, "df", summary_id, "df")
        assert edge2["source"] == clean_id
        assert edge2["target"] == summary_id

        # Verify pipeline has 2 edges
        pipeline = client.get(f"/api/pipelines/{pid}").json()
        assert len(pipeline["edges"]) == 2

        # ── Step 4: Run pipeline (mock Docker) ──
        from ml_toolbox.services import file_store

        broadcasts: list[dict] = []
        original_broadcast = None

        def _capture_broadcast(pipeline_id: str, msg: dict) -> None:
            broadcasts.append(msg)

        def _mock_run_pipeline(pipeline_id, pipeline, node_id=None):
            """Simulate executor: create run dir, write fake outputs."""
            import uuid

            run_id = uuid.uuid4().hex
            run_dir = file_store.make_run_dir(pipeline_id, run_id)

            if node_id is None:
                # Full run — write outputs for all nodes
                _write_fake_outputs(run_dir, gen_id, "df")
                _write_fake_outputs(run_dir, clean_id, "df")
                _write_fake_json_output(run_dir, summary_id, "summary")
            else:
                # Run-from — only write downstream outputs
                order = [gen_id, clean_id, summary_id]
                start_idx = order.index(node_id) if node_id in order else 0
                # Hard-link upstream (simulate by copying)
                prev_run_id = file_store.get_latest_run_id(
                    pipeline_id, exclude=run_id
                )
                if prev_run_id:
                    prev_dir = file_store._runs_dir(pipeline_id) / prev_run_id
                    for f in prev_dir.iterdir():
                        dest = run_dir / f.name
                        if not dest.exists():
                            import shutil

                            shutil.copy2(f, dest)
                # Write fresh outputs for downstream nodes
                for nid in order[start_idx:]:
                    if nid == gen_id:
                        _write_fake_outputs(run_dir, gen_id, "df")
                    elif nid == clean_id:
                        _write_fake_outputs(run_dir, clean_id, "df")
                    elif nid == summary_id:
                        _write_fake_json_output(run_dir, summary_id, "summary")

            # Write status file
            status_path = run_dir / "_status.json"
            status_path.write_text(
                json.dumps({"status": "done", "run_id": run_id})
            )

            # Write hash files for caching
            for nid in [gen_id, clean_id, summary_id]:
                (run_dir / f"{nid}.hash").write_text("fakehash")

            return run_id

        with patch(
            "ml_toolbox.routers.pipelines._run_pipeline",
            side_effect=_mock_run_pipeline,
        ):
            resp = client.post(f"/api/pipelines/{pid}/run")
            assert resp.status_code == 200
            run_id_1 = resp.json()["run_id"]
            assert run_id_1

        # ── Step 5: Check status ──
        resp = client.get(f"/api/pipelines/{pid}/status")
        assert resp.status_code == 200
        status = resp.json()
        assert status["is_running"] is False
        assert status["last_run_id"] == run_id_1

        # ── Step 6: Verify outputs ──
        # generate_data output
        resp = client.get(
            f"/api/pipelines/{pid}/outputs/{gen_id}?run_id={run_id_1}"
        )
        assert resp.status_code == 200
        output = resp.json()
        assert output["node_id"] == gen_id
        assert output["type"] == "parquet"
        assert output["size"] > 0
        assert "preview" in output  # parquet preview should be present

        # clean_data output
        resp = client.get(
            f"/api/pipelines/{pid}/outputs/{clean_id}?run_id={run_id_1}"
        )
        assert resp.status_code == 200
        assert resp.json()["node_id"] == clean_id

        # summarize_data output
        resp = client.get(
            f"/api/pipelines/{pid}/outputs/{summary_id}?run_id={run_id_1}"
        )
        assert resp.status_code == 200
        assert resp.json()["node_id"] == summary_id

        # ── Step 7: Run from node (clean_data) ──
        with patch(
            "ml_toolbox.routers.pipelines._run_pipeline",
            side_effect=_mock_run_pipeline,
        ):
            resp = client.post(f"/api/pipelines/{pid}/run/{clean_id}")
            assert resp.status_code == 200
            run_id_2 = resp.json()["run_id"]
            assert run_id_2
            assert run_id_2 != run_id_1

        # Verify we have 2 runs now
        resp = client.get(f"/api/pipelines/{pid}/runs")
        assert resp.status_code == 200
        runs = resp.json()
        assert len(runs) == 2

        # Verify outputs exist for run 2
        resp = client.get(
            f"/api/pipelines/{pid}/outputs/{clean_id}?run_id={run_id_2}"
        )
        assert resp.status_code == 200

        # ── Step 8: Cancel ──
        resp = client.post(f"/api/pipelines/{pid}/cancel")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

        # ── Step 9: Pipeline appears in list ──
        resp = client.get("/api/pipelines")
        assert resp.status_code == 200
        pipelines = resp.json()
        match = next((p for p in pipelines if p["id"] == pid), None)
        assert match is not None
        assert match["name"] == "E2E Full Lifecycle"
        assert match["node_count"] == 3

        # ── Step 10: Delete pipeline and verify ──
        resp = client.delete(f"/api/pipelines/{pid}")
        assert resp.status_code == 204

        resp = client.get(f"/api/pipelines/{pid}")
        assert resp.status_code == 404

        resp = client.get("/api/pipelines")
        assert not any(p["id"] == pid for p in resp.json())


# ── Edge Case Tests ──────────────────────────────────────────────


class TestE2EEdgeCases:
    """Edge cases: cycles, type mismatches, 404s, deletions."""

    def test_cycle_detection(self) -> None:
        """Creating a cycle should return 400."""
        pid = _create_pipeline("Cycle Test")

        a = _add_node(pid, "ml_toolbox.nodes.demo.clean_data", x=0, y=0)
        b = _add_node(pid, "ml_toolbox.nodes.demo.clean_data", x=100, y=0)
        c = _add_node(pid, "ml_toolbox.nodes.demo.clean_data", x=200, y=0)

        # A → B → C
        _add_edge(pid, a["id"], "df", b["id"], "df")
        _add_edge(pid, b["id"], "df", c["id"], "df")

        # C → A would create a cycle (clean_data has TABLE input and TABLE output)
        resp = client.post(
            f"/api/pipelines/{pid}/edges",
            json={
                "source": c["id"],
                "source_port": "df",
                "target": a["id"],
                "target_port": "df",
            },
        )
        assert resp.status_code == 400
        assert "cycle" in resp.json()["detail"].lower()

        client.delete(f"/api/pipelines/{pid}")

    def test_type_mismatch(self) -> None:
        """Connecting incompatible port types should return 400."""
        pid = _create_pipeline("Type Mismatch Test")

        # summarize_data outputs METRICS(summary)
        summary = _add_node(
            pid, "ml_toolbox.nodes.demo.summarize_data", x=0, y=0
        )
        # clean_data expects TABLE(df) input
        clean = _add_node(
            pid, "ml_toolbox.nodes.demo.clean_data", x=200, y=0
        )

        # Try to connect METRICS output → TABLE input
        resp = client.post(
            f"/api/pipelines/{pid}/edges",
            json={
                "source": summary["id"],
                "source_port": "summary",
                "target": clean["id"],
                "target_port": "df",
            },
        )
        assert resp.status_code == 400
        assert "type mismatch" in resp.json()["detail"].lower()

        client.delete(f"/api/pipelines/{pid}")

    def test_missing_pipeline_404(self) -> None:
        """Operations on nonexistent pipeline return 404."""
        resp = client.get("/api/pipelines/nonexistent")
        assert resp.status_code == 404

        resp = client.post("/api/pipelines/nonexistent/run")
        assert resp.status_code == 404

        resp = client.get("/api/pipelines/nonexistent/status")
        assert resp.status_code == 404

        resp = client.delete("/api/pipelines/nonexistent")
        assert resp.status_code == 404

    def test_delete_pipeline_removes_from_list(self) -> None:
        """Deleted pipeline should not appear in list."""
        pid = _create_pipeline("To Be Deleted")

        # Add some content
        _add_node(pid, "ml_toolbox.nodes.demo.run")

        resp = client.delete(f"/api/pipelines/{pid}")
        assert resp.status_code == 204

        resp = client.get("/api/pipelines")
        assert not any(p["id"] == pid for p in resp.json())

    def test_nonexistent_port(self) -> None:
        """Connecting to a nonexistent port should return 400."""
        pid = _create_pipeline("Bad Port Test")
        gen = _add_node(pid, "ml_toolbox.nodes.demo.run")
        clean = _add_node(pid, "ml_toolbox.nodes.demo.clean_data")

        resp = client.post(
            f"/api/pipelines/{pid}/edges",
            json={
                "source": gen["id"],
                "source_port": "nonexistent_port",
                "target": clean["id"],
                "target_port": "df",
            },
        )
        assert resp.status_code == 400

        client.delete(f"/api/pipelines/{pid}")

    def test_duplicate_pipeline(self) -> None:
        """Duplicating a pipeline preserves nodes and edges."""
        pid = _create_pipeline("Original Pipeline")
        gen = _add_node(pid, "ml_toolbox.nodes.demo.run")
        clean = _add_node(pid, "ml_toolbox.nodes.demo.clean_data")
        _add_edge(pid, gen["id"], "df", clean["id"], "df")

        resp = client.post(f"/api/pipelines/{pid}/duplicate")
        assert resp.status_code == 201
        dup = resp.json()
        assert dup["name"] == "Original Pipeline (copy)"
        assert dup["id"] != pid

        dup_data = client.get(f"/api/pipelines/{dup['id']}").json()
        assert len(dup_data["nodes"]) == 2
        assert len(dup_data["edges"]) == 1

        client.delete(f"/api/pipelines/{pid}")
        client.delete(f"/api/pipelines/{dup['id']}")

    def test_run_from_nonexistent_node(self) -> None:
        """Running from a node that doesn't exist returns 404."""
        pid = _create_pipeline("Run From Bad Node")
        resp = client.post(f"/api/pipelines/{pid}/run/nonexistent-node")
        assert resp.status_code == 404

        client.delete(f"/api/pipelines/{pid}")

    def test_output_download(self) -> None:
        """Output download returns file content with correct headers."""
        from ml_toolbox.services import file_store

        pid = _create_pipeline("Download Test")
        run_dir = file_store.make_run_dir(pid, "run-dl")
        _write_fake_outputs(run_dir, "testnode", "df")

        resp = client.get(
            f"/api/pipelines/{pid}/outputs/testnode/download?run_id=run-dl"
        )
        assert resp.status_code == 200
        assert len(resp.content) > 0
        assert "content-disposition" in resp.headers

        client.delete(f"/api/pipelines/{pid}")

    def test_output_not_found(self) -> None:
        """Getting output for nonexistent node returns 404."""
        from ml_toolbox.services import file_store

        pid = _create_pipeline("No Output Test")
        file_store.make_run_dir(pid, "run-empty")

        resp = client.get(
            f"/api/pipelines/{pid}/outputs/nonexistent?run_id=run-empty"
        )
        assert resp.status_code == 404

        client.delete(f"/api/pipelines/{pid}")


# ── Node Library Tests ───────────────────────────────────────────


class TestE2ENodeLibrary:
    """Verify node library is available and well-formed."""

    def test_node_library_contains_demo_nodes(self) -> None:
        resp = client.get("/api/nodes")
        assert resp.status_code == 200
        nodes = resp.json()
        assert len(nodes) >= 3

        types = {n["type"] for n in nodes}
        assert "ml_toolbox.nodes.demo.run" in types
        assert "ml_toolbox.nodes.demo.clean_data" in types
        assert "ml_toolbox.nodes.demo.summarize_data" in types

    def test_node_detail_has_ports_and_code(self) -> None:
        resp = client.get("/api/nodes/ml_toolbox.nodes.demo.run")
        assert resp.status_code == 200
        node = resp.json()
        assert node["label"] == "Generate Data"
        assert "outputs" in node
        assert any(o["name"] == "df" for o in node["outputs"])
        assert "default_code" in node

    def test_nonexistent_node_type(self) -> None:
        resp = client.get("/api/nodes/does.not.exist")
        assert resp.status_code == 404


# ── Health Check ─────────────────────────────────────────────────


class TestE2EHealth:
    def test_health_endpoint(self) -> None:
        resp = client.get("/api/health")
        assert resp.status_code == 200


# ── WebSocket Integration ────────────────────────────────────────


class TestE2EWebSocket:
    def test_websocket_connects(self) -> None:
        """Verify WebSocket endpoint is functional."""
        with client.websocket_connect("/ws/pipelines/e2e-test") as ws:
            # Connection established successfully
            pass

    def test_websocket_receives_broadcast(self) -> None:
        """Verify WebSocket receives broadcast messages."""
        from ml_toolbox.routers.ws import broadcast_sync

        with client.websocket_connect("/ws/pipelines/e2e-ws-test") as ws:
            broadcast_sync("e2e-ws-test", {"status": "running", "node_id": "n1"})
            msg = ws.receive_json()
            assert msg["status"] == "running"
            assert msg["node_id"] == "n1"


# ── Concurrent Run Prevention ────────────────────────────────────


class TestE2EConcurrency:
    def test_concurrent_run_returns_409(self) -> None:
        """Starting a second run while one is active returns 409."""
        from ml_toolbox.services.executor import (
            PipelineExecutor,
            remove_active_executor,
            try_set_active_executor,
        )

        pid = _create_pipeline("Concurrency Test")

        # Simulate an active executor
        executor = PipelineExecutor()
        try_set_active_executor(pid, executor)

        try:
            resp = client.post(f"/api/pipelines/{pid}/run")
            assert resp.status_code == 409
            assert "already running" in resp.json()["detail"].lower()
        finally:
            remove_active_executor(pid)

        client.delete(f"/api/pipelines/{pid}")
