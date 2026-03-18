"""Tests for the DAG execution engine."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ml_toolbox.services.executor import CycleError, PipelineExecutor


# ── Helpers ──────────────────────────────────────────────────────


def _make_pipeline(nodes: list[dict], edges: list[dict] | None = None) -> dict:
    """Build a minimal pipeline dict."""
    return {
        "id": "test-pipeline",
        "name": "Test",
        "settings": {"keep_outputs": True},
        "nodes": nodes,
        "edges": edges or [],
    }


def _node(nid: str, code: str = "", params: dict | None = None) -> dict:
    return {
        "id": nid,
        "type": "test.node",
        "position": {"x": 0, "y": 0},
        "params": params or {},
        "code": code,
        "inputs": [{"name": "input", "type": "TABLE"}],
        "outputs": [{"name": "output", "type": "TABLE"}],
    }


def _edge(src: str, tgt: str, condition: str | None = None) -> dict:
    return {
        "id": f"{src}->{tgt}",
        "source": src,
        "source_port": "output",
        "target": tgt,
        "target_port": "input",
        "condition": condition,
    }


# ── Topological Sort ─────────────────────────────────────────────


class TestTopologicalSort:
    def test_linear_chain(self):
        """A → B → C should produce [A, B, C]."""
        pipeline = _make_pipeline(
            [_node("A"), _node("B"), _node("C")],
            [_edge("A", "B"), _edge("B", "C")],
        )
        order = PipelineExecutor._topological_sort(pipeline)
        assert order.index("A") < order.index("B") < order.index("C")

    def test_diamond_dag(self):
        """A → B, A → C, B → D, C → D."""
        pipeline = _make_pipeline(
            [_node("A"), _node("B"), _node("C"), _node("D")],
            [_edge("A", "B"), _edge("A", "C"), _edge("B", "D"), _edge("C", "D")],
        )
        order = PipelineExecutor._topological_sort(pipeline)
        assert order.index("A") < order.index("B")
        assert order.index("A") < order.index("C")
        assert order.index("B") < order.index("D")
        assert order.index("C") < order.index("D")

    def test_single_node(self):
        pipeline = _make_pipeline([_node("A")])
        assert PipelineExecutor._topological_sort(pipeline) == ["A"]

    def test_disconnected_nodes(self):
        """Nodes with no edges should all appear in the result."""
        pipeline = _make_pipeline([_node("A"), _node("B"), _node("C")])
        order = PipelineExecutor._topological_sort(pipeline)
        assert set(order) == {"A", "B", "C"}

    def test_cycle_raises(self):
        """A → B → A should raise CycleError."""
        pipeline = _make_pipeline(
            [_node("A"), _node("B")],
            [_edge("A", "B"), _edge("B", "A")],
        )
        with pytest.raises(CycleError):
            PipelineExecutor._topological_sort(pipeline)

    def test_three_node_cycle_raises(self):
        """A → B → C → A should raise CycleError."""
        pipeline = _make_pipeline(
            [_node("A"), _node("B"), _node("C")],
            [_edge("A", "B"), _edge("B", "C"), _edge("C", "A")],
        )
        with pytest.raises(CycleError):
            PipelineExecutor._topological_sort(pipeline)


# ── Params Hash ──────────────────────────────────────────────────


class TestParamsHash:
    def test_same_params_same_hash(self):
        n = _node("A", code="print(1)", params={"x": 1})
        h1 = PipelineExecutor._params_hash(n)
        h2 = PipelineExecutor._params_hash(n)
        assert h1 == h2

    def test_different_params_different_hash(self):
        n1 = _node("A", code="print(1)", params={"x": 1})
        n2 = _node("A", code="print(1)", params={"x": 2})
        assert PipelineExecutor._params_hash(n1) != PipelineExecutor._params_hash(n2)

    def test_different_code_different_hash(self):
        n1 = _node("A", code="print(1)", params={"x": 1})
        n2 = _node("A", code="print(2)", params={"x": 1})
        assert PipelineExecutor._params_hash(n1) != PipelineExecutor._params_hash(n2)

    def test_hash_is_sha256_hex(self):
        n = _node("A")
        h = PipelineExecutor._params_hash(n)
        assert len(h) == 64  # SHA-256 hex length
        assert all(c in "0123456789abcdef" for c in h)


# ── Caching ──────────────────────────────────────────────────────


class TestCaching:
    def test_is_cached_false_when_no_hash(self, tmp_path: Path):
        n = _node("A", code="x", params={"a": 1})
        assert PipelineExecutor._is_cached("A", n, tmp_path) is False

    def test_is_cached_true_when_hash_matches(self, tmp_path: Path):
        n = _node("A", code="x", params={"a": 1})
        h = PipelineExecutor._params_hash(n)
        (tmp_path / "A.hash").write_text(h)
        assert PipelineExecutor._is_cached("A", n, tmp_path) is True

    def test_is_cached_false_when_hash_differs(self, tmp_path: Path):
        n = _node("A", code="x", params={"a": 1})
        (tmp_path / "A.hash").write_text("stale_hash")
        assert PipelineExecutor._is_cached("A", n, tmp_path) is False


# ── Conditions (opaque on backend, evaluated in sandbox) ─────────


class TestConditions:
    def test_no_conditions(self):
        """Edges without conditions → _has_conditions returns False."""
        pipeline = _make_pipeline(
            [_node("A"), _node("B")],
            [_edge("A", "B")],
        )
        assert PipelineExecutor._has_conditions("B", pipeline) is False

    def test_has_conditions(self):
        """Edges with conditions → _has_conditions returns True."""
        pipeline = _make_pipeline(
            [_node("A"), _node("B")],
            [_edge("A", "B", condition="len('hello') > 0")],
        )
        assert PipelineExecutor._has_conditions("B", pipeline) is True

    def test_gather_conditions(self):
        """_gather_conditions returns condition entries for the sandbox."""
        pipeline = _make_pipeline(
            [_node("A"), _node("B")],
            [_edge("A", "B", condition="result.get('rows', 0) > 10")],
        )
        conditions = PipelineExecutor._gather_conditions("B", pipeline)
        assert len(conditions) == 1
        assert conditions[0]["source_id"] == "A"
        assert conditions[0]["condition"] == "result.get('rows', 0) > 10"

    def test_gather_conditions_skips_empty(self):
        """Edges without conditions are excluded from gathered list."""
        pipeline = _make_pipeline(
            [_node("A"), _node("B"), _node("C")],
            [_edge("A", "B"), _edge("A", "C", condition="True")],
        )
        assert PipelineExecutor._gather_conditions("B", pipeline) == []
        assert len(PipelineExecutor._gather_conditions("C", pipeline)) == 1


# ── Downstream Set ───────────────────────────────────────────────


class TestDownstreamSet:
    def test_linear(self):
        pipeline = _make_pipeline(
            [_node("A"), _node("B"), _node("C")],
            [_edge("A", "B"), _edge("B", "C")],
        )
        assert PipelineExecutor._downstream_set("A", pipeline) == {"B", "C"}
        assert PipelineExecutor._downstream_set("B", pipeline) == {"C"}
        assert PipelineExecutor._downstream_set("C", pipeline) == set()


# ── Executor with Mock Docker ────────────────────────────────────


class TestExecutorWithMockDocker:
    def _mock_docker_client(self):
        """Create a mock Docker client that simulates successful execution."""
        mock_client = MagicMock()
        mock_container = MagicMock()
        mock_container.wait.return_value = {"StatusCode": 0}
        mock_container.logs.return_value = b""
        mock_client.containers.run.return_value = mock_container
        return mock_client

    def test_run_all_creates_run_dir(self, tmp_path: Path, monkeypatch):
        monkeypatch.setattr(
            "ml_toolbox.services.file_store.PROJECTS_DIR", tmp_path / "projects"
        )

        pipeline = _make_pipeline([_node("A", code="def run(i,p): pass")])
        pipeline["id"] = "p1"

        broadcasts: list[dict] = []
        executor = PipelineExecutor(broadcast=lambda pid, msg: broadcasts.append(msg))
        executor._docker = self._mock_docker_client()

        run_id = executor.run_all(pipeline)
        assert run_id  # non-empty string

        # Verify run directory was created
        run_dir = tmp_path / "projects" / "p1" / "runs" / run_id
        assert run_dir.exists()

        # Verify manifest was written
        manifest_files = list(run_dir.glob("*_manifest.json"))
        assert len(manifest_files) == 1

        # Verify broadcast messages
        statuses = [b["status"] for b in broadcasts]
        assert "running" in statuses
        assert "done" in statuses

    def test_run_all_respects_topological_order(self, tmp_path: Path, monkeypatch):
        monkeypatch.setattr(
            "ml_toolbox.services.file_store.PROJECTS_DIR", tmp_path / "projects"
        )

        pipeline = _make_pipeline(
            [_node("A"), _node("B")],
            [_edge("A", "B")],
        )
        pipeline["id"] = "p1"

        execution_order: list[str] = []

        def _fake_execute_node(node_id, pipeline, run_dir):
            execution_order.append(node_id)
            # Write hash file to prevent "done" from cache path
            (run_dir / f"{node_id}.hash").write_text("x")
            return "done"

        executor = PipelineExecutor()
        executor._docker = self._mock_docker_client()
        executor._execute_node = _fake_execute_node  # type: ignore[assignment]

        run_id = executor.run_all(pipeline)
        assert execution_order == ["A", "B"]

    def test_cancel_skips_remaining_nodes(self, tmp_path: Path, monkeypatch):
        monkeypatch.setattr(
            "ml_toolbox.services.file_store.PROJECTS_DIR", tmp_path / "projects"
        )

        pipeline = _make_pipeline(
            [_node("A"), _node("B"), _node("C")],
            [_edge("A", "B"), _edge("B", "C")],
        )
        pipeline["id"] = "p1"

        broadcasts: list[dict] = []
        executor = PipelineExecutor(broadcast=lambda pid, msg: broadcasts.append(msg))

        call_count = 0

        def _fake_execute(node_id, pipeline, run_dir):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                executor.cancel()  # cancel after first node
            return "done"

        executor._execute_node = _fake_execute  # type: ignore[assignment]
        executor.run_all(pipeline)

        skipped = [b for b in broadcasts if b["status"] == "skipped"]
        assert len(skipped) >= 1  # at least B or C should be skipped

    def test_run_from_hardlinks_upstream(self, tmp_path: Path, monkeypatch):
        monkeypatch.setattr(
            "ml_toolbox.services.file_store.PROJECTS_DIR", tmp_path / "projects"
        )

        pipeline = _make_pipeline(
            [_node("A"), _node("B")],
            [_edge("A", "B")],
        )
        pipeline["id"] = "p1"

        # Create a "previous run" with output for A
        prev_run_dir = tmp_path / "projects" / "p1" / "runs" / "prev-run"
        prev_run_dir.mkdir(parents=True)
        (prev_run_dir / "A_output.parquet").write_text("fake data")
        (prev_run_dir / "A.hash").write_text(PipelineExecutor._params_hash(_node("A")))

        executed: list[str] = []

        def _fake_execute(node_id, pipeline, run_dir):
            executed.append(node_id)
            return "done"

        executor = PipelineExecutor()
        executor._execute_node = _fake_execute  # type: ignore[assignment]
        run_id = executor.run_from("B", pipeline)

        # Only B should be executed, not A
        assert executed == ["B"]

        # A's output should be hard-linked into the new run dir
        new_run_dir = tmp_path / "projects" / "p1" / "runs" / run_id
        assert (new_run_dir / "A_output.parquet").exists()

    def test_skipped_node_from_sandbox(self, tmp_path: Path, monkeypatch):
        """When _execute_node returns 'skipped', broadcast skipped status."""
        monkeypatch.setattr(
            "ml_toolbox.services.file_store.PROJECTS_DIR", tmp_path / "projects"
        )

        pipeline = _make_pipeline(
            [_node("A"), _node("B")],
            [_edge("A", "B", condition="result.get('rows') > 100")],
        )
        pipeline["id"] = "p1"

        broadcasts: list[dict] = []
        executor = PipelineExecutor(broadcast=lambda pid, msg: broadcasts.append(msg))

        def _fake_execute(node_id, pipeline, run_dir):
            if node_id == "B":
                return "skipped"
            return "done"

        executor._execute_node = _fake_execute  # type: ignore[assignment]
        executor.run_all(pipeline)

        b_statuses = [b for b in broadcasts if b.get("node_id") == "B"]
        assert any(b["status"] == "skipped" for b in b_statuses)

    def test_error_sets_final_status(self, tmp_path: Path, monkeypatch):
        """When a node errors, final run status should be 'error'."""
        monkeypatch.setattr(
            "ml_toolbox.services.file_store.PROJECTS_DIR", tmp_path / "projects"
        )

        pipeline = _make_pipeline([_node("A")])
        pipeline["id"] = "p1"

        broadcasts: list[dict] = []
        executor = PipelineExecutor(broadcast=lambda pid, msg: broadcasts.append(msg))

        def _failing_execute(node_id, pipeline, run_dir):
            raise RuntimeError("boom")

        executor._execute_node = _failing_execute  # type: ignore[assignment]
        run_id = executor.run_all(pipeline)

        # Check error was broadcast
        assert any(b["status"] == "error" for b in broadcasts)

        # Check status file says error
        run_dir = tmp_path / "projects" / "p1" / "runs" / run_id
        status = json.loads((run_dir / "_status.json").read_text())
        assert status["status"] == "error"


# ── API Endpoint Tests ───────────────────────────────────────────


class TestExecutionAPI:
    """Test execution, status, run history, and output API endpoints."""

    @pytest.fixture(autouse=True)
    def _setup(self, tmp_path: Path, monkeypatch):
        self.tmp_path = tmp_path
        projects = tmp_path / "projects"
        monkeypatch.setattr("ml_toolbox.services.store.PROJECTS_DIR", projects)
        monkeypatch.setattr("ml_toolbox.services.file_store.PROJECTS_DIR", projects)

    def _create_pipeline(self, client) -> str:
        resp = client.post("/api/pipelines", json={"name": "Exec Test"})
        return resp.json()["id"]

    def test_run_pipeline_returns_run_id(self):
        from fastapi.testclient import TestClient
        from ml_toolbox.main import app

        client = TestClient(app)
        pid = self._create_pipeline(client)

        # Mock the _run_pipeline to avoid needing Docker
        with patch("ml_toolbox.routers.pipelines._run_pipeline", return_value="test-run-123"):
            resp = client.post(f"/api/pipelines/{pid}/run")
            assert resp.status_code == 200
            assert resp.json()["run_id"] == "test-run-123"

    def test_run_pipeline_404(self):
        from fastapi.testclient import TestClient
        from ml_toolbox.main import app

        client = TestClient(app)
        resp = client.post("/api/pipelines/nonexistent/run")
        assert resp.status_code == 404

    def test_cancel_pipeline(self):
        from fastapi.testclient import TestClient
        from ml_toolbox.main import app

        client = TestClient(app)
        pid = self._create_pipeline(client)
        resp = client.post(f"/api/pipelines/{pid}/cancel")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_pipeline_status(self):
        from fastapi.testclient import TestClient
        from ml_toolbox.main import app

        client = TestClient(app)
        pid = self._create_pipeline(client)
        resp = client.get(f"/api/pipelines/{pid}/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["is_running"] is False
        assert data["last_run_id"] is None

    def test_list_runs_empty(self):
        from fastapi.testclient import TestClient
        from ml_toolbox.main import app

        client = TestClient(app)
        pid = self._create_pipeline(client)
        resp = client.get(f"/api/pipelines/{pid}/runs")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_list_and_delete_run(self):
        from fastapi.testclient import TestClient
        from ml_toolbox.main import app
        from ml_toolbox.services import file_store

        client = TestClient(app)
        pid = self._create_pipeline(client)

        # Create a run directory manually
        run_dir = file_store.make_run_dir(pid, "run-abc")
        status_file = run_dir / "_status.json"
        status_file.write_text(json.dumps({"status": "done", "run_id": "run-abc"}))

        resp = client.get(f"/api/pipelines/{pid}/runs")
        assert resp.status_code == 200
        runs = resp.json()
        assert len(runs) == 1
        assert runs[0]["run_id"] == "run-abc"
        assert runs[0]["status"] == "done"

        # Delete the run
        resp = client.delete(f"/api/pipelines/{pid}/runs/run-abc")
        assert resp.status_code == 204

        # Verify it's gone
        resp = client.get(f"/api/pipelines/{pid}/runs")
        assert resp.json() == []

    def test_delete_run_not_found(self):
        from fastapi.testclient import TestClient
        from ml_toolbox.main import app

        client = TestClient(app)
        pid = self._create_pipeline(client)
        resp = client.delete(f"/api/pipelines/{pid}/runs/nonexistent")
        assert resp.status_code == 404

    def test_output_metadata_and_download(self):
        from fastapi.testclient import TestClient
        from ml_toolbox.main import app
        from ml_toolbox.services import file_store

        client = TestClient(app)
        pid = self._create_pipeline(client)

        # Create a run with a fake output file
        run_dir = file_store.make_run_dir(pid, "run-out")
        (run_dir / "nodeA_output.csv").write_text("col1,col2\n1,2\n3,4")

        # Get output metadata
        resp = client.get(f"/api/pipelines/{pid}/outputs/nodeA?run_id=run-out")
        assert resp.status_code == 200
        data = resp.json()
        assert data["node_id"] == "nodeA"
        assert data["type"] == "csv"
        assert data["size"] > 0

        # Download
        resp = client.get(f"/api/pipelines/{pid}/outputs/nodeA/download?run_id=run-out")
        assert resp.status_code == 200
        assert b"col1,col2" in resp.content

    def test_output_from_specific_run(self):
        from fastapi.testclient import TestClient
        from ml_toolbox.main import app
        from ml_toolbox.services import file_store

        client = TestClient(app)
        pid = self._create_pipeline(client)

        run_dir = file_store.make_run_dir(pid, "run-specific")
        (run_dir / "nodeB_output.csv").write_text("a,b\n1,2")

        resp = client.get(f"/api/pipelines/{pid}/runs/run-specific/outputs/nodeB")
        assert resp.status_code == 200
        assert resp.json()["node_id"] == "nodeB"

        resp = client.get(f"/api/pipelines/{pid}/runs/run-specific/outputs/nodeB/download")
        assert resp.status_code == 200
        assert b"a,b" in resp.content

    def test_output_not_found(self):
        from fastapi.testclient import TestClient
        from ml_toolbox.main import app
        from ml_toolbox.services import file_store

        client = TestClient(app)
        pid = self._create_pipeline(client)
        file_store.make_run_dir(pid, "run-empty")

        resp = client.get(f"/api/pipelines/{pid}/outputs/nonexistent?run_id=run-empty")
        assert resp.status_code == 404

    def test_run_from_node_404(self):
        from fastapi.testclient import TestClient
        from ml_toolbox.main import app

        client = TestClient(app)
        pid = self._create_pipeline(client)
        resp = client.post(f"/api/pipelines/{pid}/run/nonexistent")
        assert resp.status_code == 404


# ── WebSocket Tests ──────────────────────────────────────────────


class TestWebSocket:
    def test_websocket_connect_and_receive(self, tmp_path: Path, monkeypatch):
        monkeypatch.setattr(
            "ml_toolbox.services.store.PROJECTS_DIR", tmp_path / "projects"
        )
        monkeypatch.setattr(
            "ml_toolbox.services.file_store.PROJECTS_DIR", tmp_path / "projects"
        )
        from fastapi.testclient import TestClient
        from ml_toolbox.main import app
        from ml_toolbox.routers.ws import manager

        client = TestClient(app)

        with client.websocket_connect("/ws/pipelines/test-pipeline") as ws:
            # The connection should be tracked
            assert "test-pipeline" in manager._connections
            assert len(manager._connections["test-pipeline"]) == 1
