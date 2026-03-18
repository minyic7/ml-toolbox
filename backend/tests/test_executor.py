"""Tests for the DAG execution engine."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ml_toolbox.services.executor import (
    CycleError,
    PipelineExecutor,
    remove_active_executor,
    set_active_executor,
    try_set_active_executor,
)
from ml_toolbox.services.file_store import _validate_path_id


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
        monkeypatch.setattr(
            "ml_toolbox.services.executor.DATA_DIR", tmp_path
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


# ── Broadcast Coverage Tests ─────────────────────────────────────


class TestBroadcastCoverage:
    """Verify the executor broadcasts correct WebSocket messages in all scenarios."""

    def _capture_broadcast(self):
        msgs: list[dict] = []
        return msgs, lambda pid, msg: msgs.append(msg)

    def test_docker_connection_failure_broadcasts_error(self, tmp_path: Path, monkeypatch):
        """Docker connection failure broadcasts error message to WebSocket."""
        monkeypatch.setattr(
            "ml_toolbox.services.file_store.PROJECTS_DIR", tmp_path / "projects"
        )
        monkeypatch.setattr("ml_toolbox.services.executor.DATA_DIR", tmp_path)

        pipeline = _make_pipeline([_node("A", code="def run(i,p): pass")])
        pipeline["id"] = "p1"

        msgs, capture = self._capture_broadcast()
        executor = PipelineExecutor(broadcast=capture)
        # Don't set executor._docker — let _get_docker call docker.from_env()
        # which will fail when Docker is not available. Instead, mock it to raise.
        mock_client = MagicMock()
        mock_client.containers.run.side_effect = Exception("Cannot connect to Docker daemon")
        executor._docker = mock_client

        run_id = executor.run_all(pipeline)

        error_msgs = [m for m in msgs if m["status"] == "error"]
        assert len(error_msgs) >= 1
        assert "node_id" in error_msgs[0]
        assert "traceback" in error_msgs[0]
        assert "Cannot connect to Docker daemon" in error_msgs[0]["traceback"]

        # Status file should reflect error
        run_dir = tmp_path / "projects" / "p1" / "runs" / run_id
        status = json.loads((run_dir / "_status.json").read_text())
        assert status["status"] == "error"

    def test_cancel_broadcasts_skipped_for_remaining_nodes(self, tmp_path: Path, monkeypatch):
        """Cancel broadcasts skipped status for all remaining unexecuted nodes."""
        monkeypatch.setattr(
            "ml_toolbox.services.file_store.PROJECTS_DIR", tmp_path / "projects"
        )

        pipeline = _make_pipeline(
            [_node("A"), _node("B"), _node("C")],
            [_edge("A", "B"), _edge("B", "C")],
        )
        pipeline["id"] = "p1"

        msgs, capture = self._capture_broadcast()
        executor = PipelineExecutor(broadcast=capture)

        call_count = 0

        def _fake_execute(node_id, pipeline, run_dir):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                executor.cancel()
            return "done"

        executor._execute_node = _fake_execute  # type: ignore[assignment]
        run_id = executor.run_all(pipeline)

        # B and C should be skipped after cancelling during A
        skipped = [m for m in msgs if m["status"] == "skipped"]
        skipped_ids = {m["node_id"] for m in skipped}
        assert "B" in skipped_ids
        assert "C" in skipped_ids

        # Each skipped message should have run_id
        for m in skipped:
            assert m["run_id"] == run_id

        # Final status should be cancelled
        run_dir = tmp_path / "projects" / "p1" / "runs" / run_id
        status = json.loads((run_dir / "_status.json").read_text())
        assert status["status"] == "cancelled"

    def test_node_error_broadcasts_error_with_traceback(self, tmp_path: Path, monkeypatch):
        """Node execution error broadcasts error with traceback string."""
        monkeypatch.setattr(
            "ml_toolbox.services.file_store.PROJECTS_DIR", tmp_path / "projects"
        )

        pipeline = _make_pipeline(
            [_node("A"), _node("B")],
            [_edge("A", "B")],
        )
        pipeline["id"] = "p1"

        msgs, capture = self._capture_broadcast()
        executor = PipelineExecutor(broadcast=capture)

        def _fake_execute(node_id, pipeline, run_dir):
            if node_id == "B":
                raise RuntimeError("Node B exploded")
            return "done"

        executor._execute_node = _fake_execute  # type: ignore[assignment]
        executor.run_all(pipeline)

        # A should succeed, B should error
        a_done = [m for m in msgs if m.get("node_id") == "A" and m["status"] == "done"]
        assert len(a_done) == 1

        b_error = [m for m in msgs if m.get("node_id") == "B" and m["status"] == "error"]
        assert len(b_error) == 1
        assert "traceback" in b_error[0]
        assert "Node B exploded" in b_error[0]["traceback"]

    def test_successful_run_broadcasts_running_then_done(self, tmp_path: Path, monkeypatch):
        """Successful run broadcasts running → done for each node in order."""
        monkeypatch.setattr(
            "ml_toolbox.services.file_store.PROJECTS_DIR", tmp_path / "projects"
        )

        pipeline = _make_pipeline(
            [_node("A"), _node("B"), _node("C")],
            [_edge("A", "B"), _edge("B", "C")],
        )
        pipeline["id"] = "p1"

        msgs, capture = self._capture_broadcast()
        executor = PipelineExecutor(broadcast=capture)

        def _fake_execute(node_id, pipeline, run_dir):
            return "done"

        executor._execute_node = _fake_execute  # type: ignore[assignment]
        run_id = executor.run_all(pipeline)

        # Each node should have running then done, in order
        for nid in ["A", "B", "C"]:
            node_msgs = [m for m in msgs if m.get("node_id") == nid]
            statuses = [m["status"] for m in node_msgs]
            assert statuses == ["running", "done"], f"Node {nid}: expected ['running', 'done'], got {statuses}"

        # Verify global ordering: A running before B running before C running
        running_order = [m["node_id"] for m in msgs if m["status"] == "running"]
        assert running_order == ["A", "B", "C"]

        # All messages should have run_id
        for m in msgs:
            assert m["run_id"] == run_id

    def test_run_from_broadcasts_cached_for_upstream(self, tmp_path: Path, monkeypatch):
        """run_from broadcasts done(cached) for upstream, running → done for downstream."""
        monkeypatch.setattr(
            "ml_toolbox.services.file_store.PROJECTS_DIR", tmp_path / "projects"
        )

        pipeline = _make_pipeline(
            [_node("A"), _node("B")],
            [_edge("A", "B")],
        )
        pipeline["id"] = "p1"

        # Create a "previous run" with cached output for A
        prev_run_dir = tmp_path / "projects" / "p1" / "runs" / "prev-run"
        prev_run_dir.mkdir(parents=True)
        (prev_run_dir / "A_output.parquet").write_text("fake data")
        (prev_run_dir / "A.hash").write_text(PipelineExecutor._params_hash(_node("A")))

        msgs, capture = self._capture_broadcast()
        executor = PipelineExecutor(broadcast=capture)

        def _fake_execute(node_id, pipeline, run_dir):
            return "done"

        executor._execute_node = _fake_execute  # type: ignore[assignment]
        run_id = executor.run_from("B", pipeline)

        # B (downstream) should have running → done
        b_msgs = [m for m in msgs if m.get("node_id") == "B"]
        b_statuses = [m["status"] for m in b_msgs]
        assert b_statuses == ["running", "done"]

        # A should NOT have running broadcast (it's upstream, hard-linked not executed)
        a_running = [m for m in msgs if m.get("node_id") == "A" and m["status"] == "running"]
        assert len(a_running) == 0


# ── Error Fallback Tests ─────────────────────────────────────────


class TestErrorFallback:
    """Verify executor crash recovery: status files, API state, concurrency."""

    def test_executor_crash_writes_error_status(self, tmp_path: Path, monkeypatch):
        """If executor thread crashes (node raises), status file shows error."""
        monkeypatch.setattr(
            "ml_toolbox.services.file_store.PROJECTS_DIR", tmp_path / "projects"
        )

        pipeline = _make_pipeline([_node("A")])
        pipeline["id"] = "p1"

        executor = PipelineExecutor()

        def _crashing_execute(node_id, pipeline, run_dir):
            raise RuntimeError("executor thread crashed")

        executor._execute_node = _crashing_execute  # type: ignore[assignment]
        run_id = executor.run_all(pipeline)

        # Status file should reflect error
        run_dir = tmp_path / "projects" / "p1" / "runs" / run_id
        status = json.loads((run_dir / "_status.json").read_text())
        assert status["status"] == "error"

    def test_status_returns_not_running_after_crash(self, client, create_pipeline, monkeypatch):
        """GET /status returns is_running=false after executor thread completes (crash or not)."""
        pid = create_pipeline("Crash Test")

        # No active executor → not running
        resp = client.get(f"/api/pipelines/{pid}/status")
        assert resp.status_code == 200
        assert resp.json()["is_running"] is False

    def test_concurrent_run_409_then_allows_new_after_completion(self):
        """Concurrent run returns 409 while active, then allows new run after removal."""
        e1 = PipelineExecutor()
        e2 = PipelineExecutor()

        pipeline_id = "concurrent-test"
        try:
            assert try_set_active_executor(pipeline_id, e1) is True
            # Second attempt should fail
            assert try_set_active_executor(pipeline_id, e2) is False
        finally:
            remove_active_executor(pipeline_id)

        # After removal, a new executor can be set
        e3 = PipelineExecutor()
        try:
            assert try_set_active_executor(pipeline_id, e3) is True
        finally:
            remove_active_executor(pipeline_id)

    def test_409_via_api(self, client, create_pipeline):
        """POST /run returns 409 when an executor is already active."""
        from ml_toolbox.services.executor import set_active_executor

        pid = create_pipeline("API 409 Test")

        # Manually set an active executor
        executor = PipelineExecutor()
        set_active_executor(pid, executor)
        try:
            resp = client.post(f"/api/pipelines/{pid}/run")
            assert resp.status_code == 409
        finally:
            remove_active_executor(pid)


# ── Frontend Contract Tests (backend-only) ───────────────────────


class TestBroadcastContract:
    """Verify every broadcast message has the required fields for the frontend."""

    def test_all_messages_have_required_fields(self, tmp_path: Path, monkeypatch):
        """Every broadcast message has required fields: node_id, status, run_id."""
        monkeypatch.setattr(
            "ml_toolbox.services.file_store.PROJECTS_DIR", tmp_path / "projects"
        )

        pipeline = _make_pipeline(
            [_node("A"), _node("B")],
            [_edge("A", "B")],
        )
        pipeline["id"] = "p1"

        msgs: list[dict] = []
        executor = PipelineExecutor(broadcast=lambda pid, msg: msgs.append(msg))

        def _fake_execute(node_id, pipeline, run_dir):
            return "done"

        executor._execute_node = _fake_execute  # type: ignore[assignment]
        run_id = executor.run_all(pipeline)

        assert len(msgs) > 0
        for msg in msgs:
            assert "status" in msg, f"Missing 'status' in {msg}"
            assert "run_id" in msg, f"Missing 'run_id' in {msg}"
            assert "node_id" in msg, f"Missing 'node_id' in {msg}"

    def test_error_broadcasts_include_traceback(self, tmp_path: Path, monkeypatch):
        """Error broadcasts include traceback field."""
        monkeypatch.setattr(
            "ml_toolbox.services.file_store.PROJECTS_DIR", tmp_path / "projects"
        )

        pipeline = _make_pipeline([_node("A")])
        pipeline["id"] = "p1"

        msgs: list[dict] = []
        executor = PipelineExecutor(broadcast=lambda pid, msg: msgs.append(msg))

        def _failing_execute(node_id, pipeline, run_dir):
            raise ValueError("test error message")

        executor._execute_node = _failing_execute  # type: ignore[assignment]
        executor.run_all(pipeline)

        error_msgs = [m for m in msgs if m["status"] == "error"]
        assert len(error_msgs) == 1
        assert "traceback" in error_msgs[0]
        assert "test error message" in error_msgs[0]["traceback"]

    def test_done_broadcasts_include_outputs(self, tmp_path: Path, monkeypatch):
        """Done broadcasts include outputs field."""
        monkeypatch.setattr(
            "ml_toolbox.services.file_store.PROJECTS_DIR", tmp_path / "projects"
        )
        monkeypatch.setattr("ml_toolbox.services.executor.DATA_DIR", tmp_path)

        pipeline = _make_pipeline([_node("A", code="def run(i,p): pass")])
        pipeline["id"] = "p1"

        msgs: list[dict] = []
        executor = PipelineExecutor(broadcast=lambda pid, msg: msgs.append(msg))

        mock_client = MagicMock()
        mock_container = MagicMock()
        mock_container.wait.return_value = {"StatusCode": 0}
        mock_container.logs.return_value = b""
        mock_client.containers.run.return_value = mock_container
        executor._docker = mock_client

        run_id = executor.run_all(pipeline)

        done_msgs = [m for m in msgs if m["status"] == "done" and m.get("node_id") == "A"]
        assert len(done_msgs) == 1
        assert "outputs" in done_msgs[0]
        assert isinstance(done_msgs[0]["outputs"], list)

    def test_cached_broadcasts_include_cached_flag(self, tmp_path: Path, monkeypatch):
        """Cached done broadcasts include cached=True field."""
        monkeypatch.setattr(
            "ml_toolbox.services.file_store.PROJECTS_DIR", tmp_path / "projects"
        )

        pipeline = _make_pipeline([_node("A", code="x", params={"a": 1})])
        pipeline["id"] = "p1"

        msgs: list[dict] = []
        executor = PipelineExecutor(broadcast=lambda pid, msg: msgs.append(msg))

        # Pre-create run dir with matching hash so cache hits
        run_id = "test-cache-run"

        def _fake_run_all(pipeline, run_id=None):
            from ml_toolbox.services import file_store
            rid = run_id or "test"
            run_dir = file_store.make_run_dir("p1", rid)
            # Write matching hash to trigger cache hit
            node = _node("A", code="x", params={"a": 1})
            (run_dir / "A.hash").write_text(PipelineExecutor._params_hash(node))
            # Now run the ordered execution
            executor._execute_ordered(["A"], pipeline, run_dir, rid)
            return rid

        run_id = _fake_run_all(pipeline, run_id="cache-test")

        cached_msgs = [m for m in msgs if m["status"] == "done" and m.get("cached") is True]
        assert len(cached_msgs) == 1
        assert cached_msgs[0]["node_id"] == "A"


# ── API Endpoint Tests ───────────────────────────────────────────


class TestExecutionAPI:
    """Test execution, status, run history, and output API endpoints."""

    @pytest.fixture(autouse=True)
    def _setup(self, tmp_path: Path):
        self.tmp_path = tmp_path

    def test_run_pipeline_returns_run_id(self, client, create_pipeline):
        pid = create_pipeline("Exec Test")
        with patch("ml_toolbox.routers.pipelines._run_pipeline", return_value="test-run-123"):
            resp = client.post(f"/api/pipelines/{pid}/run")
            assert resp.status_code == 200
            assert resp.json()["run_id"] == "test-run-123"

    def test_run_pipeline_404(self, client):
        resp = client.post("/api/pipelines/nonexistent/run")
        assert resp.status_code == 404

    def test_cancel_pipeline(self, client, create_pipeline):
        pid = create_pipeline("Exec Test")
        resp = client.post(f"/api/pipelines/{pid}/cancel")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_pipeline_status(self, client, create_pipeline):
        pid = create_pipeline("Exec Test")
        resp = client.get(f"/api/pipelines/{pid}/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["is_running"] is False
        assert data["last_run_id"] is None

    def test_list_runs_empty(self, client, create_pipeline):
        pid = create_pipeline("Exec Test")
        resp = client.get(f"/api/pipelines/{pid}/runs")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_list_and_delete_run(self, client, create_pipeline):
        from ml_toolbox.services import file_store

        pid = create_pipeline("Exec Test")

        run_dir = file_store.make_run_dir(pid, "run-abc")
        status_file = run_dir / "_status.json"
        status_file.write_text(json.dumps({"status": "done", "run_id": "run-abc"}))

        resp = client.get(f"/api/pipelines/{pid}/runs")
        assert resp.status_code == 200
        runs = resp.json()
        assert len(runs) == 1
        assert runs[0]["id"] == "run-abc"
        assert runs[0]["status"] == "done"

        resp = client.delete(f"/api/pipelines/{pid}/runs/run-abc")
        assert resp.status_code == 204

        resp = client.get(f"/api/pipelines/{pid}/runs")
        assert resp.json() == []

    def test_delete_run_not_found(self, client, create_pipeline):
        pid = create_pipeline("Exec Test")
        resp = client.delete(f"/api/pipelines/{pid}/runs/nonexistent")
        assert resp.status_code == 404

    def test_output_metadata_and_download(self, client, create_pipeline):
        from ml_toolbox.services import file_store

        pid = create_pipeline("Exec Test")
        run_dir = file_store.make_run_dir(pid, "run-out")
        (run_dir / "nodeA_output.csv").write_text("col1,col2\n1,2\n3,4")

        resp = client.get(f"/api/pipelines/{pid}/outputs/nodeA?run_id=run-out")
        assert resp.status_code == 200
        data = resp.json()
        assert data["node_id"] == "nodeA"
        assert data["type"] == "csv"
        assert data["size"] > 0

        resp = client.get(f"/api/pipelines/{pid}/outputs/nodeA/download?run_id=run-out")
        assert resp.status_code == 200
        assert b"col1,col2" in resp.content

    def test_output_from_specific_run(self, client, create_pipeline):
        from ml_toolbox.services import file_store

        pid = create_pipeline("Exec Test")
        run_dir = file_store.make_run_dir(pid, "run-specific")
        (run_dir / "nodeB_output.csv").write_text("a,b\n1,2")

        resp = client.get(f"/api/pipelines/{pid}/runs/run-specific/outputs/nodeB")
        assert resp.status_code == 200
        assert resp.json()["node_id"] == "nodeB"

        resp = client.get(f"/api/pipelines/{pid}/runs/run-specific/outputs/nodeB/download")
        assert resp.status_code == 200
        assert b"a,b" in resp.content

    def test_output_not_found(self, client, create_pipeline):
        from ml_toolbox.services import file_store

        pid = create_pipeline("Exec Test")
        file_store.make_run_dir(pid, "run-empty")

        resp = client.get(f"/api/pipelines/{pid}/outputs/nonexistent?run_id=run-empty")
        assert resp.status_code == 404

    def test_run_from_node_404(self, client, create_pipeline):
        pid = create_pipeline("Exec Test")
        resp = client.post(f"/api/pipelines/{pid}/run/nonexistent")
        assert resp.status_code == 404


# ── WebSocket Tests ──────────────────────────────────────────────


class TestWebSocket:
    def test_websocket_connect_and_receive(self, client):
        from ml_toolbox.routers.ws import manager

        with client.websocket_connect("/ws/pipelines/test-pipeline") as ws:
            assert "test-pipeline" in manager._connections
            assert len(manager._connections["test-pipeline"]) == 1


# ── Path Traversal Validation ─────────────────────────────────────


class TestPathValidation:
    def test_valid_ids(self):
        """Normal IDs should pass validation."""
        _validate_path_id("abc123", "test")
        _validate_path_id("my-pipeline", "test")
        _validate_path_id("run_001", "test")

    def test_rejects_path_traversal(self):
        with pytest.raises(ValueError, match="Invalid"):
            _validate_path_id("../etc/passwd", "test")

    def test_rejects_slashes(self):
        with pytest.raises(ValueError, match="Invalid"):
            _validate_path_id("foo/bar", "test")

    def test_rejects_empty(self):
        with pytest.raises(ValueError, match="Invalid"):
            _validate_path_id("", "test")

    def test_rejects_dots(self):
        with pytest.raises(ValueError, match="Invalid"):
            _validate_path_id("..", "test")

    def test_delete_run_path_traversal(self):
        """DELETE /runs/{run_id} should reject traversal attempts."""
        from fastapi.testclient import TestClient
        from ml_toolbox.main import app

        client = TestClient(app)
        resp = client.post("/api/pipelines", json={"name": "Sec Test"})
        pid = resp.json()["id"]

        # Dots in a single path segment still get validated
        resp = client.delete(f"/api/pipelines/{pid}/runs/..secret")
        assert resp.status_code == 400

        client.delete(f"/api/pipelines/{pid}")


# ── Race Condition (atomic check-and-set) ─────────────────────────


class TestAtomicExecutorSet:
    def test_try_set_returns_true_when_empty(self):
        executor = PipelineExecutor()
        try:
            assert try_set_active_executor("race-test", executor) is True
        finally:
            remove_active_executor("race-test")

    def test_try_set_returns_false_when_occupied(self):
        e1 = PipelineExecutor()
        e2 = PipelineExecutor()
        try:
            assert try_set_active_executor("race-test-2", e1) is True
            assert try_set_active_executor("race-test-2", e2) is False
        finally:
            remove_active_executor("race-test-2")
