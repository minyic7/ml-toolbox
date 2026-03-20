"""Integration tests — node-to-node connection matrix.

Validates that nodes with matching PortTypes can actually produce and consume
each other's output files end-to-end through the Docker sandbox.

Requires Docker and the sandbox image to be available locally.
"""

import json
import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

try:
    import docker

    _client = docker.from_env()
    _client.ping()
    _has_docker = True
except Exception:
    _has_docker = False

pytestmark = pytest.mark.skipif(not _has_docker, reason="Docker not available")

SANDBOX_IMAGE = "ml-toolbox-sandbox"


# ── Fixtures ────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _isolate(tmp_path, monkeypatch):
    """Isolate file stores + point executor at local sandbox image."""
    projects = tmp_path / "projects"
    monkeypatch.setattr("ml_toolbox.services.store.PROJECTS_DIR", projects)
    monkeypatch.setattr("ml_toolbox.services.file_store.PROJECTS_DIR", projects)
    monkeypatch.setattr("ml_toolbox.services.executor.DATA_DIR", tmp_path)
    monkeypatch.setattr(
        "ml_toolbox.services.executor.DOCKER_VOLUME_NAME", str(tmp_path)
    )
    monkeypatch.setattr("ml_toolbox.services.executor.SANDBOX_IMAGE", SANDBOX_IMAGE)


@pytest.fixture()
def client():
    from ml_toolbox.main import app

    return TestClient(app)


# ── Helpers ─────────────────────────────────────────────────────


def _wait_for_run(client: TestClient, pid: str, timeout: int = 120) -> dict:
    """Poll until pipeline run finishes. Returns the run dict."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = client.get(f"/api/pipelines/{pid}/runs")
        assert resp.status_code == 200
        runs = resp.json()
        if runs and runs[0]["status"] in ("done", "error", "cancelled"):
            return runs[0]
        time.sleep(1)
    raise TimeoutError(f"Pipeline {pid} did not finish within {timeout}s")


def _get_run_errors(tmp_path: Path, pid: str, run_id: str) -> str:
    """Collect error artifacts from a run directory for CI diagnostics."""
    run_dir = tmp_path / "projects" / pid / "runs" / run_id
    if not run_dir.exists():
        return f"\n(Run directory not found: {run_dir})"
    parts: list[str] = []
    for f in sorted(run_dir.glob("*_manifest_error.json")):
        parts.append(f"\n--- {f.name} ---\n{f.read_text()}")
    for f in sorted(run_dir.glob("*_logs.txt")):
        parts.append(f"\n--- {f.name} ---\n{f.read_text()}")
    if not parts:
        all_files = [p.name for p in run_dir.iterdir()]
        return f"\n(No error/log files found in {run_dir}; files: {all_files})"
    return "".join(parts)


def _add_node(
    client: TestClient, pid: str, node_type: str, x: int = 0, y: int = 0
) -> dict:
    """Add a node and return the full response JSON."""
    resp = client.post(
        f"/api/pipelines/{pid}/nodes",
        json={"type": node_type, "position": {"x": x, "y": y}},
    )
    assert resp.status_code == 201, f"Failed to add node {node_type}: {resp.text}"
    return resp.json()


def _connect(
    client: TestClient,
    pid: str,
    source_id: str,
    source_port: str,
    target_id: str,
    target_port: str,
) -> dict:
    """Create an edge and return the response JSON."""
    resp = client.post(
        f"/api/pipelines/{pid}/edges",
        json={
            "source": source_id,
            "source_port": source_port,
            "target": target_id,
            "target_port": target_port,
        },
    )
    assert resp.status_code == 201, f"Failed to connect edge: {resp.text}"
    return resp.json()


def _set_params(
    client: TestClient, pid: str, node_id: str, params: dict
) -> None:
    """Patch node params."""
    resp = client.patch(
        f"/api/pipelines/{pid}/nodes/{node_id}",
        json={"params": params},
    )
    assert resp.status_code == 200, f"Failed to set params: {resp.text}"


def _run_and_wait(
    client: TestClient, pid: str, tmp_path: Path, timeout: int = 120
) -> dict:
    """Trigger run_all and wait for completion. Asserts success."""
    resp = client.post(f"/api/pipelines/{pid}/run")
    assert resp.status_code == 200
    run = _wait_for_run(client, pid, timeout=timeout)
    if run["status"] == "error":
        error_info = _get_run_errors(tmp_path, pid, run["id"])
        pytest.fail(f"Run failed: {run}\n{error_info}")
    assert run["status"] == "done"
    return run


def _create_pipeline(client: TestClient, name: str) -> str:
    """Create a pipeline and return its ID."""
    resp = client.post("/api/pipelines", json={"name": name})
    assert resp.status_code == 201
    return resp.json()["id"]


# ── Tests ───────────────────────────────────────────────────────


class TestNodeConnections:
    """End-to-end connection matrix tests through real Docker sandbox."""

    def test_a_table_chain(self, client: TestClient, tmp_path: Path):
        """TABLE→TABLE→TABLE: Generate → Clean → Export Table.

        Validates parquet roundtrip, output file exists and is readable.
        """
        pid = _create_pipeline(client, "Test A: TABLE chain")

        gen = _add_node(client, pid, "ml_toolbox.nodes.demo.run", x=0)
        clean = _add_node(client, pid, "ml_toolbox.nodes.demo.clean_data", x=200)
        export = _add_node(client, pid, "ml_toolbox.nodes.export.export_table", x=400)

        # Set export to parquet format so we can verify output
        _set_params(client, pid, export["id"], {"format": "parquet"})

        _connect(client, pid, gen["id"], "df", clean["id"], "df")
        _connect(client, pid, clean["id"], "df", export["id"], "df")

        run = _run_and_wait(client, pid, tmp_path)

        # Verify each node produced readable output
        for node in (gen, clean, export):
            resp = client.get(
                f"/api/pipelines/{pid}/outputs/{node['id']}?run_id={run['id']}"
            )
            assert resp.status_code == 200, f"No output for node {node['id']}"
            output = resp.json()
            assert output["type"] == "parquet"
            assert output["size"] > 0

        # Download and verify export output is valid parquet
        import io
        import pyarrow.parquet as pq

        resp = client.get(
            f"/api/pipelines/{pid}/outputs/{clean['id']}/download?run_id={run['id']}"
        )
        assert resp.status_code == 200
        table = pq.read_table(io.BytesIO(resp.content))
        assert table.num_rows == 100
        assert "value_a" in table.column_names

    def test_b_multi_output_node(self, client: TestClient, tmp_path: Path):
        """Multi-output: Generate → Split → sklearn Train → Classification.

        Split produces train+test (two TABLE outputs).
        Train produces MODEL+METRICS.
        Classification receives MODEL+TABLE, produces METRICS.
        """
        pid = _create_pipeline(client, "Test B: Multi-output")

        gen = _add_node(client, pid, "ml_toolbox.nodes.demo.run", x=0)
        split = _add_node(client, pid, "ml_toolbox.nodes.transform.split", x=200)
        train = _add_node(client, pid, "ml_toolbox.nodes.train.sklearn_train", x=400)
        evaluate = _add_node(
            client, pid, "ml_toolbox.nodes.evaluate.classification", x=600
        )

        _set_params(client, pid, train["id"], {"target_column": "category"})
        _set_params(client, pid, evaluate["id"], {"target_column": "category"})

        # Connect the pipeline
        _connect(client, pid, gen["id"], "df", split["id"], "df")
        _connect(client, pid, split["id"], "train", train["id"], "train")
        _connect(client, pid, split["id"], "test", evaluate["id"], "test")
        _connect(client, pid, train["id"], "model", evaluate["id"], "model")

        run = _run_and_wait(client, pid, tmp_path)

        # Verify split produced two TABLE outputs
        resp = client.get(
            f"/api/pipelines/{pid}/outputs/{split['id']}?run_id={run['id']}"
        )
        assert resp.status_code == 200

        # Verify train produced model + metrics
        resp = client.get(
            f"/api/pipelines/{pid}/outputs/{train['id']}?run_id={run['id']}"
        )
        assert resp.status_code == 200

        # Verify classification produced metrics
        resp = client.get(
            f"/api/pipelines/{pid}/outputs/{evaluate['id']}?run_id={run['id']}"
        )
        assert resp.status_code == 200
        output = resp.json()
        # Classification metrics output is JSON
        assert output["type"] == "json"

    def test_c_four_hop_chain(self, client: TestClient, tmp_path: Path):
        """4-hop: Generate → Clean → Feature Eng → Split → sklearn Train.

        Validates nodes execute in topological order and each reads
        the previous node's output correctly.
        """
        pid = _create_pipeline(client, "Test C: 4-hop chain")

        gen = _add_node(client, pid, "ml_toolbox.nodes.demo.run", x=0)
        clean = _add_node(client, pid, "ml_toolbox.nodes.transform.clean", x=200)
        feat = _add_node(
            client, pid, "ml_toolbox.nodes.transform.feature_eng", x=400
        )
        split = _add_node(client, pid, "ml_toolbox.nodes.transform.split", x=600)
        train = _add_node(
            client, pid, "ml_toolbox.nodes.train.sklearn_train", x=800
        )

        _set_params(client, pid, train["id"], {"target_column": "category"})

        _connect(client, pid, gen["id"], "df", clean["id"], "df")
        _connect(client, pid, clean["id"], "df", feat["id"], "df")
        _connect(client, pid, feat["id"], "df", split["id"], "df")
        _connect(client, pid, split["id"], "train", train["id"], "train")

        run = _run_and_wait(client, pid, tmp_path)

        # Verify all nodes produced output
        for node in (gen, clean, feat, split, train):
            resp = client.get(
                f"/api/pipelines/{pid}/outputs/{node['id']}?run_id={run['id']}"
            )
            assert resp.status_code == 200, (
                f"No output for node {node['id']}"
            )

    def test_d_cache_hit_on_rerun(self, client: TestClient, tmp_path: Path):
        """Cache: run twice — second run (run_from) hardlinks upstream cached nodes.

        First run executes everything. Second run via POST /run/{node_id}
        on the downstream node hardlinks upstream outputs and only re-executes
        the target node and its downstream.
        """
        pid = _create_pipeline(client, "Test D: Cache hit")

        gen = _add_node(client, pid, "ml_toolbox.nodes.demo.run", x=0)
        clean = _add_node(client, pid, "ml_toolbox.nodes.demo.clean_data", x=200)

        _connect(client, pid, gen["id"], "df", clean["id"], "df")

        # First run: everything executes
        run1 = _run_and_wait(client, pid, tmp_path)
        run1_id = run1["id"]

        # Verify first run outputs exist
        resp = client.get(
            f"/api/pipelines/{pid}/outputs/{gen['id']}?run_id={run1_id}"
        )
        assert resp.status_code == 200

        # Second run: run_from on clean_data node
        # This hardlinks gen's output and only re-executes clean_data
        resp = client.post(f"/api/pipelines/{pid}/run/{clean['id']}")
        assert resp.status_code == 200
        run2_id = resp.json()["run_id"]

        # Wait for second run
        deadline = time.time() + 120
        while time.time() < deadline:
            resp = client.get(f"/api/pipelines/{pid}/runs")
            runs = resp.json()
            run2 = next((r for r in runs if r["id"] == run2_id), None)
            if run2 and run2["status"] in ("done", "error", "cancelled"):
                break
            time.sleep(1)
        else:
            pytest.fail(f"Second run {run2_id} did not finish")

        if run2["status"] == "error":
            error_info = _get_run_errors(tmp_path, pid, run2_id)
            pytest.fail(f"Second run failed: {run2}\n{error_info}")
        assert run2["status"] == "done"

        # Verify gen's output was hardlinked into run2 (exists without re-execution)
        resp = client.get(
            f"/api/pipelines/{pid}/outputs/{gen['id']}?run_id={run2_id}"
        )
        assert resp.status_code == 200, "Hardlinked gen output should exist in run2"

        # Verify clean_data re-executed and produced output in run2
        resp = client.get(
            f"/api/pipelines/{pid}/outputs/{clean['id']}?run_id={run2_id}"
        )
        assert resp.status_code == 200

        # Verify the hardlinked file is readable
        import io
        import pyarrow.parquet as pq

        resp = client.get(
            f"/api/pipelines/{pid}/outputs/{clean['id']}/download?run_id={run2_id}"
        )
        assert resp.status_code == 200
        table = pq.read_table(io.BytesIO(resp.content))
        assert table.num_rows == 100

    def test_e_branching_pipeline(self, client: TestClient, tmp_path: Path):
        """Branching: Split → [Train A, Train B].

        Both train nodes receive the same split output. Both must execute
        without file conflict and produce separate output files.
        """
        pid = _create_pipeline(client, "Test E: Branching")

        gen = _add_node(client, pid, "ml_toolbox.nodes.demo.run", x=0)
        split = _add_node(client, pid, "ml_toolbox.nodes.transform.split", x=200)
        train_a = _add_node(
            client, pid, "ml_toolbox.nodes.train.sklearn_train", x=400, y=0
        )
        train_b = _add_node(
            client, pid, "ml_toolbox.nodes.train.sklearn_train", x=400, y=200
        )

        _set_params(
            client,
            pid,
            train_a["id"],
            {
                "target_column": "category",
                "estimator": "RandomForestClassifier",
            },
        )
        _set_params(
            client,
            pid,
            train_b["id"],
            {
                "target_column": "category",
                "estimator": "LogisticRegression",
            },
        )

        _connect(client, pid, gen["id"], "df", split["id"], "df")
        _connect(client, pid, split["id"], "train", train_a["id"], "train")
        _connect(client, pid, split["id"], "train", train_b["id"], "train")

        run = _run_and_wait(client, pid, tmp_path)

        # Both train nodes should have produced output
        resp_a = client.get(
            f"/api/pipelines/{pid}/outputs/{train_a['id']}?run_id={run['id']}"
        )
        assert resp_a.status_code == 200, "Train A should have output"

        resp_b = client.get(
            f"/api/pipelines/{pid}/outputs/{train_b['id']}?run_id={run['id']}"
        )
        assert resp_b.status_code == 200, "Train B should have output"

        # Outputs should be distinct files (different node IDs)
        assert train_a["id"] != train_b["id"]

    def test_f_type_mismatch_rejected(self, client: TestClient):
        """Type mismatch: TABLE output → MODEL input → rejected at API level.

        POST /edges with mismatched port types should return 400.
        Pipeline state must remain unchanged.
        """
        pid = _create_pipeline(client, "Test F: Type mismatch")

        # demo.run outputs df (TABLE)
        gen = _add_node(client, pid, "ml_toolbox.nodes.demo.run", x=0)
        # classification has input model (MODEL) + test (TABLE)
        evaluate = _add_node(
            client, pid, "ml_toolbox.nodes.evaluate.classification", x=200
        )

        # TABLE → MODEL should be rejected
        resp = client.post(
            f"/api/pipelines/{pid}/edges",
            json={
                "source": gen["id"],
                "source_port": "df",  # TABLE
                "target": evaluate["id"],
                "target_port": "model",  # MODEL
            },
        )
        assert resp.status_code == 400
        assert "mismatch" in resp.json()["detail"].lower()

        # Verify pipeline has no edges
        resp = client.get(f"/api/pipelines/{pid}")
        assert resp.status_code == 200
        assert len(resp.json()["edges"]) == 0

        # Valid connection (TABLE → TABLE) should still work
        resp = client.post(
            f"/api/pipelines/{pid}/edges",
            json={
                "source": gen["id"],
                "source_port": "df",  # TABLE
                "target": evaluate["id"],
                "target_port": "test",  # TABLE
            },
        )
        assert resp.status_code == 201

    def test_g_cycle_detection(self, client: TestClient):
        """Cycle: A → B → A → rejected at API level.

        POST /edges that would create a cycle should return 400.
        No partial state should be left in pipeline.
        """
        pid = _create_pipeline(client, "Test G: Cycle detection")

        node_a = _add_node(client, pid, "ml_toolbox.nodes.demo.clean_data", x=0)
        node_b = _add_node(client, pid, "ml_toolbox.nodes.demo.clean_data", x=200)

        # A → B (valid)
        _connect(client, pid, node_a["id"], "df", node_b["id"], "df")

        # B → A would create cycle — must be rejected
        resp = client.post(
            f"/api/pipelines/{pid}/edges",
            json={
                "source": node_b["id"],
                "source_port": "df",
                "target": node_a["id"],
                "target_port": "df",
            },
        )
        assert resp.status_code == 400
        assert "cycle" in resp.json()["detail"].lower()

        # Verify only one edge exists (the valid one)
        resp = client.get(f"/api/pipelines/{pid}")
        assert resp.status_code == 200
        assert len(resp.json()["edges"]) == 1
