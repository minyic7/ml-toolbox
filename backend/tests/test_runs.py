"""Tests for GET /api/runs global endpoint."""

import json
from pathlib import Path

import pytest


def _make_run(tmp_projects: Path, pipeline_id: str, run_id: str, status: str = "done", nodes=None):
    """Create a fake run directory with _status.json and optional node outputs."""
    run_dir = tmp_projects / pipeline_id / "runs" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    status_data = {"status": status, "run_id": run_id}
    (run_dir / "_status.json").write_text(json.dumps(status_data))

    # Write fake node outputs
    if nodes:
        for node_id, ext in nodes:
            (run_dir / f"{node_id}_output.{ext}").write_bytes(b"fake data")

    return run_dir


def _make_pipeline(tmp_projects: Path, pipeline_id: str, name: str, nodes=None):
    """Create a pipeline.json on disk."""
    pipeline_dir = tmp_projects / pipeline_id
    pipeline_dir.mkdir(parents=True, exist_ok=True)

    node_defs = []
    if nodes:
        for node_id, node_type in nodes:
            node_defs.append({
                "id": node_id,
                "type": node_type,
                "position": {"x": 0, "y": 0},
                "params": {},
                "code": "",
                "inputs": [],
                "outputs": [],
            })

    data = {
        "id": pipeline_id,
        "name": name,
        "settings": {"keep_outputs": True},
        "nodes": node_defs,
        "edges": [],
    }
    (pipeline_dir / "pipeline.json").write_text(json.dumps(data))
    return data


class TestGlobalRunsEndpoint:
    def test_empty_returns_empty_list(self, client):
        resp = client.get("/api/runs")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_returns_runs_across_pipelines(self, client, tmp_path):
        projects = tmp_path / "projects"
        _make_pipeline(projects, "p1", "Pipeline One", nodes=[("n1", "demo.run")])
        _make_pipeline(projects, "p2", "Pipeline Two", nodes=[("n2", "demo.clean")])
        _make_run(projects, "p1", "run-aaa", "done", nodes=[("n1", "parquet")])
        _make_run(projects, "p2", "run-bbb", "error")

        resp = client.get("/api/runs")
        assert resp.status_code == 200
        runs = resp.json()
        assert len(runs) == 2

        run_ids = {r["id"] for r in runs}
        assert "run-aaa" in run_ids
        assert "run-bbb" in run_ids

        # Verify shape
        for run in runs:
            assert "id" in run
            assert "pipeline_id" in run
            assert "pipeline_name" in run
            assert "status" in run
            assert "started_at" in run
            assert "completed_at" in run
            assert "duration" in run
            assert "dag_snapshot" in run
            assert "artifacts" in run

    def test_filter_by_pipeline_id(self, client, tmp_path):
        projects = tmp_path / "projects"
        _make_pipeline(projects, "p1", "Pipeline One")
        _make_pipeline(projects, "p2", "Pipeline Two")
        _make_run(projects, "p1", "run-aaa", "done")
        _make_run(projects, "p2", "run-bbb", "done")

        resp = client.get("/api/runs", params={"pipeline_id": "p1"})
        runs = resp.json()
        assert len(runs) == 1
        assert runs[0]["pipeline_id"] == "p1"

    def test_filter_by_status(self, client, tmp_path):
        projects = tmp_path / "projects"
        _make_pipeline(projects, "p1", "Pipeline One")
        _make_run(projects, "p1", "run-ok", "done")
        _make_run(projects, "p1", "run-fail", "error")
        _make_run(projects, "p1", "run-cancel", "cancelled")

        resp = client.get("/api/runs", params={"status": "error"})
        runs = resp.json()
        assert len(runs) == 1
        assert runs[0]["status"] == "error"
        assert runs[0]["id"] == "run-fail"

    def test_search_by_run_id_prefix(self, client, tmp_path):
        projects = tmp_path / "projects"
        _make_pipeline(projects, "p1", "Pipeline One")
        _make_run(projects, "p1", "abc123", "done")
        _make_run(projects, "p1", "abc456", "done")
        _make_run(projects, "p1", "def789", "done")

        resp = client.get("/api/runs", params={"search": "abc"})
        runs = resp.json()
        assert len(runs) == 2
        assert all(r["id"].startswith("abc") for r in runs)

    def test_pagination_limit_offset(self, client, tmp_path):
        projects = tmp_path / "projects"
        _make_pipeline(projects, "p1", "Pipeline One")
        for i in range(5):
            _make_run(projects, "p1", f"run-{i:03d}", "done")

        # Limit
        resp = client.get("/api/runs", params={"limit": 2})
        assert len(resp.json()) == 2

        # Offset
        resp = client.get("/api/runs", params={"limit": 2, "offset": 3})
        assert len(resp.json()) == 2

        # Past end
        resp = client.get("/api/runs", params={"offset": 100})
        assert len(resp.json()) == 0

    def test_dag_snapshot_reflects_node_status(self, client, tmp_path):
        projects = tmp_path / "projects"
        _make_pipeline(
            projects, "p1", "Pipeline One",
            nodes=[("n1", "demo.run"), ("n2", "demo.clean")],
        )
        run_dir = _make_run(
            projects, "p1", "run-snap", "done",
            nodes=[("n1", "parquet")],
        )
        # n2 has an error
        (run_dir / "n2_manifest_error.json").write_text(
            json.dumps({"error": "boom"})
        )

        resp = client.get("/api/runs")
        runs = resp.json()
        assert len(runs) == 1
        snap = runs[0]["dag_snapshot"]
        assert len(snap) == 2

        n1_snap = next(s for s in snap if s["node_id"] == "n1")
        assert n1_snap["status"] == "done"
        assert n1_snap["node_type"] == "demo.run"
        assert n1_snap["node_name"] == "run"

        n2_snap = next(s for s in snap if s["node_id"] == "n2")
        assert n2_snap["status"] == "error"

    def test_artifacts_collected(self, client, tmp_path):
        projects = tmp_path / "projects"
        _make_pipeline(
            projects, "p1", "Pipeline One",
            nodes=[("n1", "demo.run")],
        )
        _make_run(
            projects, "p1", "run-art", "done",
            nodes=[("n1", "parquet")],
        )

        resp = client.get("/api/runs")
        runs = resp.json()
        assert len(runs) == 1
        artifacts = runs[0]["artifacts"]
        assert len(artifacts) == 1
        art = artifacts[0]
        assert art["node_id"] == "n1"
        assert art["filename"] == "n1_output.parquet"
        assert art["type"] == "parquet"
        assert art["size"] > 0

    def test_multiple_artifact_types(self, client, tmp_path):
        projects = tmp_path / "projects"
        _make_pipeline(
            projects, "p1", "Pipeline One",
            nodes=[("n1", "demo.run")],
        )
        run_dir = _make_run(projects, "p1", "run-multi", "done")
        # Create multiple output types
        (run_dir / "n1_output.parquet").write_bytes(b"parquet data")
        (run_dir / "n1_chart.png").write_bytes(b"png data")
        (run_dir / "n1_model.pkl").write_bytes(b"pkl data")
        # This should be excluded (metadata)
        (run_dir / "n1_manifest.json").write_text("{}")
        (run_dir / "n1.hash").write_text("abc")
        (run_dir / "n1_logs.txt").write_text("log")

        resp = client.get("/api/runs")
        artifacts = resp.json()[0]["artifacts"]
        types = {a["type"] for a in artifacts}
        assert "parquet" in types
        assert "png" in types
        assert "pkl" in types
        # Metadata files should not appear
        filenames = {a["filename"] for a in artifacts}
        assert "n1_manifest.json" not in filenames
        assert "n1.hash" not in filenames
        assert "n1_logs.txt" not in filenames

    def test_combined_filters(self, client, tmp_path):
        projects = tmp_path / "projects"
        _make_pipeline(projects, "p1", "Pipeline One")
        _make_pipeline(projects, "p2", "Pipeline Two")
        _make_run(projects, "p1", "abc-run1", "done")
        _make_run(projects, "p1", "abc-run2", "error")
        _make_run(projects, "p2", "abc-run3", "done")

        resp = client.get("/api/runs", params={
            "pipeline_id": "p1",
            "status": "done",
            "search": "abc",
        })
        runs = resp.json()
        assert len(runs) == 1
        assert runs[0]["id"] == "abc-run1"

    def test_pipeline_name_included(self, client, tmp_path):
        projects = tmp_path / "projects"
        _make_pipeline(projects, "p1", "My Cool Pipeline")
        _make_run(projects, "p1", "run-1", "done")

        resp = client.get("/api/runs")
        assert resp.json()[0]["pipeline_name"] == "My Cool Pipeline"

    def test_run_without_status_file(self, client, tmp_path):
        """Runs without _status.json should still appear with 'unknown' status."""
        projects = tmp_path / "projects"
        _make_pipeline(projects, "p1", "Pipeline One")
        run_dir = projects / "p1" / "runs" / "orphan-run"
        run_dir.mkdir(parents=True)

        resp = client.get("/api/runs")
        runs = resp.json()
        assert len(runs) == 1
        assert runs[0]["status"] == "unknown"
        assert runs[0]["completed_at"] is None
        assert runs[0]["duration"] is None

    def test_custom_node_name_in_snapshot(self, client, tmp_path):
        """Nodes with custom names should use those in the snapshot."""
        projects = tmp_path / "projects"
        pid_dir = projects / "p1"
        pid_dir.mkdir(parents=True)
        data = {
            "id": "p1",
            "name": "Test",
            "settings": {},
            "nodes": [{
                "id": "n1",
                "type": "demo.run",
                "name": "My Custom Node",
                "position": {"x": 0, "y": 0},
                "params": {},
                "code": "",
                "inputs": [],
                "outputs": [],
            }],
            "edges": [],
        }
        (pid_dir / "pipeline.json").write_text(json.dumps(data))
        _make_run(projects, "p1", "run-named", "done")

        resp = client.get("/api/runs")
        snap = resp.json()[0]["dag_snapshot"]
        assert snap[0]["node_name"] == "My Custom Node"
