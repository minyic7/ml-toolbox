"""Tests for file store service."""

import time

from ml_toolbox.services import file_store


def test_make_run_dir():
    run_dir = file_store.make_run_dir("p1", "run-1")
    assert run_dir.exists()
    assert run_dir.name == "run-1"
    assert "p1" in str(run_dir)
    assert "runs" in str(run_dir)


def test_list_runs_empty():
    assert file_store.list_runs("p1") == []


def test_list_runs_sorted():
    file_store.make_run_dir("p1", "run-a")
    time.sleep(0.05)
    file_store.make_run_dir("p1", "run-b")

    runs = file_store.list_runs("p1")
    assert len(runs) == 2
    assert runs[0]["run_id"] == "run-b"
    assert runs[1]["run_id"] == "run-a"
    assert "created_at" in runs[0]


def test_delete_run():
    run_dir = file_store.make_run_dir("p1", "run-1")
    assert run_dir.exists()
    file_store.delete_run("p1", "run-1")
    assert not run_dir.exists()


def test_get_output_path():
    path = file_store.get_output_path("p1", "run-1", "node-a", "csv")
    assert path.name == "node-a.csv"
    assert "run-1" in str(path)


def test_output_exists():
    assert not file_store.output_exists("p1", "run-1", "node-a")
    run_dir = file_store.make_run_dir("p1", "run-1")
    (run_dir / "node-a.csv").write_text("data")
    assert file_store.output_exists("p1", "run-1", "node-a")


def test_get_latest_run_id():
    assert file_store.get_latest_run_id("p1") is None
    file_store.make_run_dir("p1", "run-a")
    time.sleep(0.05)
    file_store.make_run_dir("p1", "run-b")
    assert file_store.get_latest_run_id("p1") == "run-b"


def test_cleanup_run_dir():
    run_dir = file_store.make_run_dir("p1", "run-1")
    assert run_dir.exists()
    file_store.cleanup_run_dir("p1", "run-1")
    assert not run_dir.exists()


def test_cleanup_run_dir_nonexistent():
    # Should not raise
    file_store.cleanup_run_dir("p1", "nonexistent")
