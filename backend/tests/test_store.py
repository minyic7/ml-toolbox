"""Tests for pipeline store service."""

from ml_toolbox.services import store


def test_save_and_load_roundtrip():
    data = {"id": "p1", "name": "My Pipeline", "nodes": []}
    store.save("p1", data)
    loaded = store.load("p1")
    assert loaded == data


def test_load_missing_raises():
    import pytest

    with pytest.raises(FileNotFoundError):
        store.load("nonexistent")


def test_exists():
    assert not store.exists("p1")
    store.save("p1", {"id": "p1"})
    assert store.exists("p1")


def test_list_all_empty():
    assert store.list_all() == []


def test_list_all_sorted_by_mtime(tmp_path):
    import time

    store.save("older", {"id": "older"})
    time.sleep(0.05)
    store.save("newer", {"id": "newer"})

    result = store.list_all()
    assert len(result) == 2
    assert result[0]["id"] == "newer"
    assert result[1]["id"] == "older"


def test_delete():
    store.save("p1", {"id": "p1"})
    assert store.exists("p1")
    store.delete("p1")
    assert not store.exists("p1")
