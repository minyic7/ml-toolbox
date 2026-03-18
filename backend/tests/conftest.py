import pytest


@pytest.fixture(autouse=True)
def _isolate_data_dir(tmp_path, monkeypatch):
    """Point both stores at a temporary directory for every test."""
    projects = tmp_path / "projects"
    monkeypatch.setattr("ml_toolbox.services.store.PROJECTS_DIR", projects)
    monkeypatch.setattr("ml_toolbox.services.file_store.PROJECTS_DIR", projects)
