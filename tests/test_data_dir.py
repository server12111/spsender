"""Persisted-state directory resolution.

Hosts such as Bothost keep only one directory across restarts and pass its path
in DATA_DIR. The database, media and logs must land inside it so a restart does
not wipe the data.
"""

from app import paths
from app.config import Config


def test_data_dir_prefers_env_override(monkeypatch):
    monkeypatch.setenv("DATA_DIR", "/persistent/store")
    assert str(paths.data_dir()) == "/persistent/store"
    assert paths.default_database_path() == "/persistent/store/fe_autosender.sqlite3"
    assert str(paths.media_dir()) == "/persistent/store/media"
    assert str(paths.logs_dir()) == "/persistent/store/logs"


def test_data_dir_falls_back_to_app_data_mount(monkeypatch):
    monkeypatch.delenv("DATA_DIR", raising=False)
    monkeypatch.setattr(paths.os.path, "isdir", lambda p: p == "/app/data")
    assert str(paths.data_dir()) == "/app/data"
    assert paths.default_database_path() == "/app/data/fe_autosender.sqlite3"


def test_data_dir_defaults_to_local_data(monkeypatch):
    monkeypatch.delenv("DATA_DIR", raising=False)
    monkeypatch.setattr(paths.os.path, "isdir", lambda p: False)
    assert str(paths.data_dir()) == "data"


def test_config_load_routes_database_into_data_dir(monkeypatch):
    monkeypatch.setenv("DATA_DIR", "/persistent/store")
    monkeypatch.delenv("DATABASE_PATH", raising=False)
    assert Config.load().database_path == "/persistent/store/fe_autosender.sqlite3"


def test_explicit_database_path_still_wins(monkeypatch):
    monkeypatch.setenv("DATA_DIR", "/persistent/store")
    monkeypatch.setenv("DATABASE_PATH", "/custom/elsewhere.sqlite3")
    assert Config.load().database_path == "/custom/elsewhere.sqlite3"
