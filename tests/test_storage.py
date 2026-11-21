import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from app.__main__ import build
from app.db import Database
from app.services import Engine
from app.storage import (
    StorageError,
    resolve_database_path,
    storage_mount,
    validate_runtime_storage,
)


def old_engine(e, path):
    db = Database(str(path))
    db.migrate()
    return Engine(db, replace(e.config, database_path=str(path)), e.secrets, e.clock)


@pytest.mark.parametrize("legacy", ["data/live.sqlite3", "/app/data", "/wrong/path.sqlite3", ""])
def test_only_data_dir_selects_fixed_database_and_ignores_legacy_path(monkeypatch, tmp_path, legacy):
    root = tmp_path / "persistent"
    monkeypatch.setenv("DATA_DIR", str(root))
    monkeypatch.setenv("DATABASE_PATH", legacy)
    assert Path(resolve_database_path()) == root / "fe_autosender.sqlite3"
    assert not root.exists()


def test_data_dir_cannot_be_an_existing_file(monkeypatch, tmp_path):
    path = tmp_path / "not-a-directory"
    path.write_bytes(b"keep")
    monkeypatch.setenv("DATA_DIR", str(path))
    with pytest.raises(StorageError):
        resolve_database_path()
    assert path.read_bytes() == b"keep"


def test_rebuild_with_different_workdir_keeps_accounts_and_campaigns(env, monkeypatch, tmp_path):
    e, _, _, _ = env
    root = tmp_path / "volume"
    monkeypatch.setenv("DATA_DIR", str(root))
    monkeypatch.setenv("DATABASE_PATH", "data/live.sqlite3")
    first_dir, second_dir = tmp_path / "image-a", tmp_path / "image-b"
    first_dir.mkdir()
    second_dir.mkdir()
    monkeypatch.chdir(first_dir)
    first = old_engine(e, resolve_database_path())
    campaign = first.create_campaign("Saved campaign", ["Text"])
    account, _ = first.add_account("Saved account", "+380111111111", "saved-session", 1001)
    encrypted = first.db.scalar("SELECT encrypted_session FROM account_sessions WHERE account_id=?", (account,))
    monkeypatch.chdir(second_dir)
    second = old_engine(e, resolve_database_path())
    assert second.db.scalar("SELECT name FROM campaigns WHERE id=?", (campaign,)) == "Saved campaign"
    assert (
        second.secrets.decrypt(
            second.db.scalar("SELECT encrypted_session FROM account_sessions WHERE account_id=?", (account,))
        )
        == "saved-session"
    )
    assert (
        second.db.scalar("SELECT encrypted_session FROM account_sessions WHERE account_id=?", (account,)) == encrypted
    )


def test_invalid_session_key_never_creates_an_empty_database(env, tmp_path):
    e, _, _, _ = env
    target = tmp_path / "missing" / "empty.sqlite3"
    with pytest.raises(ValueError):
        build(replace(e.config, database_path=str(target), session_key="bad"))
    assert not target.exists()


def test_desktop_path_without_data_dir_keeps_existing_behavior(monkeypatch):
    monkeypatch.delenv("DATA_DIR", raising=False)
    monkeypatch.setattr("app.storage.persistent_directory", lambda: None)
    monkeypatch.setenv("DATABASE_PATH", "data/custom.sqlite3")
    assert resolve_database_path() == "data/custom.sqlite3"


ROOT_MOUNT = "1 0 0:1 / / rw - overlay overlay rw\n"
PARENT_MOUNT = ROOT_MOUNT + "2 1 0:2 /bot/project /app rw - ext4 /dev/sda rw\n"
DATA_MOUNT = PARENT_MOUNT + "3 2 0:3 /volume /app/data rw - ext4 /dev/sdb rw\n"


@pytest.mark.parametrize("mountinfo,expected", [(PARENT_MOUNT, "/app"), (DATA_MOUNT, "/app/data")])
def test_live_accepts_hosting_parent_bind_or_named_data_volume(env, mountinfo, expected):
    e, _, _, _ = env
    config = replace(e.config, mode="live", database_path="/app/data/bot.sqlite3")
    result = validate_runtime_storage(config, in_container=True, mountinfo=mountinfo)
    assert result["path"] == str(Path(expected).resolve())


@pytest.mark.parametrize("mountinfo", [ROOT_MOUNT, "", ROOT_MOUNT + "2 1 0:2 / /app/data rw - tmpfs tmpfs rw\n"])
def test_live_refuses_container_layer_or_memory_without_persistent_volume(env, mountinfo):
    e, _, _, _ = env
    config = replace(e.config, mode="live", database_path="/app/data/bot.sqlite3")
    with pytest.raises(StorageError, match="постоянный том"):
        validate_runtime_storage(config, in_container=True, mountinfo=mountinfo)


def test_local_desktop_does_not_require_container_mount(env):
    e, _, _, _ = env
    assert validate_runtime_storage(replace(e.config, mode="live"), in_container=False, mountinfo="") is None


def test_other_prefix_is_not_mistaken_for_data_mount():
    info = ROOT_MOUNT + "2 1 0:2 /bot /app/other rw - ext4 /dev/sda rw\n"
    assert storage_mount("/app/data/bot.sqlite3", info)["path"] == str(Path("/").resolve())


def test_startup_stops_before_creating_database_without_volume(env, monkeypatch, tmp_path):
    from app import __main__ as entry

    e, _, _, _ = env
    target = tmp_path / "must-not-exist.sqlite3"
    config = replace(e.config, mode="live", database_path=str(target))
    monkeypatch.setattr(entry.Config, "load", lambda: config)
    monkeypatch.setattr(entry, "configure_logging", lambda *args: None)

    def unavailable(*args):
        raise StorageError("persistent volume missing")

    monkeypatch.setattr(entry, "validate_runtime_storage", unavailable)
    monkeypatch.setattr("sys.argv", ["app", "run"])
    with pytest.raises(StorageError):
        entry.main()
    assert not target.exists()


def test_docker_uses_host_storage_without_anonymous_image_volume():
    root = Path(__file__).resolve().parents[1]
    instructions = [
        line.strip()
        for line in (root / "Dockerfile").read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    ]
    assert not any(line.upper().startswith("VOLUME ") for line in instructions)
    assert "ENV DATA_DIR=/app/data" in instructions
    compose = (root / "compose.yaml").read_text()
    assert "autosender_data:/app/data" in compose
    assert "DATA_DIR: /app/data" in compose
    assert "DATABASE_PATH:" not in compose


def test_database_path_cannot_be_a_directory(tmp_path):
    with pytest.raises(ValueError, match="файл SQLite"):
        Database(str(tmp_path))


def test_sqlite_cantopen_keeps_error_code_and_reports_exact_path(monkeypatch, tmp_path):
    path = tmp_path / "database.sqlite3"
    db = Database(str(path))
    error = sqlite3.OperationalError("unable to open database file")
    error.sqlite_errorcode = sqlite3.SQLITE_CANTOPEN

    def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr("app.db.database.sqlite3.connect", fail)
    with pytest.raises(sqlite3.OperationalError) as caught:
        db.connect()
    assert caught.value.sqlite_errorcode == sqlite3.SQLITE_CANTOPEN
    assert str(path) in " ".join(caught.value.__notes__)
    assert "SQLite file=" in " ".join(caught.value.__notes__)
    assert not path.exists()
