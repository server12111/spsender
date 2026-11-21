"""Persistent database paths and container mount validation."""

import os
from pathlib import Path


class StorageError(ValueError):
    pass


def persistent_directory():
    configured = (os.getenv("DATA_DIR") or "").strip()
    if configured:
        return configured
    if os.name != "nt" and Path("/app/data").is_dir():
        return "/app/data"
    return None


def resolve_database_path():
    root = persistent_directory()
    if root:
        directory = Path(root)
        if directory.exists() and not directory.is_dir():
            raise StorageError("DATA_DIR должен указывать на папку, а не на существующий файл")
        return os.path.join(root, "fe_autosender.sqlite3")
    # Desktop compatibility only. A configured DATA_DIR always wins, including
    # over a stale DATABASE_PATH left in a hosting control panel.
    return (os.getenv("DATABASE_PATH") or "").strip() or os.path.join("data", "fe_autosender.sqlite3")


def storage_mount(database_path, mountinfo=None):
    """Return the deepest Linux mount covering the database, without host paths."""
    path = Path(database_path).resolve()
    if mountinfo is None:
        try:
            mountinfo = Path("/proc/self/mountinfo").read_text(encoding="utf-8")
        except OSError:
            return None
    result = None
    for line in mountinfo.splitlines():
        before, separator, after = line.partition(" - ")
        fields = before.split()
        if not separator or len(fields) < 5 or not after.split():
            continue
        mount = fields[4]
        for encoded, decoded in ((r"\040", " "), (r"\011", "\t"), (r"\134", "\\")):
            mount = mount.replace(encoded, decoded)
        root = Path(mount).resolve()
        if path.is_relative_to(root) and (result is None or len(str(root)) > len(result["path"])):
            result = {"path": str(root), "filesystem": after.split()[0]}
    return result


def validate_runtime_storage(config, *, in_container=None, mountinfo=None):
    if in_container is None:
        in_container = Path("/.dockerenv").exists()
    mount = storage_mount(config.database_path, mountinfo)
    if config.mode == "live" and in_container:
        if not mount or mount["path"] == str(Path("/").resolve()) or mount["filesystem"] in {"tmpfs", "ramfs"}:
            raise StorageError(
                "Для live в контейнере нужен постоянный том с базой. Подключите хранилище /app/data в Bothost или используйте именованный том Docker Compose. Запуск отменён, чтобы данные снова не потерялись."
            )
    return mount
