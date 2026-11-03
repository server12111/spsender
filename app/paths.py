"""Where the bot keeps its persisted state.

Some hosts preserve exactly one directory across container restarts and wipe
everything else (for example Bothost keeps only ``/app/data``). Point every
durable artefact — the SQLite database, uploaded media and the diagnostics log
— at that directory so a restart does not lose data.

Resolution order:
1. ``DATA_DIR`` environment variable, when the host sets one;
2. ``/app/data`` when it exists (the Bothost persistent mount);
3. ``./data`` for local development and tests.
"""

import os
from pathlib import Path


def data_dir() -> Path:
    override = os.getenv("DATA_DIR")
    if override:
        return Path(override)
    if os.path.isdir("/app/data"):
        return Path("/app/data")
    return Path("data")


def media_dir() -> Path:
    return data_dir() / "media"


def logs_dir() -> Path:
    return data_dir() / "logs"


def default_database_path() -> str:
    return str(data_dir() / "fe_autosender.sqlite3")
