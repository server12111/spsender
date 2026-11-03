import asyncio
import contextvars
import functools
import os
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path

from app.async_tools import settle_cancelled


class Database:
    """Короткие транзакции BEGIN IMMEDIATE сериализуют запись разных процессов.

    Сетевые операции всегда выполняются после COMMIT. Каждый вызов получает
    собственное соединение; SQLite WAL допускает параллельные чтения.
    """

    def __init__(self, path: str, busy_ms=250):
        self.path = str(Path(path).resolve())
        self.busy_ms = busy_ms
        self._async_slots = asyncio.Semaphore(8)
        parent = Path(self.path).parent
        created = not parent.exists()
        parent.mkdir(parents=True, exist_ok=True)
        if os.name != "nt" and created:
            os.chmod(parent, 0o700)

    def connect(self):
        connection = sqlite3.connect(self.path, timeout=self.busy_ms / 1000, isolation_level=None)
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute(f"PRAGMA busy_timeout={int(self.busy_ms)}")
            connection.execute("PRAGMA synchronous=FULL")
            if os.name != "nt":
                os.chmod(self.path, 0o600)
            return connection
        except BaseException:
            connection.close()
            raise

    @contextmanager
    def read(self):
        connection = self.connect()
        try:
            connection.execute("BEGIN")
            yield connection
        finally:
            connection.rollback()
            connection.close()

    async def run(self, function, *args, **kwargs):
        """Bounded DB work outside the event loop; retry only SQLite lock failures.

        Each callable must be transactional/idempotent. Cancellation waits for an
        already executing DB call, preventing orphaned mutations after shutdown.
        """
        async with self._async_slots:
            for attempt in range(5):
                # An executor Future also survives asyncio.run() cancelling all
                # Tasks during shutdown. Keep the caller's mailing context.
                task = asyncio.get_running_loop().run_in_executor(
                    None, contextvars.copy_context().run, functools.partial(function, *args, **kwargs)
                )
                try:
                    return await asyncio.shield(task)
                except asyncio.CancelledError:
                    await settle_cancelled(task)
                    raise
                except sqlite3.OperationalError as exc:
                    if not self.is_busy(exc) or attempt == 4:
                        raise
                    await asyncio.sleep(0.025 * 2**attempt)

    @staticmethod
    def is_busy(exc):
        return isinstance(exc, sqlite3.OperationalError) and (
            "locked" in str(exc).lower() or "busy" in str(exc).lower()
        )

    @contextmanager
    def tx(self):
        connection = self.connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def migrate(self):
        connection = self.connect()
        try:
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA synchronous=FULL")
            connection.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at REAL)"
            )
        finally:
            connection.close()
        for file in sorted(Path(__file__).with_name("migrations").glob("*.sql")):
            with self.tx() as c:
                if c.execute("SELECT 1 FROM schema_migrations WHERE version=?", (file.stem,)).fetchone():
                    continue
                # Individual statements preserve the transaction, unlike executescript.
                statement = ""
                for line in file.read_text(encoding="utf-8").splitlines(keepends=True):
                    statement += line
                    if sqlite3.complete_statement(statement):
                        c.execute(statement)
                        statement = ""
                if statement.strip():
                    raise ValueError(f"Незавершённая SQL миграция: {file.name}")
                c.execute("INSERT INTO schema_migrations VALUES (?,?)", (file.stem, time.time()))

    def all(self, sql, args=()):
        c = self.connect()
        try:
            return [dict(row) for row in c.execute(sql, args).fetchall()]
        finally:
            c.close()

    def one(self, sql, args=()):
        c = self.connect()
        try:
            result = c.execute(sql, args).fetchone()
            return dict(result) if result else None
        finally:
            c.close()

    def scalar(self, sql, args=()):
        row = self.one(sql, args)
        return next(iter(row.values())) if row else None
