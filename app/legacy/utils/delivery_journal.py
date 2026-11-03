"""Small durable journal for notifications delivered before the main DB marker.

Telegram delivery and the main SQLite database cannot be committed atomically.
This sidecar is deliberately independent from ``bot.db``: if that database is
temporarily locked or unavailable immediately after a successful send, a bot
restart can still see that the notification must not be sent again.

Concurrency scope: callers currently serialize each notification lifecycle
inside this one bot process.  ``contains() -> send -> remember()`` is not an
atomic cross-process reservation, so this journal must not be treated as an
exactly-once mechanism if the application is ever run with multiple workers.
That deployment needs a shared transactional outbox/reservation design.
"""

import asyncio
import sqlite3
from pathlib import Path


class DeliveryJournal:
    def __init__(self, database_path: str):
        path = Path(database_path)
        self.path = path.with_name(f"{path.stem}.delivery-journal.sqlite3")

    async def contains(self, key: str) -> bool:
        return await asyncio.to_thread(self._contains, key)

    async def remember(self, key: str) -> None:
        await asyncio.to_thread(self._remember, key)

    async def forget(self, key: str) -> None:
        await asyncio.to_thread(self._forget, key)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=1)
        connection.execute(
            "CREATE TABLE IF NOT EXISTS delivered_notifications "
            "(key TEXT PRIMARY KEY, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
        )
        return connection

    def _contains(self, key: str) -> bool:
        with self._connect() as connection:
            return connection.execute(
                "SELECT 1 FROM delivered_notifications WHERE key = ?", (key,)
            ).fetchone() is not None

    def _remember(self, key: str) -> None:
        with self._connect() as connection:
            connection.execute(
                "INSERT OR IGNORE INTO delivered_notifications (key) VALUES (?)", (key,)
            )

    def _forget(self, key: str) -> None:
        with self._connect() as connection:
            connection.execute("DELETE FROM delivered_notifications WHERE key = ?", (key,))
