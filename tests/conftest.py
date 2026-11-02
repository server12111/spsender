from datetime import datetime, timezone

import pytest
from cryptography.fernet import Fernet

from app.config import Config
from app.db import Database
from app.security import SecretBox
from app.services import Engine
from app.transport.base import FakeNotificationSink, FakeTransport


class Clock:
    def __init__(self):
        self.now = datetime(2026, 10, 20, 9, tzinfo=timezone.utc).timestamp()

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


@pytest.fixture
def env(tmp_path):
    clock = Clock()
    config = Config(
        database_path=str(tmp_path / "test.sqlite3"), admin_ids=(123,), session_key=Fernet.generate_key().decode()
    )
    db = Database(config.database_path)
    db.migrate()
    engine = Engine(db, config, SecretBox(config.session_key), clock)
    return engine, clock, FakeTransport(), FakeNotificationSink()


def restart(engine):
    return Engine(Database(engine.db.path), engine.config, engine.secrets, engine.clock)


def setup_pair(engine, count=20, *, ready=True, running=True, greetings=False, extra=0):
    campaign_id = engine.create_campaign(
        "Основная", ["Текст один", "Текст два"], interval=300, chat_limit=count, greeting_enabled=greetings
    )
    a, _ = engine.add_account("Аккаунт A", "+380111111111", "session-a", 1001)
    b, _ = engine.add_account("Аккаунт B", "+380222222222", "session-b", 1002)
    pair = engine.db.one("SELECT * FROM pairs WHERE account_a_id=?", (a,))
    chats = [
        engine.add_chat(-100000 - i, f"Чат {i}", approved=True, greeting_allowed=greetings, join_allowed=True)
        for i in range(count + extra)
    ]
    if ready:
        with engine.db.tx() as c:
            c.execute(
                "UPDATE accounts SET onboarding_started_at=?,onboarding_stage='READY',prepared_at=?,joined_at=?,ready_at=?",
                (
                    engine.clock() - 28 * 3600,
                    engine.clock() - 4 * 3600,
                    engine.clock() - 3 * 3600,
                    engine.clock() - 3600,
                ),
            )
            c.execute("UPDATE account_chat_access SET joined=1,can_write=1")
    if running:
        engine.campaign_status(campaign_id, "RUNNING", confirmed=True)
    return campaign_id, pair["id"], a, b, chats


async def drain(engine, transport, limit=100):
    for _ in range(limit):
        if not await engine.run_one(transport):
            return
    raise AssertionError("Очередь не опустела")
