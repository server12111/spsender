import argparse
import asyncio
import json
import sys
import tempfile
from pathlib import Path

from cryptography.fernet import Fernet

from app.config import Config
from app.db import Database
from app.security import SecretBox
from app.security.secrets import configure_logging
from app.services import Engine


def build(config):
    db = Database(config.database_path, config.sqlite_busy_ms)
    db.migrate()
    if not config.session_key:
        raise ValueError("Задайте SESSION_KEY (python -m app keygen)")
    return Engine(db, config, SecretBox(config.session_key))


async def smoke():
    from app.scheduler.worker import Worker
    from app.transport.base import FakeNotificationSink, FakeTransport

    with tempfile.TemporaryDirectory() as directory:
        cfg = Config(
            database_path=str(Path(directory) / "smoke.sqlite3"),
            session_key=Fernet.generate_key().decode(),
            admin_ids=(1,),
        )
        e = build(cfg)
        campaign = e.create_campaign("Проверка запуска", ["Тестовое сообщение"])
        e.add_account("Тест A", "+10000000001", "fake", 1001)
        e.add_account("Тест B", "+10000000002", "fake", 1002)
        e.add_chat(-100001, "Тестовый чат", approved=True)
        e.campaign_status(campaign, "RUNNING", confirmed=True)
        transport, sink = FakeTransport(), FakeNotificationSink()
        await Worker(e, transport, sink).run_once()
        assert not transport.calls, "Нарушен тихий период"
        print(
            json.dumps(
                {"status": "ok", "database": "SQLite", "real_operations": 0, "silent_period": "ok"}, ensure_ascii=False
            )
        )


def main():
    if sys.platform == "win32":
        for stream in (sys.stdout, sys.stderr):
            if hasattr(stream, "reconfigure"):
                stream.reconfigure(encoding="utf-8", errors="backslashreplace")
    parser = argparse.ArgumentParser(description="FE AutoSender 2.0")
    parser.add_argument("command", choices=("keygen", "migrate", "check", "smoke", "run", "worker"))
    args = parser.parse_args()
    configure_logging()
    if args.command == "keygen":
        print(Fernet.generate_key().decode())
        return
    if args.command == "smoke":
        asyncio.run(smoke())
        return
    config = Config.load()
    if args.command == "migrate":
        Database(config.database_path).migrate()
        print("Миграции SQLite применены")
        return
    engine = build(config)
    if args.command == "check":
        print("SQLite: " + engine.db.scalar("PRAGMA integrity_check"))
        print("Режим: " + config.mode)
        print("Миграций: " + str(engine.db.scalar("SELECT count(*) FROM schema_migrations")))
        return
    from app.bot.runtime import serve
    from app.transport.base import FakeTransport

    if config.mode == "live":
        from app.transport.telegram import TelegramTransport

        transport = TelegramTransport(engine)
    else:
        transport = FakeTransport()
    try:
        asyncio.run(serve(engine, transport, args.command == "worker"))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
