"""Temporary SQLite and mocked Telegram reads; network access is disabled."""

import asyncio
import json
import sys
import tempfile
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pytest_socket
from cryptography.fernet import Fernet
from telethon import types

from app.__main__ import build
from app.bot.interface import Interface
from app.config import Config
from app.db import Database
from app.services import Engine
from app.transport.telegram import TelegramTransport
from tools.benchmark import memory_usage


async def benchmark():
    total = 1000
    pytest_socket.disable_socket(allow_unix_socket=True)
    with tempfile.TemporaryDirectory() as directory:
        config = Config(
            database_path=str(Path(directory) / "import.sqlite3"),
            session_key=Fernet.generate_key().decode(),
            admin_ids=(123,),
            batch_interval=0,
        )
        e = build(config)
        e.config = config = replace(config, mode="live")
        ui = Interface(e)
        ui.set_state(123, "import_chats")
        await ui.message(123, txt_content="\n".join(f"@group_{n:05d}" for n in range(total)))
        form = ui.state(123)["data"]
        view = ui.import_confirmation(123, {**form, "approved": True, "join_allowed": True})
        button = next(
            callback
            for group in view.buttons
            for _, callback in group
            if e.db.scalar("SELECT action FROM ui_actions WHERE token=?", (callback[2:],)) == "import_confirm"
        )
        start, cpu = time.perf_counter(), time.process_time()
        await ui.callback(123, button)
        imported_seconds = time.perf_counter() - start
        import_cpu_seconds = time.process_time() - cpu
        assert e.db.scalar("SELECT count(*) FROM chat_targets") == total
        e = Engine(Database(config.database_path), config, e.secrets)
        assert e.db.scalar("SELECT count(*) FROM chat_targets") == total
        campaign = e.create_campaign("Mock", ["Mock"], chat_limit=20)
        e.add_account("Mock A", "+10000000001", "fake-session", 1001)
        e.add_account("Mock B", "+10000000002", "fake-session", 1002)
        with e.db.tx() as c:
            c.execute("UPDATE accounts SET onboarding_started_at=?", (e.clock() - 100000,))
        e.campaign_status(campaign, "RUNNING", confirmed=True)
        calls = 0

        async def get_entity(link):
            nonlocal calls
            calls += 1
            peer = int(link.rsplit("_", 1)[1]) + 30000
            return types.Chat(
                id=peer, title=link, photo=types.ChatPhotoEmpty(), participants_count=0, date=None, version=1
            )

        transport = TelegramTransport(e)

        async def client(account_id):
            return SimpleNamespace(get_entity=get_entity)

        transport.client = client
        start, cpu = time.perf_counter(), time.process_time()
        batches = 0
        while await transport.resolve_pending_targets("benchmark"):
            batches += 1
        resolve_seconds = time.perf_counter() - start
        resolved = e.db.scalar("SELECT count(*) FROM chat_targets WHERE status='RESOLVED'")
        assert resolved == total and calls == total and batches == total // 4
        assert e.db.scalar("SELECT count(*) FROM chat_pool") == total
        assert not e.db.scalar("SELECT count(*) FROM dispatch_jobs")
        assert not e.db.all("PRAGMA foreign_key_check")
        return {
            "chat_targets": total,
            "preserved_after_restart": total,
            "resolved": resolved,
            "batches": batches,
            "checks_per_batch": 4,
            "mock_reads": calls,
            "duplicates": 0,
            "real_telegram_operations": 0,
            "message_sends": 0,
            "import_seconds": round(imported_seconds, 3),
            "import_cpu_seconds": round(import_cpu_seconds, 3),
            "resolve_seconds": round(resolve_seconds, 3),
            "resolve_cpu_seconds": round(time.process_time() - cpu, 3),
            **memory_usage(),
        }


if __name__ == "__main__":
    print(json.dumps(asyncio.run(benchmark()), indent=2))
