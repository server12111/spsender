"""Load the transferred delivery pipeline with native-client doubles, never Telegram."""

import argparse
import asyncio
import json
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cryptography.fernet import Fernet
from telethon import types

from app.__main__ import build
from app.config import Config
from app.db import Database
from app.legacy.clock import engine_context
from app.legacy.mailing import mailing_context, target_context
from app.legacy.runtime import LegacyRuntime
from app.services import Engine
from tools.benchmark import memory_usage, seed


class Probe:
    def __init__(self):
        self.keys = set()
        self.duplicates = 0
        self.active = 0
        self.max_active = 0


class NativeClient:
    def __init__(self, account_id, probe):
        self.account_id, self.probe = account_id, probe

    def is_connected(self):
        return True

    async def get_input_entity(self, peer):
        return peer

    async def get_entity(self, peer):
        if isinstance(peer, str):
            return types.InputPeerChat(abs(int(peer)))
        return peer

    async def get_permissions(self, *args):
        return SimpleNamespace(has_left=False, is_admin=True)

    async def __call__(self, request):
        return True

    async def send_message(self, peer, text, **kwargs):
        context = target_context.get()
        key = (context["id"], context["delivery_seq"])
        if key in self.probe.keys:
            self.probe.duplicates += 1
        self.probe.active += 1
        self.probe.max_active = max(self.probe.max_active, self.probe.active)
        try:
            await asyncio.sleep(0.02)
            self.probe.keys.add(key)
            return SimpleNamespace(id=len(self.probe.keys))
        finally:
            self.probe.active -= 1


async def run(engine, accounts, chats, count):
    probe = Probe()

    def runtime_for(e):
        async def client(account_id):
            return NativeClient(account_id, probe)

        async def remember(*args):
            return None

        engine_context.set(e)
        return LegacyRuntime(e, SimpleNamespace(client=client, _remember_peer=remember), "benchmark")

    runtime = runtime_for(engine)
    warm_start = time.perf_counter()
    candidates = await runtime.database.get_user_accounts(engine.config.admin_ids[0])
    for _ in range(100):
        for account in candidates:
            await runtime.database.get_warmup(account.id)
    warm_seconds = time.perf_counter() - warm_start
    work = [(pair, index) for index in range(chats) for pair in range(1, accounts // 2 + 1)][:count]
    lags = []
    running = True

    async def heartbeat():
        previous = time.perf_counter()
        while running:
            await asyncio.sleep(0.01)
            now = time.perf_counter()
            lags.append(max(0, now - previous - 0.01))
            previous = now

    beat = asyncio.create_task(heartbeat())
    cpu, wall = time.process_time(), time.perf_counter()
    restarted = False
    locks = {pair: asyncio.Lock() for pair, _ in work}

    async def deliver(pair, index):
        async with locks[pair]:
            token = mailing_context.set(pair)
            try:
                mailing = await runtime.mailing.db.get_mailing(pair)
                targets = await runtime.mailing.db.get_mailing_targets(pair)
                target = targets[index]
                messages = await runtime.mailing.db.get_mailing_messages(pair)
                message = messages[target.msg_index % len(messages)]
                runtime.mailing._target_next_msg[target.id] = (target.msg_index % len(messages)) + 1
                client = await runtime.get_client(mailing.account_id)
                await runtime.mailing._send_msg(
                    client,
                    target.chat_identifier,
                    message,
                    None,
                    account_id=mailing.account_id,
                    mailing_id=pair,
                    target_snapshot=target,
                )
                runtime.mailing._record_sent(target.id, mailing.account_id, pair)
            finally:
                mailing_context.reset(token)

    try:
        for start in range(0, len(work), 4):
            await asyncio.gather(*(deliver(pair, index) for pair, index in work[start : start + 4]))
            if not restarted and start + 4 >= len(work) // 2:
                engine = Engine(Database(engine.db.path), engine.config, engine.secrets, engine.clock)
                runtime = runtime_for(engine)
                restarted = True
    finally:
        running = False
        await beat
    return engine, {
        "confirmed": len(probe.keys),
        "duplicates": probe.duplicates,
        "max_concurrency": probe.max_active,
        "restart_performed": restarted,
        "wall_seconds": round(time.perf_counter() - wall, 4),
        "cpu_seconds": round(time.process_time() - cpu, 4),
        "event_loop_lag_max_ms": round(max(lags, default=0) * 1000, 3),
        "warmup_candidate_checks": 100 * len(candidates),
        "warmup_candidate_seconds": round(warm_seconds, 4),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--accounts", type=int, default=1000)
    parser.add_argument("--chats", type=int, default=20)
    parser.add_argument("--send", type=int, default=1000)
    parser.add_argument("--output", default="docs/legacy-load-results.json")
    args = parser.parse_args()
    if (
        args.accounts < 4
        or args.accounts % 2
        or args.chats < 1
        or not 2 <= args.send <= args.accounts // 2 * args.chats
    ):
        parser.error("Use an even account count >=4 and a send count within the chat set")
    with tempfile.TemporaryDirectory(prefix="fe-legacy-load-") as directory:
        engine = build(
            Config(
                database_path=str(Path(directory) / "load.sqlite3"),
                session_key=Fernet.generate_key().decode(),
                admin_ids=(123,),
            )
        )
        total = seed(engine, args.accounts, args.chats, 0)
        engine, processing = asyncio.run(run(engine, args.accounts, args.chats, args.send))
        report = {
            "accounts": args.accounts,
            "chats": total,
            "processing": processing,
            "sqlite_integrity": engine.db.scalar("PRAGMA integrity_check"),
            "sqlite_fk_errors": len(engine.db.all("PRAGMA foreign_key_check")),
            "confirmed_results": engine.db.scalar("SELECT count(*) FROM message_results"),
            "memory": memory_usage(),
            "real_telegram_operations": 0,
            "pipeline": "original MailingService delivery with guarded native-client doubles",
            "app_version": __import__("app").__version__,
        }
        assert processing["duplicates"] == 0 and processing["confirmed"] == args.send
        assert (
            report["confirmed_results"] == args.send
            and report["sqlite_integrity"] == "ok"
            and not report["sqlite_fk_errors"]
        )
        Path(args.output).write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
