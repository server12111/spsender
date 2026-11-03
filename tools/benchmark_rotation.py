"""Large rotation preparation: temporary SQLite, simulated clock, mock RPC only."""

import argparse
import asyncio
import json
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from cryptography.fernet import Fernet

from app.__main__ import build
from app.config import Config
from app.db import Database
from app.scheduler.worker import Worker
from app.services import Engine
from app.transport.base import FakeNotificationSink
from tools.benchmark import ProbeTransport, memory_usage, seed


async def run(e, clock, operations, planning_times):
    api = ProbeTransport()
    worker = Worker(e, api, FakeNotificationSink())
    start = time.perf_counter()
    cpu_start = time.process_time()
    restarted = False
    while len(api.keys) < operations:
        worked = await worker.run_batch(max_jobs=min(32, operations - len(api.keys)))
        clock[0] += 76
        before = time.perf_counter()
        e.tick()
        planning_times.append(time.perf_counter() - before)
        if not restarted and len(api.keys) >= operations // 2:
            e = Engine(Database(e.db.path), e.config, e.secrets, e.clock)
            worker = Worker(e, api, FakeNotificationSink())
            restarted = True
        if not worked and not e.db.scalar("SELECT count(*) FROM dispatch_jobs WHERE status='PENDING'"):
            raise AssertionError("No pending operations")
    return e, {
        "confirmed_jobs": len(api.keys),
        "duplicates": api.duplicates,
        "max_concurrency": api.max_active,
        "restart_performed": restarted,
        "wall_seconds": round(time.perf_counter() - start, 4),
        "cpu_seconds": round(time.process_time() - cpu_start, 4),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--accounts", type=int, default=1000)
    parser.add_argument("--chats", type=int, default=20)
    parser.add_argument("--operations", type=int, default=400)
    parser.add_argument("--output", default="docs/rotation-load-results.json")
    args = parser.parse_args()
    if args.accounts < 4 or args.accounts % 2 or args.chats < 1 or args.operations < 2:
        parser.error("Use an even account count >=4, positive chat count and operations>=2")
    with tempfile.TemporaryDirectory(prefix="fe-rotation-load-") as directory:
        e = build(
            Config(
                database_path=str(Path(directory) / "load.sqlite3"),
                session_key=Fernet.generate_key().decode(),
                admin_ids=(123,),
            )
        )
        clock = [e.clock()]
        e.clock = lambda: clock[0]
        total_chats = seed(e, accounts=args.accounts, chats_per_pair=args.chats, history=0)
        e.set_rotation(1, True)
        clock[0] += 4 * 86400
        planning_times = []
        while not e.db.scalar("SELECT planning_done FROM rotation_cycles ORDER BY id DESC LIMIT 1"):
            before = time.perf_counter()
            e.tick()
            planning_times.append(time.perf_counter() - before)
        e, processing = asyncio.run(run(e, clock, args.operations, planning_times))
        report = {
            "accounts": args.accounts,
            "chats": total_chats,
            "exchange_plans": e.db.scalar("SELECT count(*) FROM rotation_exchanges"),
            "account_chat_preparations": e.db.scalar("SELECT count(*) FROM rotation_progress"),
            "largest_planning_cycle_seconds": round(max(planning_times), 4),
            "pending_jobs": e.db.scalar("SELECT count(*) FROM dispatch_jobs WHERE status='PENDING'"),
            "queue_capacity": e.runtime_options()["max_pending_jobs"],
            "processing": processing,
            "confirmed_kinds": {
                r["kind"]: r["total"]
                for r in e.db.all(
                    "SELECT kind,count(*) AS total FROM dispatch_jobs WHERE status='CONFIRMED' GROUP BY kind"
                )
            },
            "sqlite_integrity": e.db.scalar("PRAGMA integrity_check"),
            "sqlite_fk_errors": len(e.db.all("PRAGMA foreign_key_check")),
            "memory": memory_usage(),
            "real_telegram_operations": 0,
            "app_version": __import__("app").__version__,
        }
        assert processing["duplicates"] == 0
        assert report["pending_jobs"] <= report["queue_capacity"]
        assert report["sqlite_integrity"] == "ok" and not report["sqlite_fk_errors"]
        Path(args.output).write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
