"""Repeatable local benchmark. Temporary SQLite; no Telegram clients or sockets."""

import argparse
import asyncio
import json
import os
import sys
import tempfile
import time
import tracemalloc
from pathlib import Path

sys.path.insert(0, os.getenv("FE_BENCH_SOURCE", str(Path(__file__).resolve().parents[1])))
from cryptography.fernet import Fernet

from app.__main__ import build
from app.config import Config
from app.scheduler.worker import Worker
from app.transport.base import FakeNotificationSink, FakeTransport, Result


def seed(e, accounts=400, chats_per_pair=20, history=20000):
    now = e.clock()
    campaign = e.create_campaign("Нагрузка", ["Проверка 1", "Проверка 2"], chat_limit=chats_per_pair)
    secret = e.secrets.encrypt("fake-session")
    with e.db.tx() as c:
        c.executemany(
            "INSERT INTO accounts(id,name,phone_mask,telegram_id,onboarding_started_at,stage_started_at,account_health,onboarding_stage,prepared_at,joined_at,ready_at,created_at) VALUES (?,?,'***',?,?,?,'OK','READY',?,?,?,?)",
            [
                (i, f"Тест {i}", i, now - 200000, now - 200000, now - 10000, now - 10000, now - 10000, now)
                for i in range(1, accounts + 1)
            ],
        )
        c.executemany(
            "INSERT INTO account_sessions VALUES (?,?,?,?)", [(i, secret, secret, now) for i in range(1, accounts + 1)]
        )
        c.executemany("INSERT INTO campaign_members VALUES (?,?)", [(i, campaign) for i in range(1, accounts + 1)])
        for p in range(1, accounts // 2 + 1):
            c.execute(
                "INSERT INTO pairs(id,campaign_id,account_a_id,account_b_id,chat_limit,status,pair_dispatch_started_at,active_dispatch_seconds,created_at) VALUES (?,?,?,?,?,'READY',?,259200,?)",
                (p, campaign, p * 2 - 1, p * 2, chats_per_pair, now - 259200, now),
            )
        total = accounts // 2 * chats_per_pair
        c.executemany(
            "INSERT INTO chat_pool(id,chat_id,title,link,approved,status,created_at) VALUES (?,?,'Нагрузочный чат',?,1,'ASSIGNED',?)",
            [(i, -100000 - i, str(-100000 - i), now) for i in range(1, total + 1)],
        )
        c.executemany(
            "INSERT INTO pair_chats(pair_id,chat_pool_id,assigned_at) VALUES (?,?,?)",
            [((i - 1) // chats_per_pair + 1, i, now) for i in range(1, total + 1)],
        )
        c.executemany(
            "INSERT INTO account_chat_access(account_id,chat_pool_id,joined,can_write) VALUES (?,?,1,1)",
            [
                (a, i)
                for i in range(1, total + 1)
                for a in (((i - 1) // chats_per_pair + 1) * 2 - 1, ((i - 1) // chats_per_pair + 1) * 2)
            ],
        )
        c.executemany(
            "INSERT INTO dispatch_jobs(id,idempotency_key,campaign_id,pair_id,account_id,chat_pool_id,kind,payload,version,planned_at,status,created_at) VALUES (?, ?, ?, ?, ?, ?, 'MAILING','{}',1,?,'CONFIRMED',?)",
            [
                (
                    i,
                    f"history:{i}",
                    campaign,
                    (i - 1) % (accounts // 2) + 1,
                    ((i - 1) % (accounts // 2)) * 2 + 1,
                    (i - 1) % total + 1,
                    now - 1000,
                    now - 1000,
                )
                for i in range(1, history + 1)
            ],
        )
        c.executemany(
            "INSERT INTO message_attempts(id,job_id,account_id,started_at,outcome) VALUES (?,?,?,?, 'CONFIRMED')",
            [(i, i, ((i - 1) % (accounts // 2)) * 2 + 1, now - 1000) for i in range(1, history + 1)],
        )
        c.executemany(
            "INSERT INTO message_results(job_id,attempt_id,api_message_id,confirmed_at,result) VALUES (?,?,?,?, '{}')",
            [(i, i, i, now - 1000) for i in range(1, history + 1)],
        )
    e.campaign_status(campaign, "RUNNING", True)
    return total


def measure(function):
    tracemalloc.start()
    wall, cpu = time.perf_counter(), time.process_time()
    result = function()
    data = {
        "wall_seconds": round(time.perf_counter() - wall, 4),
        "cpu_seconds": round(time.process_time() - cpu, 4),
        "python_peak_mb": round(tracemalloc.get_traced_memory()[1] / 1048576, 3),
    }
    tracemalloc.stop()
    return data, result


def memory_usage():
    if sys.platform == "win32":
        import ctypes
        from ctypes import wintypes

        class Counters(ctypes.Structure):
            _fields_ = [("cb", wintypes.DWORD), ("PageFaultCount", wintypes.DWORD)] + [
                (name, ctypes.c_size_t)
                for name in (
                    "PeakWorkingSetSize",
                    "WorkingSetSize",
                    "QuotaPeakPagedPoolUsage",
                    "QuotaPagedPoolUsage",
                    "QuotaPeakNonPagedPoolUsage",
                    "QuotaNonPagedPoolUsage",
                    "PagefileUsage",
                    "PeakPagefileUsage",
                )
            ]

        counters = Counters()
        counters.cb = ctypes.sizeof(counters)
        current = ctypes.windll.kernel32.GetCurrentProcess
        current.restype = wintypes.HANDLE
        function = ctypes.windll.psapi.GetProcessMemoryInfo
        function.argtypes = [wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD]
        if function(current(), ctypes.byref(counters), counters.cb):
            return {
                "rss_mb": round(counters.WorkingSetSize / 1048576, 2),
                "process_peak_rss_mb": round(counters.PeakWorkingSetSize / 1048576, 2),
            }
    else:
        import resource

        return {
            "process_peak_rss_mb": round(
                resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1048576 if sys.platform == "darwin" else 1024), 2
            )
        }
    return {}


class ProbeTransport(FakeTransport):
    def __init__(self):
        super().__init__()
        self.active = 0
        self.max_active = 0
        self.keys = set()
        self.duplicates = 0

    async def execute(self, job):
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            await asyncio.sleep(0.02)
            if job["idempotency_key"] in self.keys:
                self.duplicates += 1
            self.keys.add(job["idempotency_key"])
            return Result(len(self.keys), {"dry_run": True})
        finally:
            self.active -= 1


async def process(e, count, restart_at=0, mixed=False):
    api = ProbeTransport()
    worker = Worker(e, api, FakeNotificationSink())
    wall = time.perf_counter()
    cpu = time.process_time()
    lags = []
    running = True
    restarted = False

    async def heartbeat():
        previous = time.perf_counter()
        while running:
            await asyncio.sleep(0.01)
            current = time.perf_counter()
            lags.append(max(0, current - previous - 0.01))
            previous = current

    task = asyncio.create_task(heartbeat())
    background = []
    if mixed:

        async def stats_load():
            for _ in range(2):
                await e.db.run(e.statistics, "all")
                await asyncio.sleep(0.05)

        async def planning_load():
            for _ in range(4):
                await e.db.run(e.tick, pair_limit=64)
                await asyncio.sleep(0.05)

        background = [asyncio.create_task(stats_load()), asyncio.create_task(planning_load())]
    try:
        while len(api.keys) < count:
            if hasattr(worker, "run_batch"):
                n = await worker.run_batch(max_jobs=count - len(api.keys))
            else:
                n = await e.run_one(api)
            if not n:
                break
            if restart_at and not restarted and len(api.keys) >= restart_at:
                from app.db import Database
                from app.services import Engine

                e = Engine(Database(e.db.path), e.config, e.secrets, e.clock)
                worker = Worker(e, api, FakeNotificationSink())
                restarted = True
    finally:
        await asyncio.gather(*background)
        running = False
        await task
    return {
        "wall_seconds": round(time.perf_counter() - wall, 4),
        "cpu_seconds": round(time.process_time() - cpu, 4),
        "confirmed": len(api.keys),
        "duplicates": api.duplicates,
        "max_concurrency": api.max_active,
        "restart_performed": restarted,
        "concurrent_statistics_and_planning": mixed,
        "event_loop_lag_max_ms": round(max(lags, default=0) * 1000, 3),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--accounts", type=int, default=400)
    parser.add_argument("--chats", type=int, default=20)
    parser.add_argument("--history", type=int, default=20000)
    parser.add_argument("--send", type=int, default=200)
    parser.add_argument("--restart-at", type=int, default=0)
    parser.add_argument("--mixed", action="store_true")
    parser.add_argument("--output", default="docs/load-results.json")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as directory:
        e = build(
            Config(
                database_path=str(Path(directory) / "bench.sqlite3"),
                session_key=Fernet.generate_key().decode(),
                admin_ids=(123,),
            )
        )
        total = seed(e, args.accounts, args.chats, args.history)
        planning, _ = measure(
            lambda: (
                [e.tick(pair_limit=64) for _ in range((args.accounts // 2 + 63) // 64)]
                if hasattr(Worker, "run_batch")
                else e.tick()
            )
        )
        statistics, stats = measure(lambda: e.statistics("all"))
        processing = asyncio.run(process(e, args.send, args.restart_at, args.mixed))
        report = {
            "accounts": args.accounts,
            "chats": total,
            "historical_results": args.history,
            "planning": planning,
            "statistics": statistics,
            "statistics_confirmed": stats["confirmed"],
            "processing": processing,
            "sqlite_integrity": e.db.scalar("PRAGMA integrity_check"),
            "sqlite_fk_errors": len(e.db.all("PRAGMA foreign_key_check")),
            "python": sys.version.split()[0],
            "cpu_logical": os.cpu_count(),
            "mock_rpc_latency_seconds": 0.02,
            "memory": memory_usage(),
            "app_version": __import__("app").__version__,
        }
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.output).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
