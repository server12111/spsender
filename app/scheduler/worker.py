import asyncio
import math
import sqlite3
import time
from uuid import uuid4

from app.async_tools import settle_cancelled
from app.services.core import row


class Worker:
    def __init__(self, engine, transport, sink):
        self.e, self.transport, self.sink = engine, transport, sink
        self.owner = uuid4().hex
        self.heartbeat_interval = 10

    def runtime_lease(self):
        # Live MTProto sessions belong to one runtime process. Queue claiming itself
        # is multi-process safe, including tests with two concurrent workers.
        with self.e.db.tx() as c:
            now = self.e.clock()
            lease = row(c, "SELECT * FROM worker_leases WHERE name='telegram-runtime'")
            if lease and lease["owner"] != self.owner and lease["expires_at"] > now:
                return False
            if not lease or lease["owner"] != self.owner or lease["expires_at"] <= now:
                checkpoint = row(c, "SELECT value FROM settings WHERE key='runtime_clock_checkpoint'")
                try:
                    cutoff = float(checkpoint["value"]) if checkpoint else lease["expires_at"] - 180 if lease else 0
                except (ValueError, TypeError):
                    cutoff = 0
                # No evidence of active runtime means no extra ramp credit.
                cutoff = min(now, cutoff) if math.isfinite(cutoff) else 0
                self._pause_clocks(c, cutoff)
            self._checkpoint(c, now)
            c.execute(
                "INSERT INTO worker_leases VALUES ('telegram-runtime',?,?) ON CONFLICT(name) DO UPDATE SET owner=excluded.owner,expires_at=excluded.expires_at",
                (self.owner, now + 180),
            )
            return True

    @staticmethod
    def _checkpoint(c, now):
        c.execute(
            "INSERT INTO settings VALUES ('runtime_clock_checkpoint',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (str(now),),
        )

    @staticmethod
    def _pause_clocks(c, cutoff):
        c.execute(
            "UPDATE pairs SET active_dispatch_seconds=active_dispatch_seconds+MAX(0,?-active_since),"
            "active_since=NULL WHERE active_since IS NOT NULL",
            (cutoff,),
        )

    async def run_once(self):
        await self.e.db.run(self.e.recover_expired)
        await self.e.db.run(self.e.tick)
        worked = await self.run_batch()
        await self.e.deliver_notifications(self.sink, self.owner)
        return worked

    async def run_batch(self, max_jobs=None):
        options = await self.e.db.run(self.e.runtime_options)
        remaining = options["batch_size"] if max_jobs is None else min(options["batch_size"], max_jobs)
        completed = 0

        async def consumer():
            nonlocal remaining, completed
            while remaining > 0:
                remaining -= 1
                if not await self.e.run_one(self.transport, self.owner, emergency_sink=self.sink):
                    break
                completed += 1

        tasks = [asyncio.create_task(consumer()) for _ in range(min(options["concurrency"], remaining))]
        # Settle every sibling before surfacing a failure: re-raising early would
        # cancel consumers mid-send and turn their jobs into spurious UNKNOWN.
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        except asyncio.CancelledError:
            await asyncio.gather(*tasks, return_exceptions=True)
            raise
        for result in results:
            if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
                raise result
        return completed

    async def run(self):
        tasks = []
        try:
            if not await self.e.db.run(self.runtime_lease):
                raise RuntimeError(
                    "Другой процесс уже обслуживает Telegram. Остановите его или дождитесь истечения lease (180 с)"
                )

            async def heartbeat():
                safe_deadline = time.monotonic() + 150
                while True:
                    try:
                        if not await self.e.db.run(self.runtime_lease):
                            raise RuntimeError("Потеряна блокировка Telegram runtime")
                        safe_deadline = time.monotonic() + 150
                    except sqlite3.Error as exc:
                        await self.e.db.run(self.e.diagnostic, "heartbeat", exc)
                        if time.monotonic() >= safe_deadline:
                            raise RuntimeError("База недоступна: runtime остановлен до истечения lease") from None
                    await asyncio.sleep(self.heartbeat_interval)

            async def planning():
                while True:
                    try:
                        await self.e.db.run(self.e.recover_expired)
                        await self.e.db.run(self.e.tick)
                        await self.e.db.run(self.e.maintenance)
                    except Exception as exc:
                        await self.e.db.run(self.e.diagnostic, "planner", exc)
                    await asyncio.sleep(self.e.config.worker_interval)

            async def dispatching():
                while True:
                    delay = self.e.config.batch_interval
                    try:
                        await self.run_batch()
                        options = await self.e.db.run(self.e.runtime_options)
                        delay = options["batch_interval"]
                    except Exception as exc:
                        await self.e.db.run(self.e.diagnostic, "dispatch", exc)
                    await asyncio.sleep(max(0.05, delay))

            async def listeners():
                while True:
                    try:
                        if hasattr(self.transport, "sync_listeners"):
                            await self.transport.sync_listeners()
                    except Exception as exc:
                        await self.e.db.run(self.e.diagnostic, "connections", exc)
                    await asyncio.sleep(self.e.config.worker_interval)

            tasks = [
                asyncio.create_task(heartbeat()),
                asyncio.create_task(planning()),
                asyncio.create_task(dispatching()),
                asyncio.create_task(listeners()),
            ]
            if self.e.config.mode == "live":
                from app.legacy.runtime import LegacyRuntime

                legacy = LegacyRuntime(self.e, self.transport, self.owner, self.sink)
                tasks.append(asyncio.create_task(legacy.run()))
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        finally:
            cleanup = asyncio.create_task(self._shutdown(tasks))
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                await settle_cancelled(cleanup)
                raise

    async def _shutdown(self, tasks):
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        # Disconnect clients BEFORE releasing ownership of their sessions.
        closed = False
        try:
            await asyncio.wait_for(self.transport.close(), 10)
            closed = True
        except Exception as exc:
            await self.e.db.run(self.e.diagnostic, "shutdown", exc)

        def release():
            with self.e.db.tx() as c:
                lease = row(c, "SELECT * FROM worker_leases WHERE name='telegram-runtime'")
                if not lease or lease["owner"] != self.owner:
                    return
                self._pause_clocks(c, self.e.clock())
                self._checkpoint(c, self.e.clock())
                if closed:
                    c.execute("DELETE FROM worker_leases WHERE name='telegram-runtime' AND owner=?", (self.owner,))

        try:
            await self.e.db.run(release)
        except Exception:
            pass  # Lease expires automatically; recovery uses the last checkpoint.
