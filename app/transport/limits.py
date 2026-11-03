"""One adjustable RPC budget for mailing, preparation and original warmup."""

import asyncio
import time

from app.async_tools import settle_cancelled


class RPCBudget:
    def __init__(self, engine):
        self.engine = engine
        self.active = 0
        self.condition = asyncio.Condition()
        self.admitted = 0
        self.pause_until = 0

    async def __aenter__(self):
        while True:
            options = await self.engine.db.run(self.engine.runtime_options)
            limit = options["concurrency"]
            delay = self.pause_until - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
                continue
            async with self.condition:
                if self.pause_until > time.monotonic():
                    continue
                if self.active < limit:
                    self.active += 1
                    self.admitted += 1
                    if self.admitted >= options["batch_size"]:
                        self.admitted = 0
                        self.pause_until = time.monotonic() + options["batch_interval"]
                    if self.active < limit:
                        self.condition.notify(1)
                    return self
                try:
                    await self.condition.wait()
                except asyncio.CancelledError:
                    self.condition.notify(1)
                    raise

    async def __aexit__(self, *args):
        release = asyncio.create_task(self._release())
        try:
            await asyncio.shield(release)
        except asyncio.CancelledError:
            await settle_cancelled(release)
            raise

    async def _release(self):
        async with self.condition:
            self.active -= 1
            self.condition.notify(1)
