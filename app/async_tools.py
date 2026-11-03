"""Cancellation-safe completion of already admitted work."""

import asyncio


async def settle_cancelled(future):
    """Wait through repeated cancellation without abandoning side effects.

    The caller re-raises its original CancelledError. Consume the completed
    work's exception as well, including a rollback failure.
    """
    while not future.done():
        try:
            await asyncio.shield(future)
        except asyncio.CancelledError:
            continue
        except Exception:
            break
    if not future.cancelled():
        future.exception()
