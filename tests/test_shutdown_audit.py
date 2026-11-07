import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from conftest import setup_pair
from test_legacy_transfer import native_runtime

from app.legacy.mailing import mailing_context
from app.legacy.runtime import GuardedClient
from app.scheduler.worker import Worker
from app.transport.base import OperationError
from app.transport.limits import RPCBudget


@pytest.mark.parametrize("fails", [False, True])
async def test_repeated_cancellation_waits_for_database_thread(env, fails):
    e, _, _, _ = env
    loop = asyncio.get_running_loop()
    entered, finished = asyncio.Event(), asyncio.Event()
    release = threading.Event()

    def write():
        try:
            loop.call_soon_threadsafe(entered.set)
            if not release.wait(15):
                raise RuntimeError("test gate timed out")
            with e.db.tx() as c:
                c.execute("INSERT INTO settings VALUES ('cancelled-write','done')")
                if fails:
                    raise RuntimeError("simulated database failure")
        finally:
            loop.call_soon_threadsafe(finished.set)

    task = asyncio.create_task(e.db.run(write))
    try:
        await asyncio.wait_for(entered.wait(), 10)
        for _ in range(3):
            task.cancel()
            for _ in range(3):
                await asyncio.sleep(0)
        assert not task.done(), "cancellation returned while the database thread can still mutate state"
    finally:
        release.set()
        await asyncio.wait_for(finished.wait(), 10)
        outcome = await asyncio.gather(task, return_exceptions=True)
    assert isinstance(outcome[0], asyncio.CancelledError)
    assert e.db.scalar("SELECT count(*) FROM settings WHERE key='cancelled-write'") == int(not fails)


async def test_repeated_cancellation_during_budget_release_cannot_leak_capacity(env):
    e, _, _, _ = env
    budget = RPCBudget(e)
    await budget.__aenter__()
    await budget.condition.acquire()
    task = asyncio.create_task(budget.__aexit__(None, None, None))
    try:
        for _ in range(3):
            await asyncio.sleep(0)
        for _ in range(3):
            task.cancel()
            for _ in range(3):
                await asyncio.sleep(0)
        assert not task.done(), "budget cleanup was abandoned on repeated cancellation"
    finally:
        budget.condition.release()
        await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 10)
    assert budget.active == 0


@pytest.mark.parametrize("phase", ["get_entity", "get_permissions"])
async def test_native_preflight_has_its_own_deadline_and_never_starts_delivery(env, monkeypatch, phase):
    e, _, _, _ = env
    _, pair, a, _, _ = setup_pair(e, 1)
    runtime, native = native_runtime(e)
    gate = asyncio.Event()
    original = getattr(native, phase)

    async def stalled(*args, **kwargs):
        await gate.wait()
        return await original(*args, **kwargs)

    monkeypatch.setattr(native, phase, stalled)
    monkeypatch.setattr(GuardedClient, "preflight_timeout", 0.02, raising=False)
    message = (await runtime.mailing.db.get_mailing_messages(pair))[0]
    target = (await runtime.mailing.db.get_mailing_targets(pair))[0]
    token = mailing_context.set(pair)
    try:
        with pytest.raises(OperationError) as error:
            await asyncio.wait_for(
                runtime.mailing._send_msg(
                    await runtime.get_client(a),
                    target.chat_identifier,
                    message,
                    None,
                    account_id=a,
                    target_snapshot=target,
                ),
                10,
            )
        assert error.value.category == "transient" and error.value.code == "PREFLIGHT_TIMEOUT"
        assert not native.sent and not e.db.scalar("SELECT count(*) FROM legacy_rpc_journal")
        assert runtime.transport._operation_semaphore.active == 0
        # Undo restores BOTH the stalled phase and the 0.02 s preflight deadline:
        # the recovery send must run with the real 30 s deadline, otherwise a
        # loaded machine intermittently times out its own preflight.
        monkeypatch.undo()
        await runtime.mailing._send_msg(
            await runtime.get_client(a), target.chat_identifier, message, None, account_id=a, target_snapshot=target
        )
        assert len(native.sent) == 1
    finally:
        mailing_context.reset(token)


@pytest.mark.parametrize("change", ["client", "session"])
async def test_stale_native_client_cannot_send_after_replacement(env, change):
    e, _, _, _ = env
    _, pair, a, _, _ = setup_pair(e, 1)
    runtime, native = native_runtime(e)
    wrapped = await runtime.get_client(a)
    runtime.transport.clients = {a: native}
    runtime.transport._client_sessions = {
        a: e.db.scalar("SELECT encrypted_session FROM account_sessions WHERE account_id=?", (a,))
    }
    if change == "client":
        runtime.transport.clients[a] = object()
    else:
        e.save_session(a, "new-session", 1001)
    message = (await runtime.mailing.db.get_mailing_messages(pair))[0]
    target = (await runtime.mailing.db.get_mailing_targets(pair))[0]
    token = mailing_context.set(pair)
    try:
        with pytest.raises(OperationError) as error:
            await runtime.mailing._send_msg(
                wrapped, target.chat_identifier, message, None, account_id=a, target_snapshot=target
            )
        assert error.value.category == "cancelled"
        assert not native.sent and not native.requests
    finally:
        mailing_context.reset(token)


def test_disconnected_native_clients_are_pruned_from_wrapper_cache(env):
    e, _, _, _ = env
    runtime, _ = native_runtime(e)
    retained = SimpleNamespace(client=object())
    removed = SimpleNamespace(client=object())
    runtime._clients = {1: retained, 2: removed}
    runtime._me_ids = {1: 1001, 2: 1002}
    runtime.transport.clients = {1: retained.client}
    runtime.prune_clients()
    assert runtime._clients == {1: retained} and runtime._me_ids == {1: 1001}


@pytest.mark.parametrize("phase", ["transport", "runtime"])
async def test_held_native_client_cannot_send_after_shutdown_starts(env, phase):
    e, _, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    runtime, native = native_runtime(e)
    wrapped = await runtime.get_client(a)
    if phase == "transport":
        runtime.transport._closing = True
    else:
        await runtime.close()
    with pytest.raises(OperationError) as error:
        await wrapped.send_message("me", "test")
    assert error.value.category == "cancelled" and not native.sent


@pytest.mark.parametrize("history", ["checkpoint", "legacy-lease", "no-evidence"])
def test_restart_never_credits_server_downtime_to_pair_ramp(env, history):
    e, clock, api, sink = env
    _, pair, _, _, _ = setup_pair(e, 1)
    first = Worker(e, api, sink)
    assert first.runtime_lease()
    with e.db.tx() as c:
        c.execute("UPDATE pairs SET pair_dispatch_started_at=?,active_since=? WHERE id=?", (clock(), clock(), pair))
    clock.advance(60)
    assert first.runtime_lease()
    if history != "checkpoint":
        with e.db.tx() as c:
            c.execute("DELETE FROM settings WHERE key='runtime_clock_checkpoint'")
            if history == "no-evidence":
                c.execute("DELETE FROM worker_leases")
    clock.advance(3 * 86400)
    assert Worker(e, api, sink).runtime_lease()
    card = e.pair_card(pair)
    assert card["active_since"] is None
    expected = 0 if history == "no-evidence" else 60
    assert card["elapsed"] == expected
    e.tick()
    clock.advance(10)
    assert e.pair_card(pair)["elapsed"] == expected + 10


@pytest.mark.parametrize("close_fails", [False, True])
async def test_worker_shutdown_pauses_pair_clock_before_offline_time(env, monkeypatch, close_fails):
    e, clock, api, sink = env
    _, pair, _, _, _ = setup_pair(e, 1)
    worker = Worker(e, api, sink)
    assert worker.runtime_lease()
    with e.db.tx() as c:
        c.execute("UPDATE pairs SET pair_dispatch_started_at=?,active_since=? WHERE id=?", (clock(), clock(), pair))
    if close_fails:
        monkeypatch.setattr(api, "close", AsyncMock(side_effect=ConnectionError("simulated disconnect failure")))
    monkeypatch.setattr(worker, "run_batch", AsyncMock(return_value=0))
    monkeypatch.setattr(e, "tick", lambda: None)
    original = worker.runtime_lease
    acquired = asyncio.Event()
    loop = asyncio.get_running_loop()

    def lease():
        result = original()
        loop.call_soon_threadsafe(acquired.set)
        return result

    monkeypatch.setattr(worker, "runtime_lease", lease)
    task = asyncio.create_task(worker.run())
    try:
        await asyncio.wait_for(acquired.wait(), 10)
        clock.advance(60)
    finally:
        task.cancel()
        await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 10)
    assert e.pair_card(pair)["active_since"] is None
    clock.advance(86400)
    assert e.pair_card(pair)["elapsed"] == 60
    assert bool(e.db.scalar("SELECT count(*) FROM worker_leases")) == close_fails


async def test_repeated_worker_cancellation_finishes_disconnect_before_releasing_lease(env, monkeypatch):
    e, _, api, sink = env
    worker = Worker(e, api, sink)
    entered, release = asyncio.Event(), asyncio.Event()
    assert worker.runtime_lease()

    async def close():
        entered.set()
        await release.wait()

    monkeypatch.setattr(api, "close", close)
    task = asyncio.create_task(worker.run())
    try:
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.wait_for(entered.wait(), 10)
        for _ in range(3):
            task.cancel()
            for _ in range(3):
                await asyncio.sleep(0)
        assert not task.done()
        assert e.db.scalar("SELECT owner FROM worker_leases") == worker.owner
    finally:
        release.set()
        result = await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 10)
    assert isinstance(result[0], asyncio.CancelledError)
    assert not e.db.scalar("SELECT count(*) FROM worker_leases")


async def test_old_worker_shutdown_does_not_pause_new_owners_clocks(env):
    e, clock, api, sink = env
    _, pair, _, _, _ = setup_pair(e, 1)
    first, second = Worker(e, api, sink), Worker(e, api, sink)
    assert first.runtime_lease()
    clock.advance(181)
    assert second.runtime_lease()
    with e.db.tx() as c:
        c.execute("UPDATE pairs SET active_since=? WHERE id=?", (clock(), pair))
    clock.advance(10)
    await first._shutdown([])
    assert e.db.scalar("SELECT owner FROM worker_leases") == second.owner
    assert e.pair_card(pair)["elapsed"] == 10
    assert e.pair_card(pair)["active_since"] is not None


def test_competing_worker_does_not_touch_active_clocks_or_checkpoint(env):
    e, clock, api, sink = env
    _, pair, _, _, _ = setup_pair(e, 1)
    first = Worker(e, api, sink)
    assert first.runtime_lease()
    checkpoint = e.db.scalar("SELECT value FROM settings WHERE key='runtime_clock_checkpoint'")
    with e.db.tx() as c:
        c.execute("UPDATE pairs SET active_since=? WHERE id=?", (clock(), pair))
    clock.advance(60)
    assert not Worker(e, api, sink).runtime_lease()
    assert e.pair_card(pair)["elapsed"] == 60
    assert e.db.scalar("SELECT value FROM settings WHERE key='runtime_clock_checkpoint'") == checkpoint
