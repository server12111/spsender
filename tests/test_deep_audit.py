"""Fault injection and cross-feature regressions from the second full audit."""

import asyncio
import random
import threading
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from weakref import WeakValueDictionary

import pytest
from conftest import restart, setup_pair

from app.bot.interface import Interface
from app.scheduler.worker import Worker
from app.services.core import DomainError
from app.services.statistics import period_bounds
from app.transport.base import OperationError, Result
from app.transport.telegram import AccountAuthorizer, TelegramTransport


def incoming_group(e, pair, message_id=1):
    active = e.pair_card(pair)["active_account_id"]
    telegram_id = e.db.scalar("SELECT telegram_id FROM accounts WHERE id=?", (active,))
    return e.incoming(active, -100000, message_id, 500, False, reply_to_sender_id=telegram_id)


def full_group_queue(e):
    campaign, pair, a, b, chats = setup_pair(e, 1)
    e.edit_campaign(campaign, "group", ["Group reply"], group_reply_enabled=True)
    e.set_setting("max_pending_jobs", 1)
    e.tick()
    assert not incoming_group(e, pair)
    return campaign, pair, a, b, chats


async def test_full_queue_manual_reply_reports_failure_and_keeps_input(env):
    e, _, api, _ = env
    _, pair, _, _, _ = setup_pair(e, 1)
    e.set_setting("max_pending_jobs", 1)
    e.tick()
    active = e.pair_card(pair)["active_account_id"]
    e.incoming(active, 500, 1, 500, True)
    ui = Interface(e)
    ui.set_state(123, "manual_dm", {"account_id": active, "peer_id": 500, "message_id": 1})
    with pytest.raises(DomainError, match="Очередь заполнена"):
        await ui.message(123, "Reply")
    assert ui.state(123)["state"] == "manual_dm"
    assert await e.run_one(api)
    await ui.message(123, "Reply")
    assert ui.state(123) is None
    assert await e.run_one(api)
    assert api.calls[-1]["kind"] == "DM_MANUAL"


async def test_manual_reply_is_not_duplicated_after_campaign_edit(env):
    e, _, api, _ = env
    campaign, pair, _, _, _ = setup_pair(e, 1)
    active = e.pair_card(pair)["active_account_id"]
    e.incoming(active, 500, 1, 500, True)
    first = e.manual_dm_reply(active, 500, 1, {"text": "Reply"})
    assert await e.run_one(api)
    e.edit_campaign(campaign, name="Renamed")
    assert e.manual_dm_reply(active, 500, 1, {"text": "Reply"}) == first
    assert not await e.run_one(api)
    assert len(api.calls) == 1


@pytest.mark.parametrize(
    "template",
    [{"text": ""}, {"text": " "}, {"text": "x" * 4097}, {"text": "x" * 1025, "media_path": "data/media/test.jpg"}],
)
def test_invalid_manual_reply_never_reaches_account_queue(env, template):
    e, _, _, _ = env
    _, pair, _, _, _ = setup_pair(e, 1)
    active = e.pair_card(pair)["active_account_id"]
    e.incoming(active, 500, 1, 500, True)
    with pytest.raises(DomainError):
        e.manual_dm_reply(active, 500, 1, template)
    assert not e.db.scalar("SELECT count(*) FROM dispatch_jobs")


async def test_full_queue_group_reply_is_durable_and_deduplicated(env):
    e, _, api, _ = env
    _, pair, _, _, _ = full_group_queue(e)
    assert not incoming_group(e, pair, 2)
    assert e.db.scalar("SELECT count(*) FROM group_reply_intents WHERE status='WAITING'") == 1
    assert await e.run_one(api)
    e = restart(e)
    e.tick()
    assert await e.run_one(api)
    assert api.calls[-1]["kind"] == "GROUP_REPLY"
    assert api.calls[-1]["payload"]["reply_to"] == 1
    assert e.db.scalar("SELECT status FROM group_reply_intents") == "QUEUED"
    e.tick()
    assert not await e.run_one(api)
    assert sum(j["kind"] == "GROUP_REPLY" for j in api.calls) == 1


@pytest.mark.parametrize("change", ["PAUSED", "STOPPED", "REMOVED", "edit", "account_pause", "archive", "critical"])
def test_stopping_or_changing_context_cancels_waiting_group_reply(env, change):
    e, _, _, _ = env
    campaign, _, a, _, chats = full_group_queue(e)
    if change in {"PAUSED", "STOPPED", "REMOVED"}:
        e.campaign_status(campaign, change, confirmed=True)
    elif change == "edit":
        e.edit_campaign(campaign, "group", ["New reply"])
    elif change == "account_pause":
        e.update_account(a, "pause")
    elif change == "archive":
        e.archive_chat(chats[0])
    else:
        e.critical(a, "FROZEN")
    assert e.db.scalar("SELECT status FROM group_reply_intents") == "CANCELLED"


async def test_off_shift_waiting_group_reply_is_not_sent_by_partner(env):
    e, clock, api, _ = env
    full_group_queue(e)
    assert await e.run_one(api)
    clock.advance(6 * 3600)
    e.tick()
    assert e.db.scalar("SELECT status FROM group_reply_intents") == "CANCELLED"
    assert not e.db.scalar("SELECT count(*) FROM dispatch_jobs WHERE kind='GROUP_REPLY'")


class DisconnectClient:
    def __init__(self):
        self.failed = True
        self.connected = True

    async def disconnect(self):
        if self.failed:
            raise ConnectionError("disconnect failed")
        self.connected = False

    def is_connected(self):
        return self.connected


def closing_transport(e):
    transport = object.__new__(TelegramTransport)
    transport.engine = e
    transport._incoming_workers = []
    transport._incoming = asyncio.Queue()
    client = DisconnectClient()
    transport.clients = {1: client}
    transport._client_sessions = {1: "opaque"}
    transport.sync_listeners = AsyncMock()
    return transport, client


async def test_disconnect_failure_is_visible_and_retry_keeps_session_reference(env):
    e, _, _, _ = env
    transport, client = closing_transport(e)
    with pytest.raises(ConnectionError, match="CLIENT_DISCONNECT_FAILED"):
        await transport.close()
    assert transport.clients[1] is client and transport._client_sessions[1] == "opaque"
    with pytest.raises(OperationError, match="TRANSPORT_CLOSING"):
        await transport.client(1)
    client.failed = False
    await transport.close()
    assert not transport.clients and not client.connected


async def test_failed_real_transport_close_does_not_release_runtime_lease(env):
    e, _, _, sink = env
    transport, client = closing_transport(e)
    worker = Worker(e, transport, sink)
    running = asyncio.create_task(worker.run())
    try:
        for _ in range(100):
            if e.db.scalar("SELECT count(*) FROM worker_leases"):
                break
            await asyncio.sleep(0.005)
        assert not running.done()
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running
        assert e.db.scalar("SELECT owner FROM worker_leases") == worker.owner
    finally:
        client.failed = False
        await transport.close()
        if not running.done():
            running.cancel()
            await asyncio.gather(running, return_exceptions=True)


async def test_failed_retirement_does_not_create_a_second_session_client(env):
    e, _, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    transport, client = closing_transport(e)
    transport._client_locks = WeakValueDictionary()
    transport.new_client = lambda session: pytest.fail("Old client has not disconnected")
    e.save_session(a, "updated-session", 1001)
    with pytest.raises(ConnectionError):
        await transport.client(a)
    assert transport.clients[a] is client
    e.update_account(a, "pause")
    await TelegramTransport.sync_listeners(transport)
    assert transport.clients[a] is client
    client.failed = False
    await TelegramTransport.sync_listeners(transport)
    assert a not in transport.clients


async def test_failed_cleanup_keeps_half_created_client_for_shutdown_retry(env):
    e, _, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    transport, client = closing_transport(e)
    transport.clients.clear()
    transport._client_sessions.clear()
    transport._client_locks = WeakValueDictionary()
    client.connect = AsyncMock(side_effect=ConnectionError("connect failed"))
    transport.new_client = lambda session: client
    with pytest.raises(OperationError):
        await transport.client(a)
    assert transport.clients[a] is client
    client.failed = False
    await transport.close()
    assert not transport.clients


async def test_retirement_batches_rotate_past_failing_clients(env):
    e, _, _, _ = env
    e.config = replace(e.config, connection_batch_size=4)
    transport, _ = closing_transport(e)
    transport.clients = {i: DisconnectClient() for i in range(1, 13)}
    for i, client in transport.clients.items():
        client.failed = i <= 4
    transport._client_sessions = {i: "opaque" for i in transport.clients}
    await TelegramTransport.sync_listeners(transport)
    assert len(transport.clients) == 12  # Failed first batch remains available for retry.
    await TelegramTransport.sync_listeners(transport)
    assert len(transport.clients) == 8 and 5 not in transport.clients
    await TelegramTransport.sync_listeners(transport)
    assert set(transport.clients) == {1, 2, 3, 4}
    for client in transport.clients.values():
        client.failed = False
    await transport.close()


async def test_authorization_expiry_does_not_wait_for_active_login(env):
    e, clock, _, _ = env
    auth = AccountAuthorizer(e, None)
    auth.pending[123] = {"phone": "fake", "at": clock() - 1000}
    await auth._locks[123].acquire()
    try:
        await asyncio.wait_for(auth.expire(), 0.1)
        assert 123 in auth.pending
    finally:
        auth._locks[123].release()
    await auth.expire()
    assert not auth.pending


async def test_cancelled_claim_before_network_is_known_unsent(env, monkeypatch):
    e, _, api, _ = env
    setup_pair(e, 1)
    e.tick()
    claim = e.claim_job
    committed, release = threading.Event(), threading.Event()

    def delayed(owner):
        job = claim(owner)
        committed.set()
        assert release.wait(2)
        return job

    monkeypatch.setattr(e, "claim_job", delayed)
    task = asyncio.create_task(e.run_one(api))
    try:
        assert await asyncio.to_thread(committed.wait, 2)
        task.cancel()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not api.calls
    assert e.db.scalar("SELECT status FROM dispatch_jobs") == "PENDING"
    assert e.db.scalar("SELECT outcome FROM message_attempts") == "CANCELLED"
    assert not e.db.scalar("SELECT count(*) FROM notifications_outbox WHERE kind='unknown'")
    monkeypatch.setattr(e, "claim_job", claim)
    assert await e.run_one(api)
    assert len(api.calls) == 1


def test_retry_on_next_shift_preserves_per_account_attempt_statistics(env):
    e, clock, _, _ = env
    setup_pair(e, 1)
    e.tick()
    first = e.claim_job("A")
    e.fail_job(first, OperationError("transient", "PRE_SEND_FAILURE"))
    clock.advance(6 * 3600)
    second = e.claim_job("B")
    assert second["account_id"] != first["account_id"]
    e.finish_job(second, Result(1))
    a = e.statistics("all", "accounts", first["account_id"])
    b = e.statistics("all", "accounts", second["account_id"])
    assert (a["attempts"], a["errors"], a["confirmed"]) == (1, 1, 0)
    assert (b["attempts"], b["errors"], b["confirmed"]) == (1, 0, 1)


@pytest.mark.parametrize("period", ["today", "7", "30", "all"])
def test_statistics_end_in_second_dst_hour_is_actual_utc_now(period):
    now = datetime(2026, 10, 25, 1, 30, tzinfo=timezone.utc).timestamp()
    start, end = period_bounds(period, now, "Europe/Kyiv")
    assert start <= now < end and end == now + 1


async def test_account_check_and_import_resolution_have_rpc_timeouts(env, monkeypatch):
    import app.transport.telegram as telegram

    e, _, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    e.config = replace(e.config, mode="live")
    client = SimpleNamespace(get_me=AsyncMock(), get_entity=AsyncMock())

    async def hang(*args):
        await asyncio.Event().wait()

    client.get_me.side_effect = hang
    client.get_entity.side_effect = hang
    transport = object.__new__(TelegramTransport)
    transport.engine = e
    transport.client = AsyncMock(return_value=client)
    original = asyncio.wait_for
    deadlines = []

    async def shortened(coro, timeout):
        deadlines.append(timeout)
        return await original(coro, 0.01)

    monkeypatch.setattr(telegram.asyncio, "wait_for", shortened)
    async with asyncio.timeout(1):
        with pytest.raises(DomainError):
            await Interface(e, transport).check_account(a)
        with pytest.raises(TimeoutError):
            await transport.resolve_target("@localtest")
    assert deadlines == [30, 30]


async def test_cancel_import_prevents_late_resolve_from_adding_chat(env):
    e, _, _, _ = env
    entered, release = asyncio.Event(), asyncio.Event()

    async def resolve(value):
        entered.set()
        await release.wait()
        return -100000, 0, "@localtest", "Local"

    transport = SimpleNamespace(resolve_target=resolve)
    ui = Interface(e, transport)
    view = ui.import_confirmation(123, {"lines": ["@localtest", "@secondtest"], "approved": True})
    callback = next(
        cb
        for group in view.buttons
        for _, cb in group
        if e.db.scalar("SELECT action FROM ui_actions WHERE token=?", (cb[2:],)) == "import_confirm"
    )
    task = asyncio.create_task(ui.callback(123, callback))
    await entered.wait()
    try:
        cancel = ui.cancel(123)[0][0][1]
        async with asyncio.timeout(1):
            await ui.callback(123, cancel)
        assert ui.state(123) is None
    finally:
        release.set()
        result = await task
    assert "Импорт отменён" in result.text
    assert not e.db.scalar("SELECT count(*) FROM chat_pool")


def test_import_commit_rechecks_revision_in_same_transaction(env):
    e, _, _, _ = env
    ui = Interface(e)
    ui.import_confirmation(123, {"lines": ["@localtest"], "approved": True})
    old_revision = ui.state(123)["data"]["_revision"]
    ui.clear(123)
    ui.set_state(123, "add_account_name")
    with pytest.raises(DomainError, match="отменён"):
        ui._import_chat(123, old_revision, -100000, "Local", 0, "@localtest", {"approved": True})
    assert not e.db.scalar("SELECT count(*) FROM chat_pool")
    assert ui.clear(123, old_revision) == 0
    assert ui.state(123)["state"] == "add_account_name"


async def test_last_import_failure_does_not_clear_replacement_wizard(env):
    e, _, _, _ = env
    entered, release = asyncio.Event(), asyncio.Event()

    async def resolve(value):
        entered.set()
        await release.wait()
        raise TimeoutError()

    ui = Interface(e, SimpleNamespace(resolve_target=resolve))
    view = ui.import_confirmation(123, {"lines": ["@localtest"], "approved": True})
    callback = next(
        cb
        for group in view.buttons
        for _, cb in group
        if e.db.scalar("SELECT action FROM ui_actions WHERE token=?", (cb[2:],)) == "import_confirm"
    )
    task = asyncio.create_task(ui.callback(123, callback))
    await entered.wait()
    ui.clear(123)
    ui.set_state(123, "add_account_name")
    release.set()
    await task
    assert ui.state(123)["state"] == "add_account_name"


@pytest.mark.parametrize("seed", [7, 29, 71])
async def test_mixed_state_transitions_preserve_delivery_and_queue_invariants(env, seed):
    e, clock, _, _ = env
    rng = random.Random(seed)
    campaign, pair, a, b, chats = setup_pair(e, 3)
    for kind, flag in (
        ("dm_first", "dm_first_enabled"),
        ("dm_always", "dm_always_enabled"),
        ("group", "group_reply_enabled"),
    ):
        e.edit_campaign(campaign, kind, ["Reply"], **{flag: True})
    e.set_setting("max_pending_jobs", 3)
    delivered = set()

    class CheckedTransport:
        async def execute(self, job):
            assert e.execution_allowed(job)
            assert job["id"] not in delivered
            if job["chat_pool_id"] is not None:
                assert e.db.scalar("SELECT approved FROM chat_pool WHERE id=?", (job["chat_pool_id"],))
            if rng.random() < 0.1:
                raise OperationError("transient", "BEFORE_REQUEST")
            delivered.add(job["id"])
            return Result(job["id"])

    transport = CheckedTransport()
    for event in range(300):
        clock.advance(rng.randint(0, 300))
        active = e.pair_card(pair)["active_account_id"]
        op = rng.randrange(10)
        if op == 0:
            e.tick()
        elif op == 1:
            await e.run_one(transport)
        elif op == 2:
            status = e.db.scalar("SELECT status FROM campaigns WHERE id=?", (campaign,))
            e.campaign_status(campaign, "PAUSED" if status == "RUNNING" else "RUNNING", confirmed=True)
        elif op == 3:
            e.edit_campaign(campaign, name=f"Version {event}")
        elif op == 4:
            account = rng.choice([a, b])
            paused = e.db.scalar("SELECT paused FROM accounts WHERE id=?", (account,))
            e.update_account(account, "resume" if paused else "pause")
        elif op == 5:
            peer = rng.randrange(500, 505)
            e.incoming(active, peer, event + 1, peer, True)
        elif op == 6:
            tid = e.db.scalar("SELECT telegram_id FROM accounts WHERE id=?", (active,))
            e.incoming(active, -100000, event + 1, 500, False, reply_to_sender_id=tid)
        elif op == 7:
            approved = e.db.scalar("SELECT approved FROM chat_pool WHERE id=?", (chats[0],))
            e.set_chat_permissions(chats[0], not approved, True)
            if not approved:
                e.resume_chat(chats[0])
        elif op == 8:
            e.flood_wait(active, rng.randint(1, 30))
        else:
            e.clear_reply_history(campaign)
        assert e.db.scalar("SELECT value FROM queue_counters WHERE name='pending'") == e.db.scalar(
            "SELECT count(*) FROM dispatch_jobs WHERE status='PENDING'"
        )
        assert e.db.scalar("SELECT count(*) FROM message_results") == len(delivered)
    assert e.statistics("all")["confirmed"] == len(delivered)
    assert e.db.scalar("PRAGMA integrity_check") == "ok"
    assert not e.db.all("PRAGMA foreign_key_check")
