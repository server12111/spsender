import asyncio
import sqlite3
from dataclasses import replace
from types import SimpleNamespace

import pytest
from conftest import drain, restart, setup_pair

from app.bot.interface import Interface
from app.scheduler.worker import Worker
from app.services.core import DomainError
from app.transport.base import FakeTransport, OperationError, Result
from app.transport.telegram import TelegramTransport
from tools.benchmark import seed


class SlowTransport(FakeTransport):
    def __init__(self, delay=0.05):
        super().__init__()
        self.active = 0
        self.maximum = 0
        self.delay = delay
        self.active_accounts = set()
        self.collisions = 0

    async def execute(self, job):
        if job["account_id"] in self.active_accounts:
            self.collisions += 1
        self.active_accounts.add(job["account_id"])
        self.active += 1
        self.maximum = max(self.maximum, self.active)
        try:
            await asyncio.sleep(self.delay)
            return await super().execute(job)
        finally:
            self.active -= 1
            self.active_accounts.discard(job["account_id"])


async def test_batches_bound_parallelism_and_preserve_idempotency(env):
    e, _, _, sink = env
    seed(e, accounts=24, chats_per_pair=2, history=0)
    e.set_setting("batch_size", 7)
    e.set_setting("concurrency", 3)
    e.tick()
    api = SlowTransport()
    worker = Worker(e, api, sink)
    assert await worker.run_batch() == 7
    assert 1 < api.maximum <= 3 and not api.collisions
    assert len({j["idempotency_key"] for j in api.calls}) == 7
    assert e.statistics("all")["confirmed"] == 7
    assert e.db.scalar("SELECT value FROM queue_counters WHERE name='pending'") == e.db.scalar(
        "SELECT count(*) FROM dispatch_jobs WHERE status='PENDING'"
    )


async def test_stopping_during_batch_starts_no_new_operations(env):
    e, _, _, sink = env
    seed(e, accounts=20, chats_per_pair=2, history=0)
    e.tick()
    entered = asyncio.Event()
    release = asyncio.Event()

    class Gate(FakeTransport):
        async def execute(self, job):
            entered.set()
            await release.wait()
            return await super().execute(job)

    api = Gate()
    worker = Worker(e, api, sink)
    task = asyncio.create_task(worker.run_batch())
    await entered.wait()
    e.campaign_status(1, "STOPPED")
    release.set()
    await task
    assert len(api.calls) <= e.config.concurrency
    assert not e.db.scalar("SELECT count(*) FROM dispatch_jobs WHERE status='PENDING'")
    assert await worker.run_batch() == 0


async def test_safe_transient_retries_are_bounded_with_backoff(env):
    e, clock, api, _ = env
    _, pair, _, _, chats = setup_pair(e, 1)
    e.tick()
    active = e.pair_card(pair)["active_account_id"]
    api.errors[("MAILING", active, chats[0])] = OperationError("transient", "PRE_SEND_NETWORK_FAILURE")
    for index, delay in enumerate((5, 10, 20)):
        assert await e.run_one(api)
        job = e.db.one("SELECT * FROM dispatch_jobs")
        assert job["status"] == "PENDING" and job["retry_count"] == index + 1
        assert job["planned_at"] == clock() + delay
        assert not await e.run_one(api)
        clock.advance(delay)
    assert await e.run_one(api)
    assert e.db.scalar("SELECT status FROM dispatch_jobs") == "FAILED"
    assert len(api.calls) == 4 and e.statistics("all")["confirmed"] == 0


async def test_stop_while_retry_result_arrives_does_not_requeue(env):
    e, _, _, _ = env
    ca, _, _, _, _ = setup_pair(e, 1)
    e.tick()
    job = e.claim_job("old")
    assert job
    e.campaign_status(ca, "STOPPED")
    e.fail_job(job, OperationError("transient", "PRE_SEND_FAILURE"))
    assert e.db.scalar("SELECT status FROM dispatch_jobs") == "CANCELLED"


def test_late_error_cannot_overwrite_confirmed_delivery(env):
    e, _, _, _ = env
    setup_pair(e, 1)
    e.tick()
    job = e.claim_job("old")
    e.finish_job(job, Result(10))
    e.fail_job(job, TimeoutError("secret"))
    assert e.db.scalar("SELECT status FROM dispatch_jobs") == "CONFIRMED"
    assert e.statistics("all")["confirmed"] == 1


def test_old_attempt_cannot_finish_a_new_attempt(env):
    e, clock, _, _ = env
    ca, pair, _, _, _ = setup_pair(e, 1)
    e.tick()
    old = e.claim_job("old")
    clock.advance(181)
    e.recover_expired()
    e.resolve_unknown(old["id"], False)
    with e.db.tx() as c:
        e._enqueue(
            c,
            e._pair(c, pair),
            e.campaign(c, ca),
            "MAILING",
            old["chat_pool_id"],
            None,
            old["payload"],
            old["idempotency_key"],
        )
    new = e.claim_job("new")
    assert new and new["attempt_id"] != old["attempt_id"]
    e.finish_job(old, Result(10))
    e.fail_job(old, OperationError("critical", "FROZEN"))
    assert e.db.scalar("SELECT status FROM dispatch_jobs") == "INFLIGHT"
    assert e.db.scalar("SELECT account_health FROM accounts LIMIT 1") == "OK"
    e.finish_job(new, Result(11))
    assert e.statistics("all")["confirmed"] == 1


async def test_cancelled_worker_records_unknown_without_replay(env):
    e, _, _, _ = env
    setup_pair(e, 1)
    e.tick()
    entered = asyncio.Event()

    class Hanging(FakeTransport):
        async def execute(self, job):
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(e.run_one(Hanging()))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert e.db.scalar("SELECT status FROM dispatch_jobs") == "UNKNOWN"
    assert not await e.run_one(FakeTransport())


async def test_sqlite_lock_does_not_block_event_loop(env):
    e, _, _, _ = env
    c = e.db.connect()
    c.execute("BEGIN IMMEDIATE")
    ticks = 0

    async def release():
        nonlocal ticks
        for _ in range(20):
            await asyncio.sleep(0.01)
            ticks += 1
        c.rollback()
        c.close()

    unlock = asyncio.create_task(release())
    result = await e.db.run(e.create_campaign, "После блокировки", ["Текст"])
    await unlock
    assert result and ticks == 20


async def test_database_outage_after_send_preserves_unknown_on_restart(env, monkeypatch):
    e, clock, _, _ = env
    setup_pair(e, 1)
    e.tick()

    def disk_failure(*args):
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(e, "finish_job", disk_failure)
    with pytest.raises(sqlite3.OperationalError):
        await e.run_one(FakeTransport())
    assert e.db.scalar("SELECT status FROM dispatch_jobs") == "INFLIGHT"
    clock.advance(181)
    recovered = restart(e)
    recovered.recover_expired()
    assert recovered.db.scalar("SELECT status FROM dispatch_jobs") == "UNKNOWN"
    assert not await recovered.run_one(FakeTransport())


def test_runtime_lease_fences_an_old_owner(env):
    e, _, _, sink = env
    setup_pair(e, 1)
    e.tick()
    worker = Worker(e, FakeTransport(), sink)
    worker.runtime_lease()
    job = e.claim_job(worker.owner)
    e.config = replace(e.config, mode="live")
    assert e.execution_allowed(job)
    with e.db.tx() as c:
        c.execute("UPDATE worker_leases SET owner='replacement' WHERE name='telegram-runtime'")
    assert not e.execution_allowed(job)


def test_queue_capacity_and_stable_planner_rotation(env):
    e, _, _, _ = env
    seed(e, accounts=12, chats_per_pair=2, history=0)
    e.set_setting("max_pending_jobs", 3)
    e.tick(pair_limit=2)
    assert e.db.scalar("SELECT count(*) FROM dispatch_jobs WHERE status='PENDING'") == 3
    assert e.db.scalar("SELECT value FROM settings WHERE key='planner_cursor'") == "2"
    e.tick(pair_limit=2)
    assert e.db.scalar("SELECT value FROM settings WHERE key='planner_cursor'") == "4"
    assert e.db.scalar("SELECT count(*) FROM dispatch_jobs WHERE status='PENDING'") == 3


async def test_parallel_button_presses_consume_one_token(env):
    e, _, _, _ = env
    ca, _, _, _, _ = setup_pair(e, 1)
    ui = Interface(e)
    button = ui.button(123, "Стоп", "campaign_status", {"id": ca, "status": "STOPPED"}, True)
    results = await asyncio.gather(ui.callback(123, button[1]), ui.callback(123, button[1]), return_exceptions=True)
    assert sum(isinstance(r, DomainError) for r in results) == 1
    assert e.db.scalar("SELECT count(*) FROM audit_events WHERE event='CAMPAIGN_STOPPED'") == 1


async def test_old_wizard_step_buttons_cannot_skip_numeric_steps(env):
    e, _, _, _ = env
    ui = Interface(e)
    ui.wizard(123, {"step": 2, "texts": [{"text": "текст"}], "accounts": []})
    token = ui.button(123, "Далее", "wizard_next", once=True)
    await ui.callback(123, token[1])
    with pytest.raises(DomainError):
        await ui.callback(
            123,
            ui.e.db.all(
                "SELECT token FROM ui_actions WHERE action='wizard_next' AND consumed_at IS NULL ORDER BY rowid LIMIT 1"
            )[0]["token"].join(("u:", "")),
        )
    assert ui.state(123)["data"]["step"] == 3


async def test_settings_and_diagnostic_ui_are_accessible(env):
    e, _, _, _ = env
    ui = Interface(e)
    view = ui.listing(123, "settings")
    queue = next(b for group in view.buttons for b in group if "Очередь и повторы" in b[0])
    view = await ui.callback(123, queue[1])
    button = next(b for group in view.buttons for b in group if "Размер пакета" in b[0])
    await ui.callback(123, button[1])
    await ui.message(123, "17")
    assert e.runtime_options()["batch_size"] == 17
    with pytest.raises(DomainError):
        e.set_setting("concurrency", 1000)
    assert "Диагностика" in ui.diagnostics(123).text


def test_session_cannot_switch_identity_or_resurrect_removed_account(env):
    e, _, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    with pytest.raises(DomainError):
        e.save_session(a, "new", 999999)
    e.update_account(a, "remove")
    with pytest.raises(DomainError):
        e.save_session(a, "new", 1001)


def test_pair_roles_are_exclusive_even_for_direct_sql(env):
    e, _, _, _ = env
    ca, _, a, b, _ = setup_pair(e, 1)
    with pytest.raises(sqlite3.IntegrityError), e.db.tx() as c:
        c.execute(
            "INSERT INTO pairs(campaign_id,account_a_id,chat_limit,created_at) VALUES (?,?,1,?)", (ca, b, e.clock())
        )


def test_diagnostics_redact_secrets_and_config_repr(env):
    e, _, _, _ = env
    secret = "123456:long_token_that_must_never_appear"
    with e.db.tx() as c:
        e.audit(c, "TEST_SECRET", token=secret, password="password-value", code="112233", phone="+380111111111")
    stored = e.db.scalar("SELECT detail FROM audit_events WHERE event='TEST_SECRET'")
    assert secret not in stored and "112233" not in stored and "password-value" not in stored
    assert "private-key" not in repr(
        replace(e.config, session_key="private-key", bot_token=secret, api_hash="private-key")
    )


def test_expired_operational_state_is_cleaned_but_history_remains(env):
    e, clock, _, _ = env
    ui = Interface(e)
    ui.button(123, "Меню", "main", ttl=1)
    ui.set_state(123, "search")
    clock.advance(3601)
    e.maintenance()
    assert not e.db.scalar("SELECT count(*) FROM ui_actions")
    assert not e.db.scalar("SELECT count(*) FROM ui_states")


def test_greeting_format_change_never_repeats_finished_steps(env):
    e, _, api, _ = env
    ca, pair, _, _, chats = setup_pair(e, 1, greetings=True)
    e.edit_campaign(ca, "greeting", ["Основной текст"], greeting_split=True)
    e.set_chat_permissions(chats[0], True, True, True, True)
    e.tick()
    job = e.claim_job("greet")
    e.finish_job(job, Result(1))
    assert job["payload"]["stage"] == "emoji"
    emoji = job["payload"]["text"]
    e.edit_campaign(ca, greeting_split=False)
    e.tick()
    next_job = e.claim_job("greet2")
    assert next_job and emoji not in next_job["payload"]["text"]


async def test_incoming_listener_filters_plain_discussion_without_sender_rpc(env):
    e, _, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    transport = object.__new__(TelegramTransport)
    transport.engine = e

    class Event:
        is_private = False
        is_reply = False
        raw_text = "Обычное обсуждение"
        chat_id = -100000

        async def get_sender(self):
            raise AssertionError("Не нужен RPC")

    await transport._handle_incoming(a, Event())
    assert not e.db.scalar("SELECT count(*) FROM chat_issues")


class LocalClient:
    def __init__(self):
        self.connected = False
        self.disconnections = 0
        self.handlers = []

    async def connect(self):
        await asyncio.sleep(0.02)
        self.connected = True

    async def is_user_authorized(self):
        return True

    def is_connected(self):
        return self.connected

    async def disconnect(self):
        self.connected = False
        self.disconnections += 1

    def on(self, event):
        def register(handler):
            self.handlers.append(handler)
            return handler

        return register


async def test_one_mtproto_client_per_account_and_session_refresh(env, monkeypatch):
    e, _, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    e.config = replace(e.config, mode="live")
    transport = TelegramTransport(e)
    created = []

    def factory(*args, **kwargs):
        client = LocalClient()
        created.append(client)
        return client

    monkeypatch.setattr(transport, "new_client", factory)
    try:
        clients = await asyncio.gather(*(transport.client(a) for _ in range(10)))
        assert len(created) == 1 and all(c is clients[0] for c in clients)
        e.save_session(a, "renewed-session", 1001)
        fresh = await transport.client(a)
        assert fresh is not clients[0] and clients[0].disconnections == 1
        fresh.connected = False
        assert await transport.client(a) is not fresh
    finally:
        await transport.close()
    assert all(not c.connected for c in created)


async def test_connect_cancellation_disconnects_half_created_client(env, monkeypatch):
    e, _, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    e.config = replace(e.config, mode="live")
    transport = TelegramTransport(e)
    started = asyncio.Event()

    class Hanging(LocalClient):
        async def connect(self):
            started.set()
            await asyncio.Event().wait()

    client = Hanging()
    monkeypatch.setattr(transport, "new_client", lambda *a, **k: client)
    task = asyncio.create_task(transport.client(a))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert client.disconnections == 1 and not transport.clients
    await transport.close()


async def test_incoming_overload_is_bounded_and_visible_to_admin(env, monkeypatch):
    e, _, _, sink = env
    _, _, a, _, _ = setup_pair(e, 1)
    e.config = replace(e.config, mode="live", incoming_queue_size=1)
    transport = TelegramTransport(e)
    client = LocalClient()
    monkeypatch.setattr(transport, "new_client", lambda *a, **k: client)
    gate = asyncio.Event()

    async def handle(*args):
        await gate.wait()

    monkeypatch.setattr(transport, "_handle_incoming", handle)
    await transport.client(a)
    try:
        event = SimpleNamespace()
        for _ in range(10):
            await client.handlers[0](event)
        assert transport._incoming.qsize() <= 1 and transport._incoming_dropped >= 1
        await asyncio.sleep(0.08)
        assert e.db.scalar("SELECT count(*) FROM notifications_outbox WHERE kind='overload'") == 1
        await e.deliver_notifications(sink)
        assert sink.messages[-1][1] == "overload"
    finally:
        gate.set()
        await transport.close()


@pytest.mark.parametrize("phase", ["preflight", "send"])
async def test_retry_classification_depends_on_whether_send_may_have_started(env, phase):
    e, _, _, _ = env
    setup_pair(e, 1)
    e.tick()
    job = e.claim_job("phases")
    adapter = object.__new__(TelegramTransport)
    adapter.engine = e

    class Client:
        async def __call__(self, request):
            return True

        async def get_entity(self, peer):
            if phase == "preflight":
                raise TimeoutError("secret")
            return SimpleNamespace(default_banned_rights=None, broadcast=False)

        async def get_permissions(self, *args):
            return SimpleNamespace(has_left=False, is_admin=False, participant=SimpleNamespace(banned_rights=None))

        async def send_message(self, *args, **kwargs):
            raise TimeoutError("secret")

    async def client(account):
        return Client()

    adapter.client = client
    with pytest.raises(OperationError) as error:
        await adapter.execute(job)
    assert error.value.category == ("transient" if phase == "preflight" else "unknown")


def test_manual_verification_repeated_cycles_notify_again_but_not_each_message(env):
    e, clock, _, _ = env
    _, _, a, _, chats = setup_pair(e, 1)
    e.require_manual_verification(a, -100000)
    e.require_manual_verification(a, -100000)
    assert e.db.scalar("SELECT count(*) FROM notifications_outbox WHERE kind='manual'") == 1
    e.resume_chat(chats[0])
    clock.advance(1)
    e.require_manual_verification(a, -100000)
    assert e.db.scalar("SELECT count(*) FROM notifications_outbox WHERE kind='manual'") == 2


def test_reusing_chat_waits_for_old_uncertain_operation(env):
    e, clock, _, _ = env
    ca, pair, a, b, chats = setup_pair(e, 1)
    e.tick()
    job = e.claim_job("old")
    assert job
    e.campaign_status(ca, "REMOVED", True)
    new = e.create_campaign("Вторая", ["Новый текст"], chat_limit=1)
    # Account changes must wait for any old pair member's operation.
    with pytest.raises(DomainError):
        e.assign_account(b, new)
    assert e.db.scalar("SELECT status FROM chat_pool WHERE id=?", (chats[0],)) == "FREE"
    clock.advance(181)
    e.recover_expired()
    with e.db.tx() as c:
        e._fill_waiting(c)
    assert not e.db.one("SELECT * FROM pair_chats WHERE chat_pool_id=? AND removed_at IS NULL", (chats[0],))


def test_edit_version_is_checked_inside_transaction(env):
    e, _, _, _ = env
    ca, _, _, _, _ = setup_pair(e, 1)
    e.edit_campaign(ca, "mailing", ["Один"], expected_version=1)
    with pytest.raises(DomainError):
        e.edit_campaign(ca, "mailing", ["Потеря изменения"], expected_version=1)
    assert e.templates(ca)[0]["text"] == "Один"
    with pytest.raises(DomainError):
        e.edit_campaign(ca, name="   ")


@pytest.mark.parametrize("health", ["DISCONNECTED", "SESSION_EXPIRED", "FROZEN", "RESTRICTED", "REMOVED"])
async def test_every_non_healthy_account_state_blocks_publications(env, health):
    e, _, api, _ = env
    _, pair, a, b, _ = setup_pair(e, 1)
    active = e.pair_card(pair)["active_account_id"]
    with e.db.tx() as c:
        c.execute("UPDATE accounts SET account_health=? WHERE id=?", (health, active))
    e.tick()
    await drain(e, api)
    assert not api.calls


async def test_parallel_outbox_deliveries_do_not_send_same_item_twice(env):
    e, _, _, sink = env
    _, _, a, _, _ = setup_pair(e, 1)
    e.critical(a, "FROZEN")
    await asyncio.gather(e.deliver_notifications(sink, "one"), e.deliver_notifications(sink, "two"))
    assert len(sink.messages) == 1


async def test_critical_stop_button_is_not_blocked_by_long_admin_operation(env):
    e, _, _, _ = env
    ca, _, _, _, _ = setup_pair(e, 1)
    ui = Interface(e)
    button = ui.button(123, "Стоп", "campaign_status", {"id": ca, "status": "STOPPED"}, True)
    await ui._user_locks[123].acquire()
    try:
        await asyncio.wait_for(ui.callback(123, button[1]), 1)
        assert e.db.scalar("SELECT status FROM campaigns") == "STOPPED"
    finally:
        ui._user_locks[123].release()


def test_telethon_session_caches_stay_bounded_without_losing_latest_peers():
    from telethon import types

    from app.transport.session import BoundedStringSession

    session = BoundedStringSession(entity_limit=100, file_limit=8)
    for batch in range(50):
        users = [types.User(id=batch * 100 + i + 1, access_hash=123, first_name="Тест") for i in range(100)]
        session.process_entities(SimpleNamespace(users=users))
        assert len(session._entities) <= 100 and len(session._recent_entities) <= 100
    assert session.get_input_entity(5000).user_id == 5000
    for i in range(100):
        session.cache_file(i.to_bytes(16, "big"), 100, types.InputPhoto(id=i, access_hash=i, file_reference=b""))
    assert len(session._files) == 8


async def test_notifications_recover_from_database_outage(env, monkeypatch):
    e, _, _, sink = env
    _, _, a, _, _ = setup_pair(e, 1)
    e.critical(a, "SESSION_EXPIRED")
    original = e._claim_notification

    def outage(*args):
        raise sqlite3.OperationalError("unable to open database file")

    monkeypatch.setattr(e, "_claim_notification", outage)
    with pytest.raises(sqlite3.OperationalError):
        await e.deliver_notifications(sink)
    assert not sink.messages
    monkeypatch.setattr(e, "_claim_notification", original)
    await e.deliver_notifications(sink)
    assert len(sink.messages) == 1 and sink.messages[0][1] == "critical"


def test_outbox_fencing_rejects_a_late_owner(env):
    e, clock, _, _ = env
    _, _, a, _, _ = setup_pair(e, 1)
    e.critical(a, "FROZEN")
    old = e._claim_notification("same-owner")
    clock.advance(121)
    new = e._claim_notification("same-owner")
    assert old["fence"] != new["fence"]
    e._finish_notification(old)
    assert e.db.scalar("SELECT status FROM notifications_outbox") == "SENDING"
    e._finish_notification(new)
    assert e.db.scalar("SELECT status FROM notifications_outbox") == "SENT"


def test_hourly_payload_and_issue_pages_are_bounded(env):
    e, _, _, _ = env
    _, pair, a, _, chats = setup_pair(e, 20)
    for chat in chats:
        e.chat_failure(pair, chat, a, "PUBLICATION_FORBIDDEN", True)
    e.hourly_summary()
    import json

    payload = json.loads(e.db.scalar("SELECT payload FROM notifications_outbox WHERE kind='summary'"))
    assert len(payload["issues"]) == 6 and payload["total"] == 20
    view = Interface(e).issues(123, page=2)
    assert "№13" in view.text and "№18" in view.text and "\n№1 ·" not in view.text


async def test_many_simultaneous_incoming_events_create_only_one_reply(env):
    e, _, _, _ = env
    ca, pair, _, _, _ = setup_pair(e, 1)
    e.edit_campaign(ca, "dm_always", ["Ответ"], dm_always_enabled=True)
    active = e.pair_card(pair)["active_account_id"]
    results = await asyncio.gather(*(e.db.run(e.incoming, active, 800, 77, 800, True) for _ in range(50)))
    assert sum(bool(result) for result in results) == 1
    api = FakeTransport()
    await drain(e, api)
    assert len(api.calls) == 1


def test_greeting_progress_changed_while_another_step_was_inflight(env):
    e, _, _, _ = env
    ca, pair, _, _, chats = setup_pair(e, 1, greetings=True)
    e.edit_campaign(ca, "greeting", ["Основной текст"], greeting_split=True)
    e.set_chat_permissions(chats[0], True, True, True, True)
    e.tick()
    first = e.claim_job("first")
    e.edit_campaign(ca, greeting_split=False)
    e.tick()
    e.finish_job(first, Result(101))
    assert e.claim_job("stale") is None
    e.tick()
    next_job = e.claim_job("fresh")
    assert next_job and first["payload"]["text"] not in next_job["payload"]["text"]
    e.finish_job(next_job, Result(102))
    e.tick()
    assert e.db.scalar("SELECT count(*) FROM message_results") == 2


async def test_campaign_media_error_never_blames_the_chat(env):
    e, _, api, sink = env
    ca, pair, _, _, chats = setup_pair(e, 1)
    active = e.pair_card(pair)["active_account_id"]
    api.errors[("MAILING", active, chats[0])] = OperationError("configuration", "MEDIA_UNAVAILABLE")
    e.tick()
    await drain(e, api)
    assert e.db.scalar("SELECT status FROM campaigns") == "PAUSED"
    assert not e.db.scalar("SELECT count(*) FROM chat_issues")
    await e.deliver_notifications(sink)
    assert sink.messages[-1][1] == "campaign_error"


async def test_abrupt_process_exit_recovers_to_unknown(env):
    import os
    import subprocess
    import sys

    e, clock, api, _ = env
    setup_pair(e, 1)
    e.tick()
    program = """
import os,sys
from app.config import Config
from app.db import Database
from app.security import SecretBox
from app.services import Engine
cfg=Config(database_path=sys.argv[1],session_key=os.environ['FE_TEST_KEY'],admin_ids=(123,))
engine=Engine(Database(cfg.database_path),cfg,SecretBox(cfg.session_key),lambda:float(sys.argv[2]))
assert engine.claim_job('crash-process')
os._exit(17)
"""
    result = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, "-c", program, e.db.path, str(clock())],
        env={**os.environ, "FE_TEST_KEY": e.config.session_key},
        capture_output=True,
        timeout=20,
    )
    assert result.returncode == 17
    assert e.db.scalar("SELECT status FROM dispatch_jobs") == "INFLIGHT"
    clock.advance(181)
    e.recover_expired()
    assert e.db.scalar("SELECT status FROM dispatch_jobs") == "UNKNOWN"
    assert not await e.run_one(api) and not api.calls
    assert e.db.scalar("PRAGMA integrity_check") == "ok"


async def test_critical_delivery_does_not_wait_for_a_slow_hourly_report(env):
    e, _, _, _ = env
    _, pair, a, _, chats = setup_pair(e, 1)
    e.chat_failure(pair, chats[0], a, "PUBLICATION_FORBIDDEN", True)
    e.hourly_summary()
    entered = asyncio.Event()
    release = asyncio.Event()
    delivered = []

    class Sink:
        async def send(self, admin, kind, payload):
            if kind == "summary":
                entered.set()
                await release.wait()
            delivered.append(kind)

    sink = Sink()
    normal = asyncio.create_task(e.deliver_notifications(sink, "normal", urgent=False))
    await entered.wait()
    e.critical(a, "FROZEN")
    await asyncio.wait_for(e.deliver_notifications(sink, "urgent", urgent=True), 1)
    assert delivered == ["critical"]
    release.set()
    await normal


def test_pending_hourly_reports_are_coalesced_after_an_outage(env):
    e, clock, _, _ = env
    _, pair, a, _, chats = setup_pair(e, 1)
    e.chat_failure(pair, chats[0], a, "PUBLICATION_FORBIDDEN", True)
    e.hourly_summary()
    clock.advance(3600)
    e.hourly_summary()
    assert e.db.scalar("SELECT count(*) FROM notifications_outbox WHERE kind='summary' AND status='PENDING'") == 1


def test_late_ack_updates_historical_chat_cooldown(env):
    e, clock, _, _ = env
    ca, pair, _, _, chats = setup_pair(e, 1)
    e.tick()
    job = e.claim_job("old")
    e.campaign_status(ca, "REMOVED", True)
    e.finish_job(job, Result(20))
    assert e.db.scalar("SELECT cooldown_until FROM pair_chats WHERE chat_pool_id=?", (chats[0],)) == clock() + 300


async def test_duplicate_runtime_is_rejected_before_bot_polling(env, monkeypatch):
    from app.bot.runtime import serve

    e, _, api, sink = env
    first = Worker(e, api, sink)
    assert first.runtime_lease()
    e.config = replace(e.config, bot_token="123456:fake_test_token")
    closed = []

    class Session:
        async def close(self):
            closed.append(True)

    monkeypatch.setattr("app.bot.runtime.Bot", lambda token: SimpleNamespace(session=Session()))
    with pytest.raises(RuntimeError, match="Другой процесс"):
        await serve(e, api)
    assert closed == [True]


def test_pending_greeting_rechecks_partner_state(env):
    e, _, _, _ = env
    ca, pair, a, b, _ = setup_pair(e, 1, greetings=True)
    e.edit_campaign(ca, "greeting", ["???????????"])
    e.tick()
    job = e.claim_job("greet")
    assert job and job["kind"] == "GREETING"
    partner = b if job["account_id"] == a else a
    e.update_account(partner, "pause")
    assert not e.execution_allowed(job)


async def test_runtime_survives_a_short_database_failure_in_heartbeat(env, monkeypatch):
    e, _, api, sink = env
    worker = Worker(e, api, sink)
    worker.heartbeat_interval = 0.01
    original = worker.runtime_lease
    calls = []

    def flaky_lease():
        calls.append(True)
        if len(calls) == 2:
            raise sqlite3.OperationalError("disk I/O error")
        return original()

    monkeypatch.setattr(worker, "runtime_lease", flaky_lease)
    task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.12)
    assert len(calls) >= 3 and not task.done()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


def test_upgrade_from_existing_v200_preserves_data_and_attempts(env, tmp_path):
    from pathlib import Path

    from app.db import Database

    db = Database(str(tmp_path / "legacy.sqlite3"))
    c = db.connect()
    migrations = Path("app/db/migrations")
    c.execute("CREATE TABLE schema_migrations(version TEXT PRIMARY KEY,applied_at REAL)")
    for name in ("001_initial", "002_ui_receipts", "003_template_cursor"):
        c.executescript((migrations / (name + ".sql")).read_text(encoding="utf-8"))
        c.execute("INSERT INTO schema_migrations VALUES (?,0)", (name,))
    c.execute("INSERT INTO campaigns(id,name,timezone,created_at) VALUES (1,'Legacy','Europe/Kyiv',0)")
    c.execute(
        "INSERT INTO accounts(id,name,phone_mask,onboarding_started_at,stage_started_at,created_at) VALUES (1,'Legacy account','***1234',0,0,0)"
    )
    c.execute("INSERT INTO account_sessions VALUES (1,'cipher-preserved','phone-cipher',0)")
    c.execute("INSERT INTO pairs(id,campaign_id,account_a_id,chat_limit,created_at) VALUES (1,1,1,1,0)")
    c.execute(
        "INSERT INTO dispatch_jobs(id,idempotency_key,campaign_id,pair_id,account_id,kind,payload,version,planned_at,status,lease_until,created_at) VALUES (1,'legacy',1,1,1,'CHECK','{}',1,0,'INFLIGHT',0,0)"
    )
    c.execute("INSERT INTO message_attempts(id,job_id,account_id,started_at) VALUES (7,1,1,0)")
    c.close()
    db.migrate()
    db.migrate()
    assert db.scalar("SELECT current_attempt_id FROM dispatch_jobs") == 7
    assert db.scalar("SELECT encrypted_session FROM account_sessions") == "cipher-preserved"
    assert db.scalar("SELECT count(*) FROM schema_migrations") == 8
    assert not db.all("PRAGMA foreign_key_check")


async def test_expired_callback_ack_does_not_prevent_stopping_campaign(env, monkeypatch):
    from aiogram.methods import AnswerCallbackQuery
    from test_bot import Harness

    e, _, api, _ = env
    setup_pair(e, 1)
    h = Harness(e, api)
    await h.text("/start")
    await h.click("Рассылки")
    await h.click("Основная")
    original = h.api.make_request

    async def old_query(bot, method, timeout=None):
        if isinstance(method, AnswerCallbackQuery):
            raise RuntimeError("QUERY_ID_INVALID")
        return await original(bot, method, timeout)

    monkeypatch.setattr(h.api, "make_request", old_query)
    await h.click("Остановить рассылку")
    assert e.db.scalar("SELECT status FROM campaigns") == "STOPPED"
    await h.bot.session.close()


@pytest.mark.parametrize("reason", ["unknown", "flood"])
def test_pair_display_distinguishes_preparation_from_operational_pause(env, reason):
    e, _, _, _ = env
    _, pair, _, _, _ = setup_pair(e, 1)
    e.tick()
    job = e.claim_job("reason")
    e.fail_job(job, OperationError(reason, "FLOOD_WAIT" if reason == "flood" else "TIMEOUT", 30))
    e.tick()
    assert e.pair_card(pair)["status"] == ("NEEDS_ATTENTION" if reason == "unknown" else "PAUSED")


async def test_detected_freeze_is_reported_even_when_outbox_write_fails(env, monkeypatch):
    e, _, api, sink = env
    _, pair, _, _, chats = setup_pair(e, 1)
    e.tick()
    active = e.pair_card(pair)["active_account_id"]
    api.errors[("MAILING", active, chats[0])] = OperationError("critical", "FROZEN")

    def disk_failure(*args):
        raise sqlite3.OperationalError("disk I/O error")

    monkeypatch.setattr(e, "fail_job", disk_failure)
    with pytest.raises(sqlite3.OperationalError):
        await Worker(e, api, sink).run_batch()
    assert sink.messages[-1][1] == "emergency" and sink.messages[-1][2]["reason"] == "FROZEN"
    assert e.db.scalar("SELECT status FROM dispatch_jobs") == "INFLIGHT"


async def test_exhausted_first_target_does_not_starve_other_targets(env):
    e, _, api, _ = env
    _, pair, _, _, chats = setup_pair(e, 20)
    e.set_setting("max_pending_jobs", 1)
    e.set_setting("max_retries", 0)
    e.tick()
    first = e.claim_job("first")
    e.fail_job(first, OperationError("transient", "PRE_SEND_FAILURE"))
    e.tick()
    next_job = e.claim_job("next")
    assert next_job and next_job["chat_pool_id"] != first["chat_pool_id"]
    e.finish_job(next_job, Result(1))
    assert e.statistics("all")["confirmed"] == 1


def test_failed_greeting_recovers_after_explicit_edit_and_restart(env):
    e, _, _, _ = env
    ca, pair, _, _, _ = setup_pair(e, 1, greetings=True)
    e.edit_campaign(ca, "greeting", ["Старое приветствие"])
    e.tick()
    old = e.claim_job("first")
    e.fail_job(old, OperationError("configuration", "MEDIA_UNAVAILABLE"))
    e.edit_campaign(ca, "greeting", ["Исправленное приветствие"])
    e.campaign_status(ca, "RUNNING", True)
    e.tick()
    new = e.claim_job("fixed")
    assert new and "Исправленное приветствие" in new["payload"]["text"]
    assert "Старое приветствие" not in new["payload"]["text"]
    e.finish_job(new, Result(10))
    e.tick()
    assert e.statistics("all")["confirmed"] == 1
